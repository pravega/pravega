/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests for the {@link TableService} class.
 */
@Slf4j
public class TableServiceTests extends ThreadPooledTestSuite {
    //region Config and Setup

    private static final int THREADPOOL_SIZE_SEGMENT_STORE = 20;
    private static final int THREADPOOL_SIZE_SEGMENT_STORE_STORAGE = 10;
    private static final int THREADPOOL_SIZE_TEST = 3;
    private static final int SEGMENT_COUNT = 10;
    private static final int KEY_COUNT = 1000;
    private static final int MAX_KEY_LENGTH = 128;
    private static final int MAX_VALUE_LENGTH = 32;
    private static final int[] FIXED_KEY_LENGTHS = new int[]{8, 16, 32, 64, 128, 256};
    private static final String TABLE_SEGMENT_NAME_PREFIX = "TableSegment_";
    private static final String TABLE_SEGMENT_FIXED_KEY_NAME_PREFIX = "TableSegmentFixedKey_";
    private static final Comparator<BufferView> KEY_COMPARATOR = BufferViewComparator.create()::compare;
    private static final Duration TIMEOUT = Duration.ofSeconds(30); // Individual call timeout
    @Rule
    public Timeout globalTimeout = new Timeout((int) TIMEOUT.toMillis() * 4, TimeUnit.MILLISECONDS);

    private final ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
            .builder()
            .include(ServiceConfig
                    .builder()
                    .with(ServiceConfig.CONTAINER_COUNT, 1)
                    .with(ServiceConfig.THREAD_POOL_SIZE, THREADPOOL_SIZE_SEGMENT_STORE)
                    .with(ServiceConfig.STORAGE_THREAD_POOL_SIZE, THREADPOOL_SIZE_SEGMENT_STORE_STORAGE)
                    .with(ServiceConfig.CACHE_POLICY_MAX_SIZE, 16 * 1024 * 1024L)
                    .with(ServiceConfig.CACHE_POLICY_MAX_TIME, 30))
            .include(ContainerConfig
                    .builder()
                    .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS))
            .include(DurableLogConfig
                    .builder()
                    .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
                    .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                    .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L))
            .include(ReadIndexConfig
                    .builder()
                    .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024))
            .include(TableExtensionConfig
                    .builder()
                    .with(TableExtensionConfig.MAX_TAIL_CACHE_PREINDEX_BATCH_SIZE, (MAX_KEY_LENGTH + MAX_KEY_LENGTH) * 13))
            .include(WriterConfig
                    .builder()
                    .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                    .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                    .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                    .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L));
    private InMemoryStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory durableDataLogFactory;

    @Override
    protected int getThreadPoolSize() {
        return THREADPOOL_SIZE_TEST;
    }

    @Before
    public void setUp() {
        this.storageFactory = new InMemoryStorageFactory(executorService());
        this.durableDataLogFactory = new PermanentDurableDataLogFactory(executorService());
    }

    @After
    public void tearDown() {
        if (this.durableDataLogFactory != null) {
            this.durableDataLogFactory.close();
            this.durableDataLogFactory = null;
        }

        if (this.storageFactory != null) {
            this.storageFactory.close();
            this.storageFactory = null;
        }
    }

    //endregion

    /**
     * Tests an End-to-End scenario for a {@link TableStore} implementation using a real implementation of {@link StreamSegmentStore}
     * (without any mocks or manual event triggering or other test aids). Features tested:
     * - Table Segment creation and deletion.
     * - Conditional and unconditional updates.
     * - Conditional and unconditional removals.
     * - Recovering of Table Segments after failover.
     *
     * This tests both Hash Table Segments and Fixed-Key-Length Table Segments.
     */
    @Test
    public void testEndToEnd() throws Exception {
        val rnd = new Random(0);
        val segmentTypes = new SegmentType[]{SegmentType.builder().tableSegment().build(), SegmentType.builder().fixedKeyLengthTableSegment().build()};
        ArrayList<String> segmentNames;
        HashMap<BufferView, EntryData> keyInfo;

        // Phase 1: Create some segments and update some data (unconditionally).
        log.info("Starting Phase 1");
        try (val builder = createBuilder()) {
            val tableStore = builder.createTableStoreService();

            // Create the Table Segments.
            segmentNames = createSegments(tableStore, segmentTypes);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));

            // Generate the keys and map them to segments.
            keyInfo = generateKeysForSegments(segmentNames, rnd);

            // Unconditional updates.
            val updates = generateUpdates(keyInfo, false, rnd);
            val updateVersions = executeUpdates(updates, tableStore);
            acceptUpdates(updates, updateVersions, keyInfo);
            log.info("Finished unconditional updates.");

            // Check.
            check(keyInfo, tableStore);

            log.info("Finished Phase 1");
        }

        // Phase 2: Force a recovery and remove all data (unconditionally)
        log.info("Starting Phase 2");
        try (val builder = createBuilder()) {
            val tableStore = builder.createTableStoreService();

            // Check (after recovery)
            check(keyInfo, tableStore);

            // Unconditional removals.
            val removals = generateRemovals(keyInfo, false);
            executeRemovals(removals, tableStore);
            acceptRemovals(removals, keyInfo);

            // Check.
            check(keyInfo, tableStore);

            log.info("Finished Phase 2");
        }

        // Phase 3: Force a recovery and conditionally update and remove data
        log.info("Starting Phase 3");
        try (val builder = createBuilder()) {
            val tableStore = builder.createTableStoreService();

            // Check (after recovery).
            check(keyInfo, tableStore);

            // Conditional update.
            val updates = generateUpdates(keyInfo, true, rnd);
            val updateVersions = executeUpdates(updates, tableStore);
            acceptUpdates(updates, updateVersions, keyInfo);

            val offsetConditionedUpdates = generateUpdates(keyInfo, true, rnd);
            val offsetUpdateVersions = executeOffsetConditionalUpdates(offsetConditionedUpdates, -1L, tableStore);
            acceptUpdates(offsetConditionedUpdates, offsetUpdateVersions, keyInfo);
            log.info("Finished conditional updates.");

            // Check.
            check(keyInfo, tableStore);

            // Conditional remove.
            val removals = generateRemovals(keyInfo, true);
            executeRemovals(removals, tableStore);
            acceptRemovals(removals, keyInfo);

            val offsetConditionedRemovals = generateRemovals(keyInfo, true);
            executeOffsetConditonalRemovals(offsetConditionedRemovals, -1L, tableStore);
            acceptRemovals(offsetConditionedRemovals, keyInfo);
            log.info("Finished conditional removes.");

            // Check.
            check(keyInfo, tableStore);

            log.info("Finished Phase 3");
        }

        // Phase 4: Force a recovery and conditionally remove all data
        log.info("Starting Phase 4");
        try (val builder = createBuilder()) {
            val tableStore = builder.createTableStoreService();

            // Check (after recovery)
            check(keyInfo, tableStore);

            // Conditional update again.
            val updates = generateUpdates(keyInfo, true, rnd);
            val updateVersions = executeUpdates(updates, tableStore);
            acceptUpdates(updates, updateVersions, keyInfo);
            log.info("Finished conditional updates.");

            // Check.
            check(keyInfo, tableStore);

            // Delete all.
            val deletions = segmentNames.stream().map(s -> tableStore.deleteSegment(s, false, TIMEOUT)).collect(Collectors.toList());
            Futures.allOf(deletions).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            log.info("Finished Phase 4");
        }
    }

    private void check(HashMap<BufferView, EntryData> keyInfo, TableStore tableStore) throws Exception {
        val bySegment = keyInfo.entrySet().stream()
                .collect(Collectors.groupingBy(e -> e.getValue().segmentName));

        // Check inexistent keys.
        val searchFutures = new ArrayList<CompletableFuture<List<TableEntry>>>();
        val iteratorFutures = new ArrayList<CompletableFuture<List<TableEntry>>>();

        // Delta Iteration does not support fixed-key-length TableSegments.
        val unsortedIteratorFutures = new ArrayList<CompletableFuture<List<TableEntry>>>();
        val offsetIteratorFutures = new ArrayList<CompletableFuture<List<IteratorItem<TableEntry>>>>();
        val expectedResult = new ArrayList<Map.Entry<BufferView, EntryData>>();
        for (val e : bySegment.entrySet()) {
            String segmentName = e.getKey();
            boolean fixedKeyLength = isFixedKeyLength(segmentName);
            val info = tableStore.getInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertEquals(segmentName, info.getName());
            AssertExtensions.assertGreaterThan("Unexpected length for " + segmentName, 0, info.getLength());
            val expectedKeyLength = isFixedKeyLength(segmentName) ? getFixedKeyLength(segmentName) : 0;
            Assert.assertEquals("Unexpected key length for " + segmentName, expectedKeyLength, info.getKeyLength());
            Assert.assertEquals(fixedKeyLength, info.getType().isFixedKeyLengthTableSegment());

            val keys = new ArrayList<BufferView>();
            for (val se : e.getValue()) {
                keys.add(se.getKey());
                expectedResult.add(se);
            }

            searchFutures.add(tableStore.get(segmentName, keys, TIMEOUT));
            CompletableFuture<List<TableEntry>> entryIteratorFuture = tableStore.entryIterator(segmentName, IteratorArgs.builder().fetchTimeout(TIMEOUT).build())
                    .thenCompose(ei -> {
                        val result = new ArrayList<TableEntry>();
                        return ei.forEachRemaining(i -> result.addAll(i.getEntries()), executorService())
                                .thenApply(v -> {
                                    if (fixedKeyLength) {
                                        checkSortedOrder(result);
                                    }
                                    return result;
                                });
                    });
            iteratorFutures.add(entryIteratorFuture);
            if (!fixedKeyLength) {
                unsortedIteratorFutures.add(entryIteratorFuture);
                // For simplicity, always start from beginning of TableSegment.
                offsetIteratorFutures.add(tableStore.entryDeltaIterator(segmentName, 0L, TIMEOUT)
                        .thenCompose(ei -> {
                            val result = new ArrayList<IteratorItem<TableEntry>>();
                            return ei.forEachRemaining(result::add, executorService())
                                    .thenApply(v -> result);
                        }));
            }
        }

        // Check search results.
        val actualResults = Futures.allOfWithResults(searchFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                   .stream().flatMap(List::stream).collect(Collectors.toList());
        Assert.assertEquals("Unexpected number of search results.", expectedResult.size(), actualResults.size());
        for (int i = 0; i < expectedResult.size(); i++) {
            val expectedKey = expectedResult.get(i).getKey();
            val expectedEntry = expectedResult.get(i).getValue();
            val actual = actualResults.get(i);
            if (expectedEntry.isDeleted()) {
                // Deleted keys will be returned as nulls.
                if (actual != null) {
                    val r2 = tableStore.get(expectedEntry.segmentName, Collections.singletonList(expectedKey), TIMEOUT).join();
                }
                Assert.assertNull("Not expecting a value for a deleted Key ", actual);
            } else {
                Assert.assertEquals("Unexpected value for non-deleted Key.", expectedEntry.getValue(), actual.getValue());
                Assert.assertEquals("Unexpected key for non-deleted Key.", expectedKey, actual.getKey().getKey());
                Assert.assertEquals("Unexpected TableKey.Version for non-deleted Key.", expectedEntry.getVersion(), actual.getKey().getVersion());
            }
        }

        // Check iterator results. We sort it (and actualResults) by Version/Offset to ease the comparison.
        val actualIteratorResults = Futures.allOfWithResults(iteratorFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .stream()
                .flatMap(List::stream)
                .sorted(Comparator.comparingLong(e -> e.getKey().getVersion()))
                .collect(Collectors.toList());
        val expectedIteratorResults = actualResults.stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(e -> e.getKey().getVersion()))
                .collect(Collectors.toList());
        // These lists are used to compare non-delta based iteration with delta based iteration.
        val actualUnsortedIteratorResults = Futures.allOfWithResults(unsortedIteratorFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .stream()
                .flatMap(List::stream)
                .sorted(Comparator.comparingLong(e -> e.getKey().getVersion()))
                .collect(Collectors.toList());
        val expectedUnsortedIteratorResults = actualUnsortedIteratorResults.stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(e -> e.getKey().getVersion()))
                .collect(Collectors.toList());
        val actualOffsetIteratorList = Futures.allOfWithResults(offsetIteratorFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        val actualOffsetIteratorResults = processDeltaIteratorItems(actualOffsetIteratorList).stream()
                .sorted(Comparator.comparingLong(e -> e.getKey().getVersion()))
                .collect(Collectors.toList());

        AssertExtensions.assertListEquals("Unexpected result from entryIterator().", expectedIteratorResults, actualIteratorResults, TableEntry::equals);
        for (val entry : expectedUnsortedIteratorResults) {
            Assert.assertNotNull("Missing expected TableEntry from deltaEntryIterator()", actualOffsetIteratorResults.contains(entry));
        }

    }

    private List<TableEntry> processDeltaIteratorItems(List<IteratorItem<TableEntry>> entries) {
        Map<BufferView, TableEntry> result = new HashMap<>();
        for (val item : entries) {
            TableEntry entry = item.getEntries().iterator().next();
            DeltaIteratorState state = DeltaIteratorState.deserialize(item.getState());
            if (state.isDeletionRecord() && result.containsKey(entry.getKey().getKey())) {
                result.remove(entry.getKey().getKey());
            } else {
                result.compute(entry.getKey().getKey(), (key, value) -> {
                    if (value == null) {
                        return entry;
                    } else {
                        return value.getKey().getVersion() < entry.getKey().getVersion() ? entry : value;
                    }
                });
            }
        }
        return new ArrayList<>(result.values());
    }

    private void checkSortedOrder(List<TableEntry> entries) {
        if (entries.size() > 0) {
            for (int i = 1; i < entries.size(); i++) {
                int c = KEY_COMPARATOR.compare(entries.get(i - 1).getKey().getKey(), entries.get(i).getKey().getKey());
                AssertExtensions.assertLessThan("", 0, c);
            }
        }
    }

    private Map<String, List<Long>> executeUpdates(HashMap<String, ArrayList<TableEntry>> updates, TableStore tableStore) throws Exception {
        val updateResult = updates.entrySet().stream()
                                  .collect(Collectors.toMap(Map.Entry::getKey, e -> tableStore.put(e.getKey(), e.getValue(), TIMEOUT)));
        return Futures.allOfWithResults(updateResult).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Map<String, List<Long>> executeOffsetConditionalUpdates(HashMap<String, ArrayList<TableEntry>> updates,
                                                                    long tableSegmentOffset,
                                                                    TableStore tableStore) throws Exception {
        val updateResult = updates.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> tableStore.put(e.getKey(), e.getValue(), tableSegmentOffset, TIMEOUT)));
        return Futures.allOfWithResults(updateResult).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void executeOffsetConditonalRemovals(HashMap<String, ArrayList<TableKey>> removals,
                                                 long tableSegmentOffset,
                                                 TableStore tableStore) throws Exception {
        val updateResult = removals.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> tableStore.remove(e.getKey(), e.getValue(), tableSegmentOffset, TIMEOUT)));
        Futures.allOf(updateResult.values()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    }

    private void executeRemovals(HashMap<String, ArrayList<TableKey>> removals, TableStore tableStore) throws Exception {
        val updateResult = removals.entrySet().stream()
                                   .collect(Collectors.toMap(Map.Entry::getKey, e -> tableStore.remove(e.getKey(), e.getValue(), TIMEOUT)));
        Futures.allOf(updateResult.values()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private HashMap<String, ArrayList<TableEntry>> generateUpdates(HashMap<BufferView, EntryData> keyInfo, boolean conditional, Random rnd) {
        val result = new HashMap<String, ArrayList<TableEntry>>();
        for (val e : keyInfo.entrySet()) {
            val ed = e.getValue();
            val newValue = generateValue(rnd);
            TableEntry te = conditional
                    ? TableEntry.versioned(e.getKey(), newValue, ed.getVersion())
                    : TableEntry.unversioned(e.getKey(), newValue);

            val segmentUpdate = result.computeIfAbsent(ed.segmentName, ignored -> new ArrayList<>());
            segmentUpdate.add(te);
        }

        return result;
    }

    private HashMap<String, ArrayList<TableKey>> generateRemovals(HashMap<BufferView, EntryData> keyInfo, boolean conditional) {
        val result = new HashMap<String, ArrayList<TableKey>>();
        for (val e : keyInfo.entrySet()) {
            val ed = e.getValue();
            TableKey tk = conditional
                    ? TableKey.versioned(e.getKey(), ed.getVersion())
                    : TableKey.unversioned(e.getKey());

            val segmentUpdate = result.computeIfAbsent(ed.segmentName, ignored -> new ArrayList<>());
            segmentUpdate.add(tk);
        }

        return result;
    }

    private void acceptUpdates(Map<String, ArrayList<TableEntry>> updatesBySegment, Map<String, List<Long>> versionsBySegment,
                               Map<BufferView, EntryData> keyInfo) {
        Assert.assertEquals(updatesBySegment.size(), versionsBySegment.size());
        for (val e : updatesBySegment.entrySet()) {
            val updates = e.getValue();
            val versions = versionsBySegment.get(e.getKey());
            Assert.assertEquals(updates.size(), versions.size());
            for (int i = 0; i < updates.size(); i++) {
                val u = updates.get(i);
                val ki = keyInfo.get(u.getKey().getKey());
                ki.setValue(u.getValue(), versions.get(i));
            }
        }
    }

    private void acceptRemovals(HashMap<String, ArrayList<TableKey>> removals, HashMap<BufferView, EntryData> keyInfo) {
        for (val removeSet : removals.values()) {
            for (val r : removeSet) {
                val ki = keyInfo.get(r.getKey());
                ki.deleteValue();
            }
        }
    }

    private HashMap<BufferView, EntryData> generateKeysForSegments(ArrayList<String> segments, Random rnd) {
        val result = new HashMap<BufferView, EntryData>();
        val keysPerSegment = KEY_COUNT / segments.size();
        for (val segmentName : segments) {
            ArrayList<BufferView> keys;
            if (isFixedKeyLength(segmentName)) {
                val keyLength = getFixedKeyLength(segmentName);
                keys = generateKeys(keysPerSegment, keyLength, keyLength, rnd);
            } else {
                keys = generateKeys(keysPerSegment, 1, MAX_KEY_LENGTH, rnd);
            }
            for (val key : keys) {
                result.put(key, new EntryData(segmentName));
            }
        }

        return result;
    }

    private BufferView generateValue(Random rnd) {
        return generateData(0, MAX_VALUE_LENGTH, rnd);
    }

    private ArrayList<BufferView> generateKeys(int keyCount, int minKeyLength, int maxKeyLength, Random rnd) {
        val result = new ArrayList<BufferView>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            result.add(generateData(minKeyLength, maxKeyLength, rnd));
        }

        return result;
    }

    private BufferView generateData(int minLength, int maxLength, Random rnd) {
        byte[] keyData = new byte[Math.max(minLength, rnd.nextInt(maxLength))];
        rnd.nextBytes(keyData);
        return new ByteArraySegment(keyData);
    }

    private ArrayList<String> createSegments(TableStore store, SegmentType... segmentTypes) throws Exception {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            val segmentType = segmentTypes[i % segmentTypes.length];
            String segmentName = getSegmentName(i, segmentType);
            segmentNames.add(segmentName);
            val config = TableSegmentConfig.builder();
            if (segmentType.isFixedKeyLengthTableSegment()) {
                config.keyLength(getFixedKeyLength(segmentName));
            }
            futures.add(store.createSegment(segmentName, segmentType, config.build(), TIMEOUT));
        }

        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return segmentNames;
    }

    private boolean isFixedKeyLength(String segmentName) {
        return segmentName.startsWith(TABLE_SEGMENT_FIXED_KEY_NAME_PREFIX);
    }

    private int getFixedKeyLength(int segmentIndex) {
        return FIXED_KEY_LENGTHS[segmentIndex % FIXED_KEY_LENGTHS.length];
    }

    private int getFixedKeyLength(String segmentName) {
        assert segmentName.startsWith(TABLE_SEGMENT_FIXED_KEY_NAME_PREFIX) : segmentName;
        return getFixedKeyLength(Integer.parseInt(segmentName.substring(TABLE_SEGMENT_FIXED_KEY_NAME_PREFIX.length())));
    }

    private static String getSegmentName(int i, SegmentType segmentType) {
        if (segmentType.isFixedKeyLengthTableSegment()) {
            return TABLE_SEGMENT_FIXED_KEY_NAME_PREFIX + i;
        } else if (segmentType.isTableSegment()) {
            return TABLE_SEGMENT_NAME_PREFIX + i;
        }

        throw new IllegalArgumentException(segmentType.toString());
    }

    private ServiceBuilder createBuilder() throws Exception {
        val builder = ServiceBuilder.newInMemoryBuilder(this.configBuilder.build())
                .withStorageFactory(setup -> this.storageFactory)
                .withDataLogFactory(setup -> this.durableDataLogFactory);
        try {
            builder.initialize();
        } catch (Throwable ex) {
            builder.close();
            throw ex;
        }
        return builder;
    }

    @RequiredArgsConstructor
    private static class EntryData {
        final String segmentName;
        private final AtomicLong version = new AtomicLong(TableKey.NOT_EXISTS);
        private final AtomicReference<BufferView> value = new AtomicReference<>(null);

        void setValue(BufferView value, long version) {
            this.value.set(value);
            this.version.set(version);
        }

        void deleteValue() {
            setValue(null, TableKey.NOT_EXISTS);
        }

        boolean isDeleted() {
            return this.version.get() == TableKey.NOT_EXISTS;
        }

        BufferView getValue() {
            return this.value.get();
        }

        long getVersion() {
            return this.version.get();
        }

        @Override
        public String toString() {
            return String.format("[%s @%s]: %s.", this.segmentName, this.version, this.value);
        }
    }

    private static class PermanentDurableDataLogFactory extends InMemoryDurableDataLogFactory {
        PermanentDurableDataLogFactory(ScheduledExecutorService executorService) {
            super(executorService);
        }

        @Override
        public void close() {
            // This method intentionally left blank; we want this factory to live between multiple recovery attempts.
        }
    }
}
