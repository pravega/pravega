/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
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
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static final Duration TIMEOUT = Duration.ofSeconds(30); // Individual call timeout
    @Rule
    public Timeout globalTimeout = new Timeout((int) TIMEOUT.toMillis() * 4, TimeUnit.MILLISECONDS);

    private final ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
            .builder()
            .include(ServiceConfig
                    .builder()
                    .with(ServiceConfig.CONTAINER_COUNT, 4)
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
     */
    @Test
    public void testEndToEnd() throws Exception {
        val rnd = new Random(0);
        ArrayList<String> segmentNames;
        HashMap<HashedArray, EntryData> keyInfo;

        // Phase 1: Create some segments and update some data (unconditionally).
        log.info("Starting Phase 1");
        try (val builder = createBuilder()) {
            val tableStore = builder.createTableStoreService();

            // Create the Table Segments.
            segmentNames = createSegments(tableStore);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));

            // Generate the keys and map them to segments.
            keyInfo = mapToSegments(generateKeys(rnd), segmentNames);

            // Unconditional updates.
            val updates = generateUpdates(keyInfo, false, rnd);
            val updateVersions = executeUpdates(updates, tableStore);
            acceptUpdates(updates, updateVersions, keyInfo);
            log.info("Finished unconditional updates.");

            // Check.
            check(keyInfo, tableStore);

            log.info("Finished Phase 1");
        }

        // Phase 2: Force a recovery and remove all data (unconditionally
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
            log.info("Finished conditional updates.");

            // Check.
            check(keyInfo, tableStore);

            // Conditional remove.
            val removals = generateRemovals(keyInfo, true);
            executeRemovals(removals, tableStore);
            acceptRemovals(removals, keyInfo);
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
            val deletions = segmentNames.stream().map(s -> tableStore.deleteSegment(s, TIMEOUT)).collect(Collectors.toList());
            Futures.allOf(deletions).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            log.info("Finished Phase 4");
        }
    }

    private void check(HashMap<HashedArray, EntryData> keyInfo, TableStore tableStore) throws Exception {
        val bySegment = keyInfo.entrySet().stream()
                               .collect(Collectors.groupingBy(e -> e.getValue().segmentName));

        // Check inexistent keys.
        val searchFutures = new ArrayList<CompletableFuture<List<TableEntry>>>();
        val expectedResult = new ArrayList<Map.Entry<HashedArray, EntryData>>();
        for (val e : bySegment.entrySet()) {
            String segmentName = e.getKey();
            val keys = new ArrayList<ArrayView>();
            for (val se : e.getValue()) {
                keys.add(se.getKey());
                expectedResult.add(se);
            }

            searchFutures.add(tableStore.get(segmentName, keys, TIMEOUT));
        }

        val actualResults = Futures.allOfWithResults(searchFutures).get()//TODO.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                   .stream().flatMap(List::stream).collect(Collectors.toList());

        Assert.assertEquals("Unexpected number of search results.", expectedResult.size(), actualResults.size());
        for (int i = 0; i < expectedResult.size(); i++) {
            val expectedKey = expectedResult.get(i).getKey();
            val expectedEntry = expectedResult.get(i).getValue();
            val actual = actualResults.get(i);
            if (expectedEntry.isDeleted()) {
                // Deleted keys will be returned as nulls.
                Assert.assertNull("Not expecting a value for a deleted Key", actual);
            } else {
                Assert.assertTrue("Unexpected value for non-deleted Key.", HashedArray.arrayEquals(expectedEntry.getValue(), actual.getValue()));
                Assert.assertTrue("Unexpected key for non-deleted Key.", HashedArray.arrayEquals(expectedKey, actual.getKey().getKey()));
                Assert.assertEquals("Unexpected TableKey.Version for non-deleted Key.", expectedEntry.getVersion(), actual.getKey().getVersion());
            }
        }
    }

    private Map<String, List<Long>> executeUpdates(HashMap<String, ArrayList<TableEntry>> updates, TableStore tableStore) throws Exception {
        val updateResult = updates.entrySet().stream()
                                  .collect(Collectors.toMap(Map.Entry::getKey, e -> tableStore.put(e.getKey(), e.getValue(), TIMEOUT)));
        return Futures.allOfWithResults(updateResult).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void executeRemovals(HashMap<String, ArrayList<TableKey>> removals, TableStore tableStore) throws Exception {
        val updateResult = removals.entrySet().stream()
                                   .collect(Collectors.toMap(Map.Entry::getKey, e -> tableStore.remove(e.getKey(), e.getValue(), TIMEOUT)));
        Futures.allOf(updateResult.values()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private HashMap<String, ArrayList<TableEntry>> generateUpdates(HashMap<HashedArray, EntryData> keyInfo, boolean conditional, Random rnd) {
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

    private HashMap<String, ArrayList<TableKey>> generateRemovals(HashMap<HashedArray, EntryData> keyInfo, boolean conditional) {
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
                               Map<HashedArray, EntryData> keyInfo) {
        Assert.assertEquals(updatesBySegment.size(), versionsBySegment.size());
        for (val e : updatesBySegment.entrySet()) {
            val updates = e.getValue();
            val versions = versionsBySegment.get(e.getKey());
            Assert.assertEquals(updates.size(), versions.size());
            for (int i = 0; i < updates.size(); i++) {
                val u = updates.get(i);
                val ki = keyInfo.get(new HashedArray(u.getKey().getKey()));
                ki.setValue(u.getValue(), versions.get(i));
            }
        }
    }

    private void acceptRemovals(HashMap<String, ArrayList<TableKey>> removals, HashMap<HashedArray, EntryData> keyInfo) {
        for (val removeSet : removals.values()) {
            for (val r : removeSet) {
                val ki = keyInfo.get(new HashedArray(r.getKey()));
                ki.deleteValue();
            }
        }
    }

    private HashMap<HashedArray, EntryData> mapToSegments(ArrayList<HashedArray> keys, ArrayList<String> segments) {
        val result = new HashMap<HashedArray, EntryData>();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), new EntryData(segments.get(i % segments.size())));
        }

        return result;
    }

    private HashedArray generateValue(Random rnd) {
        return generateData(0, MAX_VALUE_LENGTH, rnd);
    }

    private ArrayList<HashedArray> generateKeys(Random rnd) {
        val result = new ArrayList<HashedArray>(KEY_COUNT);
        for (int i = 0; i < KEY_COUNT; i++) {
            result.add(generateData(1, MAX_KEY_LENGTH, rnd));
        }

        return result;
    }

    private HashedArray generateData(int minLength, int maxLength, Random rnd) {
        byte[] keyData = new byte[Math.max(minLength, rnd.nextInt(maxLength))];
        rnd.nextBytes(keyData);
        return new HashedArray(keyData);
    }

    private ArrayList<String> createSegments(TableStore store) throws Exception {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(store.createSegment(segmentName, TIMEOUT));
        }

        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return segmentNames;
    }

    private static String getSegmentName(int i) {
        return "TableSegment_" + i;
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
        private final AtomicReference<ArrayView> value = new AtomicReference<>(null);

        void setValue(ArrayView value, long version) {
            this.value.set(value);
            this.version.set(version);
        }

        void deleteValue() {
            setValue(null, TableKey.NOT_EXISTS);
        }

        boolean isDeleted() {
            return this.version.get() == TableKey.NOT_EXISTS;
        }

        ArrayView getValue() {
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
