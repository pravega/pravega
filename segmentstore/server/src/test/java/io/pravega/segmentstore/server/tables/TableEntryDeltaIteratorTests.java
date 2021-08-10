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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableEntryDeltaIterator} class. It tests various scenarios, without consideration of compaction.
 */
@Slf4j
public class TableEntryDeltaIteratorTests extends ThreadPooledTestSuite {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final long SEGMENT_ID = 2L;
    private static final String SEGMENT_NAME = "TableSegment";

    private static final int NUM_ENTRIES = 10;
    private static final int MAX_UPDATE_COUNT = 10000;
    private static final TableExtensionConfig CONFIG = TableExtensionConfig.builder()
            .with(TableExtensionConfig.MAX_COMPACTION_SIZE, 50000)
            .build();

    private static final TableEntry NON_EXISTING_ENTRY = TableEntry.notExists(
            new ByteArraySegment("NULL".getBytes()),
            new ByteArraySegment("NULL".getBytes()));


    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test
    public void testEmptySegment() {
        TestData data = createTestData(0);
        test(data, NON_EXISTING_ENTRY);
    }

    @Test
    public void testUsingInvalidArgs() {
        @Cleanup
        TableContext context = new TableContext(CONFIG, executorService());
        context.ext.createSegment(SEGMENT_NAME, SegmentType.TABLE_SEGMENT_HASH, TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "entryDeltaIterator should throw an IllegalArgumentException if 'fromPosition' > segment length.",
                () -> context.ext.entryDeltaIterator(SEGMENT_NAME, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testEmptyIterator() throws Exception {
        val empty = TableEntryDeltaIterator.empty();
        val next = empty.getNext().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Empty iterator should return null from getNext().", next);
    }

    @Test
    public void testFromStart() {
        TestData data = createTestData(NUM_ENTRIES);
        test(data, data.getExpectedStartEntry(0));
    }

    @Test
    public void testFromMid() {
        TestData data = createTestData(NUM_ENTRIES);
        test(data, data.getExpectedStartEntry(NUM_ENTRIES / 2));
    }

    @Test
    @SneakyThrows
    public void testWithConcurrentAdds() {
        TestData data = createTestData(NUM_ENTRIES);
        test(data, data.getExpectedStartEntry(NUM_ENTRIES-1));
        data.concurrentlyAddEntries(TestData.generateEntries(NUM_ENTRIES, NUM_ENTRIES)).join();
    }

    @Test
    public void testWithCompaction() throws ExecutionException, InterruptedException {
        TestData data = createTestData(NUM_ENTRIES);
        // Initialize our TableSegment with TableEntries in order to peform updates for compaction.
        data.setActualEntries(data.getEntriesFromIteration(data.getExpectedStartEntry(0).getKey()));
        data.generateUntilCompacted().thenAccept(truncatedEntry -> test(data, truncatedEntry)).get();
    }

    /**
     * Tests the {@link TableIterator#empty()} method.
     */
    @Test
    public void testEmpty() throws Exception {
        val empty = TableIterator.empty();
        val next = empty.getNext().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNull("Empty iterator should return null from getNext().", next);
    }

    @SneakyThrows
    private void test(TestData testData, TableEntry startEntry) {
        val actual = testData.getEntriesFromIteration(startEntry.getKey());
        // Get a sub-list, starting at the same TableKey in the 'expected' list.
        List<TableEntry> expected =  new ArrayList<>();
        for (int i = 0; i < testData.getExpected().size(); i++) {
            if (compareKeys(testData.getExpected().get(i), startEntry)) {
                expected = testData.getExpected().subList(i, testData.getExpected().size());
                break;
            }
        }
        testData.setActualEntries(actual);

        Assert.assertEquals(actual.size(), expected.size());
        AssertExtensions.assertListEquals("Assert equivalency by TableKey and Value (ignoring version).", expected, actual,
                (t1, t2) -> compareKeys(t1, t2) &&  t1.getValue().equals(t2.getValue()));
    }

    private TestData createTestData(int numEntriesToGenerate) {
        TestData data = TestData.builder()
                .context(new TableContext(CONFIG, executorService()))
                .executorService(executorService())
                .reader(new IndexReader(executorService()))
                .expected(TestData.generateEntries(numEntriesToGenerate, 0))
                .actual(Collections.emptyList())
                .iterating(new AtomicReference<>(false))
                .build();
        // Setup necessary TableSegment state.
        data.createSegment();
        data.addEntries(data.expected);

        return data;
    }

    private boolean compareKeys(TableEntry t1, TableEntry t2) {
        return t1.getKey().getKey().equals(t2.getKey().getKey());
    }

    @Builder
    @NotThreadSafe
    private static class TestData {

        private static final String KEY_PREFIX = "KEY";
        private static final Random RANDOM = new Random(0);

        private final IndexReader reader;
        private final TableContext context;
        private final ScheduledExecutorService executorService;

        @Getter
        private final List<TableEntry> expected;
        private List<TableEntry> actual;
        // Flag used to indicate we can be doing various concurrent operations on our TableSegment.
        private final AtomicReference<Boolean> iterating;

        private final AtomicInteger entriesCreated;

        // Continually update a particular key until some compaction occurs.Then end result should be a set of unique
        // TableKeys (after iteration), and the update Key maps to it's most recent value.
        @SneakyThrows
        public CompletableFuture<TableEntry> generateUntilCompacted() {
            Assert.assertFalse(actual.isEmpty());

            context.segment().updateAttributes(Collections.singletonMap(TableAttributes.MIN_UTILIZATION, 99L));
            @Cleanup
            val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);

            int i = RANDOM.nextInt(actual.size());
            TableEntry toUpdate = actual.get(i);
            AtomicReference<TableEntry> mostRecent = new AtomicReference<>(toUpdate);
            for (int numUpdates = 0; !hasCompacted() && numUpdates < MAX_UPDATE_COUNT; numUpdates++) {
                TableEntry entry = generateUpdatedKey(mostRecent.get());
                addToProcessor(
                        () -> context.ext.put(SEGMENT_NAME, Collections.singletonList(entry), TIMEOUT),
                        processor,
                        context.segment().getInfo()::getLength).thenAccept(value -> {
                            @SuppressWarnings("unchecked")
                            List<Long> version = (List<Long>) value;
                            mostRecent.set(TableEntry.versioned(entry.getKey().getKey(), entry.getValue(), version.iterator().next()));
                }).get();
            }
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            // Must remove previous (now compacted) entry.
            expected.remove(i);
            // If compaction just happened, the most recently added Entry should be the head of the TableSegment.
            expected.add(0, mostRecent.get());
            Assert.assertTrue("The TableSegment has not performed any compactions.", hasCompacted());

            log.info("Compaction Offset: {} Entry Version: {}", IndexReader.getCompactionOffset(context.segment().getInfo()), toUpdate.getKey().getVersion());
            // The initial key is returned because we want to test iteration when using a key that has been truncated due to compaction.
            return CompletableFuture.completedFuture(toUpdate);
        }

        @SneakyThrows
        private CompletableFuture<?> addToProcessor(Supplier<CompletableFuture<?>> action, WriterTableProcessor processor, Supplier<Long> getSegmentLength) {
            // Determine the length of the modified data.
            int initialLength = (int) (long) getSegmentLength.get();
            val ret = action.get().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            int newLength = (int) (long) getSegmentLength.get();
            AssertExtensions.assertGreaterThan("Expected segment length to increase.", initialLength, newLength);

            // Queue up an append operation. We don't care about the payload, as the data have already been added to the segment.
            addToProcessor(initialLength, newLength - initialLength, processor);

            return CompletableFuture.completedFuture(ret);
        }

        @SneakyThrows(DataCorruptionException.class)
        private void addToProcessor(long offset, int length, WriterTableProcessor processor) {
            val op = new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(new byte[length]), null);
            op.setStreamSegmentOffset(offset);
            op.setSequenceNumber(offset);
            processor.add(new CachedStreamSegmentAppendOperation(op));
        }

        public static List<TableEntry> generateEntries(int numEntries, int keyLowerBound) {
            List<TableEntry> entries = new ArrayList<>();
            for (int i = 0; i < numEntries; i++) {
                    entries.add(generateEntry(i + keyLowerBound));
            }
            return entries;
        }

        public static TableEntry generateEntry(int i) {
            byte[] val = new byte[16];
            RANDOM.nextBytes(val);
            return TableEntry.unversioned(
                    new ByteArraySegment((KEY_PREFIX + i).getBytes()),
                    new ByteArraySegment(val));
        }

        public static TableEntry generateUpdatedKey(TableEntry entry) {
            byte[] val = new byte[16];
            RANDOM.nextBytes(val);
            return TableEntry.versioned(entry.getKey().getKey(), new ByteArraySegment(val), entry.getKey().getVersion());
        }

        @SneakyThrows
        public void addEntries(List<TableEntry> entries) {
            if (entries.isEmpty()) {
                return;
            }

            @Cleanup
            val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
            addToProcessor(
                    () -> context.ext.put(SEGMENT_NAME, entries, TIMEOUT),
                    processor,
                    context.segment().getInfo()::getLength).get();
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        public CompletableFuture<Void> concurrentlyAddEntries(List<TableEntry> entries) {
            // Wait until we have began iteration.
            Futures.loop(() -> !iterating.get(), () -> CompletableFuture.completedFuture(null), this.executorService);
            return Futures.toVoid(context.ext.put(SEGMENT_NAME, entries, TIMEOUT));
        }

        public void removeEntries(List<TableEntry> entries) {
            List<TableKey> keys = entries.stream().map(TableEntry::getKey).collect(Collectors.toList());
            context.ext.remove(SEGMENT_NAME, keys, TIMEOUT);
        }

        public List<TableEntry> getEntriesFromIteration(TableKey startKey) throws ExecutionException, InterruptedException {
            List<TableEntry> startEntry = context.ext.get(SEGMENT_NAME, Collections.singletonList(startKey.getKey()), TIMEOUT).get();
            // If TableKey DNE, start from 0.
            TableEntry entry = startEntry.iterator().next();
            long startOffset = (entry == null) ? 0 : entry.getKey().getVersion();
            val iterator = context.ext.entryDeltaIterator(SEGMENT_NAME, startOffset, TIMEOUT).get();

            List<TableEntry> observed = new ArrayList<>();
            val iteration = iterator.forEachRemaining(item -> {
                iterating.set(true);
                observed.addAll(item.getEntries());
            }, executorService).get();
            iterating.set(false);

            return observed;
        }

        public void setActualEntries(List<TableEntry> entries) {
            this.actual = entries;
        }

        public TableEntry getExpectedStartEntry(int i) {
            return expected.get(i);
        }

        public boolean hasCompacted() {
            return IndexReader.getCompactionOffset(context.segment().getInfo()) > 0;
        }

        public void createSegment() {
            context.ext.createSegment(SEGMENT_NAME, SegmentType.TABLE_SEGMENT_HASH, TIMEOUT).join();
        }

    }

}
