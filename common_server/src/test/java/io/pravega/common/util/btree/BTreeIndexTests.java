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
package io.pravega.common.util.btree;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the BTreeIndex class.
 */
public class BTreeIndexTests extends ThreadPooledTestSuite {
    private static final BufferViewComparator KEY_COMPARATOR = BufferViewComparator.create();
    private static final int KEY_LENGTH = 4;
    private static final int VALUE_LENGTH = 2;
    private static final int MAX_PAGE_SIZE = 128;
    private static final int MAX_ENTRIES_PER_PAGE = MAX_PAGE_SIZE / (KEY_LENGTH + VALUE_LENGTH) - 2;

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the put() method sequentially making sure we do not split the root page.
     */
    @Test
    public void testInsertNoSplitSequential() {
        testInsert(MAX_ENTRIES_PER_PAGE, false, false);
    }

    /**
     * Tests the put() method using bulk-loading making sure we do not split the root page.
     */
    @Test
    public void testInsertNoSplitBulk() {
        testInsert(MAX_ENTRIES_PER_PAGE, false, true);
    }

    /**
     * Tests the put() method sequentially using already sorted entries.
     */
    @Test
    public void testInsertSortedSequential() {
        testInsert(10000, true, false);
    }

    /**
     * Tests the put() method sequentially using unsorted entries.
     */
    @Test
    public void testInsertRandomSequential() {
        testInsert(10000, false, false);
    }

    /**
     * Tests the put() method using bulk-loading with sorted entries.
     */
    @Test
    public void testInsertSortedBulk() {
        testInsert(10000, true, true);
    }

    /**
     * Tests the put() method using bulk-loading with unsorted entries.
     */
    @Test
    public void testInsertRandomBulk() {
        testInsert(10000, false, true);
    }

    /**
     * Tests the remove() method sequentially.
     */
    @Test
    public void testRemoveSequential() {
        testDelete(1000, 1);
    }

    /**
     * Tests the remove() method using multiple keys at once.
     */
    @Test
    public void testRemoveBulk() {
        testDelete(10000, 123);
    }

    /**
     * Tests the remove() method for all the keys at once.
     */
    @Test
    public void testRemoveAll() {
        final int count = 10000;
        testDelete(count, count);
    }

    /**
     * Tests the put() method with the ability to replace entries.
     */
    @Test
    public void testUpdate() {
        final int count = 10000;
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        index.initialize(TIMEOUT).join();
        val entries = generate(count);
        sort(entries);
        index.update(entries, TIMEOUT).join();

        // Delete every 1/3 of the keys
        val toUpdate = new ArrayList<PageEntry>();
        val expectedEntries = new ArrayList<PageEntry>(entries);
        val rnd = new Random(0);
        for (int i = entries.size() - 1; i >= 0; i--) {
            PageEntry e = expectedEntries.get(i);
            boolean delete = i % 3 == 0;
            boolean update = i % 2 == 0;
            if (delete && !update) {
                // Delete about 1/3 of the entries.
                toUpdate.add(PageEntry.noValue(expectedEntries.get(i).getKey()));
                    expectedEntries.remove(i);
            }

            if (update) {
                // Update (reinsert or update) 1/2 of the entries.
                val newValue = new byte[VALUE_LENGTH];
                rnd.nextBytes(newValue);
                e = new PageEntry(e.getKey(), new ByteArraySegment(newValue));
                toUpdate.add(e);
                expectedEntries.set(i, e);
            }
        }

        // Perform the removals and updates.
        index.update(toUpdate, TIMEOUT).join();

        // Verify final result.
        check("Unexpected index contents.", index, expectedEntries, 0);
    }

    /**
     * Tests the get() method. getBulk() is already extensively tested in other tests, so we are not explicitly testing it here.
     */
    @Test
    public void testGet() {
        final int count = 500;
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        index.initialize(TIMEOUT).join();
        val entries = generate(count);
        for (val e : entries) {
            index.update(Collections.singleton(e), TIMEOUT).join();
            val value = index.get(e.getKey(), TIMEOUT).join();
            Assert.assertEquals("Unexpected key.", e.getValue(), value);
        }
    }

    /**
     * Tests the ability to iterate through entries using {@link BTreeIndex#iterator}.
     */
    @Test
    public void testIterator() {
        final int count = 1000;
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        index.initialize(TIMEOUT).join();
        val entries = generate(count);
        index.update(entries, TIMEOUT).join();
        sort(entries);

        for (int i = 0; i < entries.size() / 2; i++) {
            int startIndex = i;
            int endIndex = entries.size() - i - 1;
            ByteArraySegment firstKey = entries.get(startIndex).getKey();
            ByteArraySegment lastKey = entries.get(endIndex).getKey();

            // We make sure that throughout the test we check all possible combinations of firstInclusive & lastInclusive.
            boolean firstInclusive = i % 2 == 0;
            boolean lastInclusive = i % 4 < 2;
            if (i == entries.size() / 2) {
                // For same keys, they must both be inclusive.
                firstInclusive = true;
                lastInclusive = true;
            }

            val iterator = index.iterator(firstKey, firstInclusive, lastKey, lastInclusive, TIMEOUT);
            val actualEntries = new ArrayList<PageEntry>();
            iterator.forEachRemaining(actualEntries::addAll, executorService()).join();

            // Determine expected keys.
            if (!firstInclusive) {
                startIndex++;
            }
            if (!lastInclusive) {
                endIndex--;
            }

            val expectedEntries = entries.subList(startIndex, endIndex + 1);
            AssertExtensions.assertListEquals("Wrong result for " + i + ".", expectedEntries, actualEntries,
                    (e, a) -> KEY_COMPARATOR.compare(e.getKey(), a.getKey()) == 0 && KEY_COMPARATOR.compare(e.getValue(), a.getValue()) == 0);
        }
    }

    /**
     * Tests the behavior of the index when there are data source write errors.
     */
    @Test
    public void testWriteErrors() {
        final int count = 1000;
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        index.initialize(TIMEOUT).join();
        val entries = generate(count);
        index.update(entries, TIMEOUT).join();

        val newEntry = generateEntry(Byte.MAX_VALUE, (byte) 0);
        ds.setWriteInterceptor(Futures.failedFuture(new IntentionalException()));
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected an exception during write.",
                () -> index.update(Collections.singleton(newEntry), TIMEOUT),
                ex -> ex instanceof IntentionalException);

        check("Not expecting any change after failed write.", index, entries, 0);
        ds.setWriteInterceptor(null);
        index.update(Collections.singleton(newEntry), TIMEOUT).join();
        entries.add(newEntry);
        check("Expecting the new entry to have been added.", index, entries, 0);
    }

    /**
     * Tests the behavior of the index when it identifies extraneous data written to the file, but it has an accurate
     * root pointer to read from.
     */
    @Test
    public void testRootPointer() {
        final int count = 100;
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        index.initialize(TIMEOUT).join();
        val entries = generate(count);
        index.update(entries, TIMEOUT).join();

        // Verify index.
        check("after insert", index, entries, 0);

        // Corrupt the tail part of the index (simulate a partial update).
        ds.appendData(new byte[1234]);

        // Verify index after a full recovery.
        val recoveredIndex = defaultBuilder(ds).build();
        recoveredIndex.initialize(TIMEOUT).join();
        check("after recovery", recoveredIndex, entries, 0);

        // Now verify how it would behave if we had no root pointer. An exception should be thrown.
        ds.rootPointer.set(-1L);
        val corruptedIndex = defaultBuilder(ds).build();
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected corrupted index to fail initialization.",
                () -> corruptedIndex.initialize(TIMEOUT),
                ex -> ex instanceof IllegalArgumentException); // Captures IllegalDataFormatException as well.
    }

    /**
     * Tests the behavior of the BTreeIndex when there are more than one writers modifying the index at the same time.
     */
    @Test
    public void testConcurrentModification() {
        final int count = 1000;
        val ds1 = new DataSource();
        ds1.setCheckOffsets(false); // Disable offset checking in this case; it's really hard to keep track of the right values.
        val index1 = defaultBuilder(ds1).build();
        index1.initialize(TIMEOUT).join();
        val entries1 = generate(count);
        long version = index1.update(entries1, TIMEOUT).join();

        // Create a second index using a cloned DataSource, but which share the exact storage.
        val ds2 = new DataSource(ds1);
        ds2.setCheckOffsets(false);
        val index2 = defaultBuilder(ds2).build();
        index2.initialize(TIMEOUT).join();
        check("Expected second index to be identical prior to any change.", index2, entries1, 0);

        // We will try to add one entry to one index, and another to the second index. Both have the same keys but different values.
        val newEntry1 = generateEntry(Byte.MAX_VALUE, (byte) 1);
        val newEntry2 = generateEntry(Byte.MAX_VALUE, (byte) 2);

        // We initiate the update on the first index, but delay it. At this point, the first index should have made
        // the modification "in memory", but not persist it yet.
        CompletableFuture<Void> writeDelay = new CompletableFuture<>();
        ds1.setWriteInterceptor(writeDelay);
        CompletableFuture<Long> update1 = index1.update(Collections.singleton(newEntry1), TIMEOUT);
        Assert.assertFalse("Not expecting first index's update to be completed yet.", update1.isDone());

        // We initiate the update on the second index. This should succeed right away.
        long version2 = index2.update(Collections.singleton(newEntry2), TIMEOUT).join();
        AssertExtensions.assertGreaterThan("Expected a larger version.", version, version2);
        val entries2 = new ArrayList<PageEntry>(entries1);
        entries2.add(newEntry2);
        check("Expected second index to reflect changes.", index2, entries2, 0);
        check("Expected first index to not reflect changes yet.", index1, entries1, 0);

        // "Release" the first write.
        writeDelay.complete(null);
        AssertExtensions.assertThrows(
                "Expected the first update to have failed.",
                update1::join,
                ex -> ex instanceof IllegalArgumentException);

        // Verify that the first index is now in a bad state. This is by design, as we don't want it to auto-pick up the
        // correct index state and thus allow possibly out-of-date updates to occur. The solution in that case is to
        // reinstantiate the BTreeIndex which will force a refresh.
        AssertExtensions.assertThrows(
                "Expected the first index be in a bad state.",
                update1::join,
                ex -> ex instanceof IllegalArgumentException);

        // Verify that only the correct updates made it to the data source.
        val index3 = defaultBuilder(ds1).build();
        index3.initialize(TIMEOUT).join();
        check("Expected recovered index to reflect changes now", index3, entries2, 0);
    }

    /**
     * Tests the behavior of {@link BTreeIndex.BTreeIndexBuilder#maintainStatistics(boolean)} when disabled and eventually
     * enabled (on a previously disabled index). This simulates an "accidental upgrade" - we cannot compute statistics on
     * segments that haven't had statistics computed from the beginning.
     */
    @Test
    public void testMaintainStatisticsDisabled() {
        // Normal operations (start fresh).
        val ds1 = new DataSource();
        val index1 = defaultBuilder(ds1).maintainStatistics(false).build();
        index1.initialize(TIMEOUT).join();
        Assert.assertNull(index1.getStatistics());
        val allEntries = generate(3);
        allEntries.sort((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()));

        index1.update(allEntries.subList(0, 1), TIMEOUT).join();
        Assert.assertNull(index1.getStatistics());
        index1.update(allEntries.subList(1, 2), TIMEOUT).join();
        Assert.assertNull(index1.getStatistics());

        // Test recovery.
        val index2 = defaultBuilder(ds1).maintainStatistics(false).build();
        index2.initialize(TIMEOUT).join();
        Assert.assertNull(index2.getStatistics());
        index2.update(allEntries.subList(2, 3), TIMEOUT).join();

        val expectedValues = allEntries.stream()
                .map(PageEntry::getValue)
                .collect(Collectors.toList());
        val actualValues = index2.get(allEntries.stream().map(PageEntry::getKey).collect(Collectors.toList()), TIMEOUT).join();
        AssertExtensions.assertListEquals("Unexpected results after recovery.", expectedValues, actualValues, Object::equals);

        // Test "upgrade" to maintainStatistics==true
        val index3 = defaultBuilder(ds1).maintainStatistics(true).build();
        index3.initialize(TIMEOUT).join();
        Assert.assertNull(index3.getStatistics());
        val actualValues2 = index3.get(allEntries.stream().map(PageEntry::getKey).collect(Collectors.toList()), TIMEOUT).join();
        AssertExtensions.assertListEquals("Unexpected results after recovery and upgrade.", expectedValues, actualValues2, Object::equals);

        // Make one modification and verify it still works (remove one entry).
        index3.update(Collections.singletonList(PageEntry.noValue(allEntries.get(0).getKey())), TIMEOUT).join();
        expectedValues.set(0, null);
        val actualValues3 = index3.get(allEntries.stream().map(PageEntry::getKey).collect(Collectors.toList()), TIMEOUT).join();
        AssertExtensions.assertListEquals("Unexpected results after recovery and upgrade.", expectedValues, actualValues3, Object::equals);
        Assert.assertNull(index3.getStatistics());

        // Now remove all entries.
        index3.update(allEntries.stream().map(e -> PageEntry.noValue(e.getKey())).collect(Collectors.toList()), TIMEOUT).join();

        // Final recovery - empty index.
        val index4 = defaultBuilder(ds1).maintainStatistics(true).build();
        index4.initialize(TIMEOUT).join();
        Assert.assertNull(index4.getStatistics());
    }

    private void testDelete(int count, int deleteBatchSize) {
        final int checkEvery = count / 10; // checking is very expensive; we don't want to do it every time.
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        index.initialize(TIMEOUT).join();
        val entries = generate(count);
        long lastRetVal = index.update(entries, TIMEOUT).join();

        int firstIndex = 0;
        int lastCheck = -1;
        while (firstIndex < count) {
            int batchSize = Math.min(deleteBatchSize, count - firstIndex);
            val toDelete = entries.subList(firstIndex, firstIndex + batchSize)
                    .stream().map(e -> PageEntry.noValue(e.getKey())).collect(Collectors.toList());
            long retVal = index.update(toDelete, TIMEOUT).join();
            AssertExtensions.assertGreaterThan("Expecting return value to increase.", lastRetVal, retVal);

            // Determine if it's time to check the index.
            if (firstIndex - lastCheck > checkEvery) {
                // Search for all entries, and make sure only the ones we care about are still there.
                check("after deleting " + (firstIndex + 1), index, entries, firstIndex + batchSize);
                int keyCount = getKeyCount(index);
                Assert.assertEquals("Unexpected index key count after deleting " + (firstIndex + 1),
                        entries.size() - firstIndex - batchSize, keyCount);
                lastCheck = firstIndex;
            }

            firstIndex += batchSize;
        }

        // Verify again, now that we have an empty index.
        check("at the end", index, entries, entries.size());
        val finalKeyCount = getKeyCount(index);
        Assert.assertEquals("Not expecting any keys after deleting everything.", 0, finalKeyCount);

        // Verify again, after a full recovery.
        val recoveredIndex = defaultBuilder(ds).build();
        recoveredIndex.initialize(TIMEOUT).join();
        check("after recovery", recoveredIndex, entries, entries.size());
    }

    private int getKeyCount(BTreeIndex index) {
        val minKey = new byte[KEY_LENGTH];
        Arrays.fill(minKey, BufferViewComparator.MIN_VALUE);
        val maxKey = new byte[KEY_LENGTH];
        Arrays.fill(maxKey, BufferViewComparator.MAX_VALUE);

        val count = new AtomicInteger();
        index.iterator(new ByteArraySegment(minKey), true, new ByteArraySegment(maxKey), true, TIMEOUT)
             .forEachRemaining(k -> count.addAndGet(k.size()), executorService()).join();
        return count.get();
    }

    private void testInsert(int count, boolean sorted, boolean bulk) {
        val ds = new DataSource();
        val index = defaultBuilder(ds).build();
        index.initialize(TIMEOUT).join();
        val entries = generate(count);
        if (sorted) {
            sort(entries);
        }

        if (bulk) {
            index.update(entries, TIMEOUT).join();
        } else {
            long lastRetVal = 0;
            for (val e : entries) {
                long retVal = index.update(Collections.singleton(e), TIMEOUT).join();
                AssertExtensions.assertGreaterThan("Expecting return value to increase.", lastRetVal, retVal);
                lastRetVal = retVal;
            }
        }

        // Verify index.
        check("after insert", index, entries, 0);

        // Verify index after a full recovery.
        val recoveredIndex = defaultBuilder(ds).build();
        recoveredIndex.initialize(TIMEOUT).join();
        check("after recovery", recoveredIndex, entries, 0);
    }

    private void check(String message, BTreeIndex index, List<PageEntry> entries, int firstValidEntryIndex) {
        val keys = entries.stream().map(PageEntry::getKey).collect(Collectors.toList());
        val actualValues = index.get(keys, TIMEOUT).join();

        // Bulk-get returns a list of values in the same order as the keys, so we need to match up on the indices.
        Assert.assertEquals("Unexpected key count.", keys.size(), actualValues.size());
        int entryCount = 0;
        for (int i = 0; i < keys.size(); i++) {
            val av = actualValues.get(i);
            if (i < firstValidEntryIndex) {
                Assert.assertNull("Not expecting a result for index " + i, av);
            } else {
                val expectedValue = entries.get(i).getValue();
                Assert.assertEquals(message + ": value mismatch for entry index " + i, expectedValue, av);
            }

            if (av != null) {
                entryCount++;
            }
        }

        val stats = index.getStatistics();
        Assert.assertEquals("Unexpected entry count.", entryCount, stats.getEntryCount());
        if (entryCount <= MAX_ENTRIES_PER_PAGE) {
            Assert.assertEquals(1, stats.getPageCount());
        }
    }

    private ArrayList<PageEntry> generate(int count) {
        val result = new ArrayList<PageEntry>(count);
        val rnd = new Random(count);
        for (int i = 0; i < count; i++) {
            val key = new byte[KEY_LENGTH];
            val value = new byte[VALUE_LENGTH];
            rnd.nextBytes(key);
            rnd.nextBytes(value);
            result.add(new PageEntry(new ByteArraySegment(key), new ByteArraySegment(value)));
        }

        return result;
    }

    private void sort(List<PageEntry> entries) {
        entries.sort((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()));
    }

    private PageEntry generateEntry(byte keyByte, byte valueByte) {
        val key = new ByteArraySegment(new byte[KEY_LENGTH]);
        Arrays.fill(key.array(), keyByte);
        val newValue = new ByteArraySegment(new byte[VALUE_LENGTH]);
        Arrays.fill(newValue.array(), valueByte);
        return new PageEntry(key, newValue);
    }

    private BTreeIndex.BTreeIndexBuilder defaultBuilder(DataSource ds) {
        return BTreeIndex.builder()
                .maxPageSize(MAX_PAGE_SIZE)
                .keyLength(KEY_LENGTH)
                .valueLength(VALUE_LENGTH)
                .maintainStatistics(true)
                .readPage(ds::read)
                .writePages(ds::write)
                .getLength(ds::getLength)
                .executor(executorService());
    }


    @ThreadSafe
    private class DataSource {
        @GuardedBy("data")
        private final ByteBufferOutputStream data;
        private final AtomicLong rootPointer;
        @GuardedBy("data")
        private final HashMap<Long, Boolean> offsets; // Key: Offset, Value: valid(true), obsolete(false).
        private final AtomicReference<CompletableFuture<Void>> writeInterceptor = new AtomicReference<>();
        private final AtomicBoolean checkOffsets = new AtomicBoolean(true);

        DataSource() {
            this.data = new ByteBufferOutputStream();
            this.rootPointer = new AtomicLong(BTreeIndex.IndexInfo.EMPTY.getRootPointer());
            this.offsets = new HashMap<>();
        }

        DataSource(DataSource other) {
            this.data = other.data;
            this.rootPointer = other.rootPointer;
            this.offsets = new HashMap<>();
        }

        void setWriteInterceptor(CompletableFuture<Void> wi) {
            this.writeInterceptor.set(wi);
        }

        void setCheckOffsets(boolean check) {
            this.checkOffsets.set(check);
        }

        CompletableFuture<BTreeIndex.IndexInfo> getLength(Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    return new BTreeIndex.IndexInfo(this.data.size(), this.rootPointer.get());
                }
            }, executorService());
        }

        CompletableFuture<ByteArraySegment> read(long offset, int length, boolean shouldCache, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    if (this.checkOffsets.get()) {
                        // We want to make sure that we actually read pages that we wrote, and not from arbitrary locations
                        // in the data source.
                        Preconditions.checkArgument(this.offsets.isEmpty() || this.offsets.getOrDefault(offset, false),
                                "Offset not registered or already obsolete: " + offset);
                    }

                    return new ByteArraySegment(this.data.getData().slice((int) offset, length).getCopy());

                }
            }, executorService());
        }

        CompletableFuture<Long> write(List<BTreeIndex.WritePage> toWrite, Collection<Long> obsoleteOffsets,
                                      long truncateOffset, Duration timeout) {
            val wi = this.writeInterceptor.get();
            if (wi != null) {
                return wi.thenCompose(v -> writeInternal(toWrite, obsoleteOffsets, truncateOffset));
            } else {
                return writeInternal(toWrite, obsoleteOffsets, truncateOffset);
            }
        }

        void appendData(byte[] data) {
            synchronized (this.data) {
                this.data.write(data);
            }
        }

        private CompletableFuture<Long> writeInternal(List<BTreeIndex.WritePage> toWrite,
                                                      Collection<Long> obsoleteOffsets, long truncateOffset) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    if (toWrite.isEmpty()) {
                        return (long) this.data.size();
                    }

                    long originalOffset = this.data.size();
                    long expectedOffset = this.data.size();
                    for (val e : toWrite) {
                        Preconditions.checkArgument(expectedOffset == e.getOffset(), "Bad Offset. Expected %s, given %s.",
                                expectedOffset, e.getOffset());
                        try {
                            e.getContents().copyTo(this.data);
                        } catch (Exception ex) {
                            throw new CompletionException(ex);
                        }

                        this.offsets.put(e.getOffset(), true);
                        expectedOffset += e.getContents().getLength();
                    }

                    assert expectedOffset == this.data.size() : "unexpected number of bytes copied";

                    // Now, validate and clear obsolete offsets.
                    if (this.checkOffsets.get()) {
                        for (long offset : obsoleteOffsets) {
                            boolean exists = this.offsets.isEmpty() || this.offsets.getOrDefault(offset, false);
                            Assert.assertTrue("Unexpected offset removed: " + offset, exists || offset < originalOffset);
                            if (this.offsets.containsKey(offset)) {
                                // Mark as obsolete.
                                this.offsets.put(offset, false);
                            }
                        }

                        // Validate that the given truncation offset is correct - we do not want to truncate live data.
                        long expectedTruncationOffset = this.offsets.entrySet().stream()
                                .filter(Map.Entry::getValue)
                                .map(Map.Entry::getKey)
                                .min(Long::compare)
                                .orElse((long) this.data.size());
                        Assert.assertEquals("Unexpected truncation offset.", expectedTruncationOffset, truncateOffset);
                    }

                    // Truncate data out of the data source.
                    val toRemove = this.offsets.keySet().stream()
                            .filter(offset -> offset < truncateOffset)
                            .collect(Collectors.toList());
                    toRemove.forEach(this.offsets::remove);

                    // Update root pointer.
                    this.rootPointer.set(Math.max(this.rootPointer.get(), toWrite.get(toWrite.size() - 1).getOffset())); // Last thing to write is the root pointer.
                    return (long) this.data.size();
                }
            }, executorService());
        }
    }
}