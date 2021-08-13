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
package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Base class for testing any SortedIndex implementation.
 */
abstract class SortedIndexTestBase {
    private static final int ITEM_COUNT = 100 * 1000;
    private static final Comparator<Long> KEY_COMPARATOR = Long::compare;
    private static final Comparator<Long> KEY_REVERSE_COMPARATOR = (n1, n2) -> Long.compare(n2, n1);

    //region Test Targets

    /**
     * Unit tests for the AvlTreeIndex class.
     */
    public static class AvlTreeIndexTests extends SortedIndexTestBase {
        @Override
        protected SortedIndex<TestEntry> createIndex() {
            return new AvlTreeIndex<>();
        }
    }

    /**
     * Unit tests for the RedBlackTreeIndex class.
     */
    public static class RedBlackTreeIndexTests extends SortedIndexTestBase {
        @Override
        protected SortedIndex<TestEntry> createIndex() {
            return new RedBlackTreeIndex<>();
        }
    }

    //endregion

    //region Test Definitions

    /**
     * Tests the put(), size(), get(), getFirst() and getLast() methods
     */
    @Test
    public void testPut() {
        final int reinsertFrequency = 100;
        val index = createIndex();
        Random rnd = new Random(0);
        TestEntry firstEntry = null;
        TestEntry lastEntry = null;
        val reinsertKeys = new ArrayList<Integer>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            // Generate a unique key.
            int key;
            do {
                key = rnd.nextInt();
            } while (index.get(key) != null);

            // Keep track of it for reinsertion later.
            if (i % reinsertFrequency == 0) {
                reinsertKeys.add(key);
            }

            val entry = new TestEntry(key);
            if (firstEntry == null || entry.key() <= firstEntry.key()) {
                firstEntry = entry;
            }

            if (lastEntry == null || entry.key() >= lastEntry.key()) {
                lastEntry = entry;
            }

            // Insert into the index and verify size(), getFirst(), getLast().
            index.put(entry);

            Assert.assertEquals("Unexpected size.", i + 1, index.size());
            Assert.assertEquals("Unexpected value from getFirst() after " + index.size() + " insertions.", firstEntry, index.getFirst());
            Assert.assertEquals("Unexpected value from getLast() after " + index.size() + " insertions.", lastEntry, index.getLast());
        }

        // Now try to reinsert some of the items.
        for (int key : reinsertKeys) {
            val oldEntry = index.get(key);
            val entry = new TestEntry(key);
            val overriddenEntry = index.put(entry);
            val reRetrievedEntry = index.get(key);
            Assert.assertEquals("Unexpected overridden entry for key " + key, oldEntry, overriddenEntry);
            Assert.assertEquals("New entry was not placed in the index for key " + key, entry, reRetrievedEntry);
            Assert.assertEquals("Unexpected size when overriding entry.", ITEM_COUNT, index.size());
        }
    }

    /**
     * Tests the remove(), size(), get(), getFirst(), getLast() methods.
     */
    @Test
    public void testRemove() {
        val index = createIndex();
        val keys = populate(index);

        // Remove the items, in order.
        keys.sort(KEY_COMPARATOR);
        val keysToRemove = new LinkedList<Long>(keys);
        int expectedSize = index.size();
        while (keysToRemove.size() > 0) {
            // Remove either the first or the last key - this helps test getFirst/getLast properly.
            long key = expectedSize % 2 == 0 ? keysToRemove.removeLast() : keysToRemove.removeFirst();
            val entry = index.get(key);
            val removedEntry = index.remove(key);
            expectedSize--;

            Assert.assertEquals("Unexpected removed entry for key " + key, entry, removedEntry);
            Assert.assertEquals("Unexpected size after removing key " + key, expectedSize, index.size());
            Assert.assertNull("Entry was not removed for key " + key, index.get(key));

            if (expectedSize == 0) {
                Assert.assertNull("Unexpected value from getFirst() when index is empty.", index.getFirst());
                Assert.assertNull("Unexpected value from getLast() when index is empty.", index.getLast());
            } else {
                Assert.assertEquals("Unexpected value from getFirst() after removing key " + key, (long) keysToRemove.getFirst(), index.getFirst().key());
                Assert.assertEquals("Unexpected value from getLast() after removing key " + key, (long) keysToRemove.getLast(), index.getLast().key());
            }
        }
    }

    /**
     * Tests the clear() method.
     */
    @Test
    public void testClear() {
        val index = createIndex();
        val keys = populate(index);

        Assert.assertNotNull("Unexpected return value for getFirst() on non-empty index.", index.getFirst());
        Assert.assertNotNull("Unexpected return value for getLast() on non-empty index.", index.getLast());

        index.clear();
        Assert.assertEquals("Unexpected size of empty index.", 0, index.size());
        Assert.assertNull("Unexpected return value for getFirst() on empty index.", index.getFirst());
        Assert.assertNull("Unexpected return value for getLast() on empty index.", index.getLast());

        for (long key : keys) {
            Assert.assertNull("Unexpected value for get() on empty index.", index.get(key));
            Assert.assertNull("Unexpected value for getCeiling() on empty index.", index.getCeiling(key));
        }
    }

    /**
     * Tests the getCeiling() method.
     */
    @Test
    public void testGetCeiling() {
        final int itemCount = 1000;
        final int maxKey = itemCount * 10;

        // Create an index and populate sparsely.
        val index = createIndex();
        val validKeys = populate(index, itemCount, maxKey);
        validKeys.sort(KEY_COMPARATOR);

        val validKeysIterator = validKeys.iterator();
        Long expectedValue = -1L;
        for (long testKey = 0; testKey < maxKey; testKey++) {
            // Since both testKey and validKeysIterator increase with natural ordering, finding the next expected value
            // is a straightforward call to the iterator next() method.
            while (expectedValue != null && testKey > expectedValue) {
                if (validKeysIterator.hasNext()) {
                    expectedValue = validKeysIterator.next();
                } else {
                    expectedValue = null;
                }
            }

            val ceilingEntry = index.getCeiling(testKey);
            Long actualValue = ceilingEntry != null ? ceilingEntry.key() : null;
            Assert.assertEquals("Unexpected value for getCeiling for key " + testKey, expectedValue, actualValue);
        }
    }

    /**
     * Tests the getFloor() method.
     */
    @Test
    public void testGetFloor() {
        final int itemCount = 1000;
        final int maxKey = itemCount * 10;

        // Create an index and populate sparsely.
        val index = createIndex();
        val validKeys = populate(index, itemCount, maxKey);
        validKeys.sort(KEY_REVERSE_COMPARATOR);

        val validKeysIterator = validKeys.iterator();
        Long expectedValue = (long) Integer.MAX_VALUE;
        for (long testKey = maxKey; testKey >= 0; testKey--) {
            // Since both testKey and validKeysIterator increase with natural ordering, finding the next expected value
            // is a straightforward call to the iterator next() method.
            while (expectedValue != null && testKey < expectedValue) {
                if (validKeysIterator.hasNext()) {
                    expectedValue = validKeysIterator.next();
                } else {
                    expectedValue = null;
                }
            }

            val ceilingEntry = index.getFloor(testKey);
            Long actualValue = ceilingEntry != null ? ceilingEntry.key() : null;
            Assert.assertEquals("Unexpected value for getCeiling for key " + testKey, expectedValue, actualValue);
        }
    }

    /**
     * Tests the forEach() method.
     */
    @Test
    public void testForEach() {
        // Create an index and populate it.
        val index = createIndex();
        val validKeys = populate(index);

        // Extract the keys using forEach - they should be ordered naturally.
        val actualKeys = new ArrayList<Long>();
        index.forEach(e -> actualKeys.add(e.key()));

        // Order the inserted keys using the same comparator we used for the index.
        validKeys.sort(KEY_COMPARATOR);

        // Verify that modifying the index while looping through it does throw an exception.
        AssertExtensions.assertThrows(
                "forEach did not throw when a new item was added during enumeration.",
                () -> index.forEach(e -> index.put(new TestEntry(index.size()))),
                ex -> ex instanceof ConcurrentModificationException);

        AssertExtensions.assertThrows(
                "forEach did not throw when an item was removed during enumeration.",
                () -> index.forEach(e -> index.remove(e.key())),
                ex -> ex instanceof ConcurrentModificationException);

        AssertExtensions.assertThrows(
                "forEach did not throw when the index was cleared during enumeration.",
                () -> index.forEach(e -> index.clear()),
                ex -> ex instanceof ConcurrentModificationException);
    }

    /**
     * Tests various operations on already sorted input.
     */
    @Test
    public void testSortedInput() {
        val index = createIndex();
        for (int key = 0; key < ITEM_COUNT; key++) {
            index.put(new TestEntry(key));
        }

        //Get + GetCeiling.
        for (int key = 0; key < ITEM_COUNT; key++) {
            Assert.assertEquals("Unexpected value from get() for key " + key, key, index.get(key).key());
            Assert.assertEquals("Unexpected value from getCeiling() for key " + key, key, index.getCeiling(key).key());
        }

        // Remove + get.
        for (long key = 0; key < ITEM_COUNT; key++) {
            long removedKey = index.remove(key).key();
            Assert.assertEquals("Unexpected value from remove(). ", key, removedKey);
            Assert.assertNull("Unexpected value from get() for removed key " + key, index.get(key));
            if (key == ITEM_COUNT - 1) {
                Assert.assertNull("Unexpected value from getCeiling() for removed key " + key, index.getCeiling(key));
            } else {
                Assert.assertEquals("Unexpected value from getCeiling() for removed key " + key, key + 1, index.getCeiling(key).key());
            }
        }
    }

    //endregion

    //region Helpers

    abstract SortedIndex<TestEntry> createIndex();

    private ArrayList<Long> populate(SortedIndex<TestEntry> index) {
        return populate(index, ITEM_COUNT, Integer.MAX_VALUE);
    }

    private ArrayList<Long> populate(SortedIndex<TestEntry> index, int itemCount, int maxKey) {
        Random rnd = new Random(0);
        val keys = new ArrayList<Long>();
        for (int i = 0; i < itemCount; i++) {
            // Generate a unique key.
            long key;
            do {
                key = rnd.nextInt(maxKey);
            } while (index.get(key) != null);

            keys.add(key);
            index.put(new TestEntry(key));
        }

        return keys;
    }

    //endregion

    //region TestEntry

    private static class TestEntry implements SortedIndex.IndexEntry {
        // Note: do not implement equals() or hash() for this class - the tests rely on object equality, not key equality.
        private final long key;

        TestEntry(long key) {
            this.key = key;
        }

        @Override
        public long key() {
            return this.key;
        }

        @Override
        public String toString() {
            return "Key = " + this.key;
        }
    }

    //endregion

    //region Performance Testing

    /**
     * Tests the SortedIndex for performance and outputs results to the console.
     * Not a real unit test - to be used to judge performance of various operations of the index. Long running test.
     */
    @Test
    @Ignore
    public void testPerformance() {
        final int itemCount = 10 * 1000 * 1000;
        final int iterationCount = 5;
        PerfTester tester = new PerfTester(this::createIndex, itemCount, iterationCount);
        tester.run();
    }

    @RequiredArgsConstructor
    private static class PerfTester {
        private static final int NANOS_TO_MILLIS = 1000;
        private final Supplier<SortedIndex<TestEntry>> indexSupplier;
        private final int itemCount;
        private final int iterationCount;

        void run() {
            ArrayList<PerfResult> results = new ArrayList<>();
            String indexName = null;
            for (int i = 0; i < iterationCount; i++) {
                SortedIndex<TestEntry> index = indexSupplier.get();
                if (indexName == null) {
                    indexName = indexSupplier.get().getClass().getSimpleName();
                }

                PerfResult partialResult = new PerfResult(itemCount);
                results.add(partialResult);

                partialResult.insertElapsed = measure(() -> insert(index, itemCount));
                partialResult.getElapsed = measure(() -> readExact(index, itemCount));
                partialResult.ceilingElapsed = measure(() -> readCeiling(index, itemCount));
                partialResult.lastElapsed = measure(() -> readLast(index, itemCount));
            }

            outputStats(indexName, "Insert ", r -> r.insertElapsed, results);
            outputStats(indexName, "Get    ", r -> r.getElapsed, results);
            outputStats(indexName, "Ceiling", r -> r.ceilingElapsed, results);
            outputStats(indexName, "Last   ", r -> r.lastElapsed, results);
        }

        private void outputStats(String indexName, String statsName, Function<PerfResult, Long> statsProvider, Collection<PerfResult> results) {
            double min = results.stream().mapToDouble(r -> statsProvider.apply(r) / (double) r.count).min().orElse(-1) / NANOS_TO_MILLIS;
            double max = results.stream().mapToDouble(r -> statsProvider.apply(r) / (double) r.count).max().orElse(-1) / NANOS_TO_MILLIS;
            double avg = results.stream().mapToDouble(r -> statsProvider.apply(r) / (double) r.count).average().orElse(-1) / NANOS_TO_MILLIS;
            System.out.println(String.format("%s.%s: Min = %.2f us, Max = %.2f us, Avg = %.2f us", indexName, statsName, min, max, avg));
        }

        private void insert(SortedIndex<TestEntry> rbt, int count) {
            for (int i = 0; i < count; i++) {
                rbt.put(new TestEntry(i));
            }
        }

        private void readExact(SortedIndex<TestEntry> rbt, int count) {
            for (int i = 0; i < count; i++) {
                rbt.get(i);
            }
        }

        private void readCeiling(SortedIndex<TestEntry> rbt, int count) {
            for (int i = 0; i < count; i++) {
                rbt.getCeiling(i);
            }
        }

        private void readLast(SortedIndex<TestEntry> rbt, int count) {
            for (int i = 0; i < count; i++) {
                rbt.getLast();
            }
        }

        private long measure(Runnable r) {
            System.gc();
            long rbtStart = System.nanoTime();
            r.run();
            return System.nanoTime() - rbtStart;
        }

        @RequiredArgsConstructor
        private static class PerfResult {
            final int count;
            long insertElapsed;
            long getElapsed;
            long ceilingElapsed;
            long lastElapsed;
        }
    }

    //endregion
}
