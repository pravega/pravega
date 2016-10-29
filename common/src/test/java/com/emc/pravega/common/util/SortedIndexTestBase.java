/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common.util;

import com.emc.pravega.testcommon.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for testing any SortedIndex implementation.
 */
abstract class SortedIndexTestBase {
    private static final int ITEM_COUNT = 100 * 1000;
    private static final Comparator<Integer> KEY_COMPARATOR = Integer::compare;

    //region Test Targets

    /**
     * Unit tests for the AvlTreeIndex class.
     */
    public static class AvlTreeIndexTests extends SortedIndexTestBase {
        @Override
        protected SortedIndex<Integer, TestEntry> createIndex(Comparator<Integer> comparator) {
            return new AvlTreeIndex<>(comparator);
        }
    }

    /**
     * Unit tests for the RedBlackTreeIndex class.
     */
    public static class RedBlackTreeIndexTests extends SortedIndexTestBase {
        @Override
        protected SortedIndex<Integer, TestEntry> createIndex(Comparator<Integer> comparator) {
            return new RedBlackTreeIndex<>(comparator);
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
        val keysToRemove = new LinkedList<Integer>(keys);
        int expectedSize = index.size();
        val dummyEntry = new TestEntry(Integer.MAX_VALUE);
        while (keysToRemove.size() > 0) {
            // Remove either the first or the last key - this helps test getFirst/getLast properly.
            int key = expectedSize % 2 == 0 ? keysToRemove.removeLast() : keysToRemove.removeFirst();
            val entry = index.get(key);
            val removedEntry = index.remove(key);
            expectedSize--;

            Assert.assertEquals("Unexpected removed entry for key " + key, entry, removedEntry);
            Assert.assertEquals("Unexpected size after removing key " + key, expectedSize, index.size());
            Assert.assertNull("Entry was not removed for key " + key, index.get(key));
            Assert.assertEquals("Unexpected return value for get-or-default with removed key " + key, dummyEntry, index.get(key, dummyEntry));

            if (expectedSize == 0) {
                Assert.assertNull("Unexpected value from getFirst() when index is empty.", index.getFirst());
                Assert.assertNull("Unexpected value from getLast() when index is empty.", index.getLast());
            } else {
                Assert.assertEquals("Unexpected value from getFirst() after removing key " + key, keysToRemove.getFirst(), index.getFirst().key());
                Assert.assertEquals("Unexpected value from getLast() after removing key " + key, keysToRemove.getLast(), index.getLast().key());
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

        index.clear();
        Assert.assertEquals("Unexpected size of empty index.", 0, index.size());
        Assert.assertNull("Unexpected return value for getFirst() on empty index.", index.getFirst());
        Assert.assertNull("Unexpected return value for getLast() on empty index.", index.getLast());
        for (int key : keys) {
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
        Integer expectedValue = -1;
        for (int testKey = 0; testKey < maxKey; testKey++) {
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
            Integer actualValue = ceilingEntry == null ? null : ceilingEntry.key();
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
        val actualKeys = new ArrayList<Integer>();
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
            Assert.assertEquals("Unexpected value from get() for key " + key, key, (long) index.get(key).key());
            Assert.assertEquals("Unexpected value from getCeiling() for key " + key, key, (long) index.getCeiling(key).key());
        }

        // Remove + get.
        for (int key = 0; key < ITEM_COUNT; key++) {
            int removedKey = index.remove(key).key();
            Assert.assertEquals("Unexpected value from remove(). ", key, removedKey);
            Assert.assertNull("Unexpected value from get() for removed key " + key, index.get(key));
            if (key == ITEM_COUNT - 1) {
                Assert.assertNull("Unexpected value from getCeiling() for removed key " + key, index.getCeiling(key));
            } else {
                Assert.assertEquals("Unexpected value from getCeiling() for removed key " + key, key + 1, (long) index.getCeiling(key).key());
            }
        }
    }

    //endregion

    //region Helpers

    private SortedIndex<Integer, TestEntry> createIndex() {
        return createIndex(KEY_COMPARATOR);
    }

    protected abstract SortedIndex<Integer, TestEntry> createIndex(Comparator<Integer> comparator);

    private ArrayList<Integer> populate(SortedIndex<Integer, TestEntry> index) {
        return populate(index, ITEM_COUNT, Integer.MAX_VALUE);
    }

    private ArrayList<Integer> populate(SortedIndex<Integer, TestEntry> index, int itemCount, int maxKey) {
        Random rnd = new Random(0);
        val keys = new ArrayList<Integer>();
        for (int i = 0; i < itemCount; i++) {
            // Generate a unique key.
            int key;
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

    private static class TestEntry implements IndexEntry<Integer> {
        private static final AtomicLong ID_GENERATOR = new AtomicLong();
        // Note: do not implement equals() or hash() for this class - the tests rely on object equality, not key equality.
        private final int key;
        private final long id;

        TestEntry(int key) {
            this.key = key;
            this.id = ID_GENERATOR.incrementAndGet();
        }

        @Override
        public Integer key() {
            return this.key;
        }

        @Override
        public String toString() {
            return String.format("Key = %s, Id = %s", this.key, this.id);
        }
    }

    //endregion
}
