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

import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the BTreePage class.
 */
public class BTreePageTests {
    private static final int ITEM_COUNT = 1000;
    private static final BTreePage.Config CONFIG = new BTreePage.Config(Integer.BYTES, Long.BYTES, 512, true);
    private static final BufferViewComparator KEY_COMPARATOR = BufferViewComparator.create();
    private final Random rnd = new Random(0);

    /**
     * Tests the ability to use update() to insert or update values.
     */
    @Test
    public void testUpdate() {
        val page = new BTreePage(CONFIG);
        int headerId = page.getHeaderId();
        val entries = new HashMap<Integer, Long>();
        while (entries.size() < ITEM_COUNT) {
            val updateKeys = new HashSet<Integer>();
            int half = entries.size() / 2;

            // Update half of the existing items.
            for (int i = 0; i < half; i++) {
                // Pick random keys from the set of existing entries and update them.
                updateKeys.add(pickUnusedKey(entries.size(), updateKeys));
            }

            // Create new items.
            half = Math.max(1, half);
            for (int i = 0; i < half; i++) {
                updateKeys.add(entries.size() + i);
            }

            // Apply to the page.
            val updateValues = updateKeys.stream().collect(Collectors.toMap(key -> key, key -> ((long) entries.size() << 32) + key));
            int delta = page.update(serialize(updateValues, true));
            Assert.assertEquals("Unexpected number of entries reported as inserted.", half, delta);

            // Update our expected values.
            entries.putAll(updateValues);

            // Verify.
            checkPage(page, entries);
        }

        Assert.assertEquals("Unexpected header id.", headerId, page.getHeaderId());
    }

    /**
     * Tests the ability to use update() to remove entries.
     */
    @Test
    public void testDelete() {
        val page = new BTreePage(CONFIG);
        int headerId = page.getHeaderId();
        val remainingEntries = IntStream.range(0, ITEM_COUNT).boxed().collect(Collectors.toMap(i -> i, i -> (long) (i + 1) * (i + 1)));

        // Initial page population.
        page.update(serialize(remainingEntries, true));
        checkPage(page, remainingEntries);

        // Begin deleting.
        int deleteCount = 0;
        while (!remainingEntries.isEmpty()) {
            val deleteKeys = new HashSet<Integer>();
            if (deleteCount % 3 == 0) {
                // Every now and then, try to delete inexistent keys.
                deleteKeys.add(ITEM_COUNT + 1);
            } else {
                for (int i = 0; i < deleteCount; i++) {
                    deleteKeys.add(pickUnusedKey(ITEM_COUNT, deleteKeys));
                }
            }

            val keys = serialize(deleteKeys, true);
            val existingKeys = keys.stream().filter(k -> page.searchExact(k) != null).count();
            int delta = page.update(toDelete(keys));
            Assert.assertEquals("Unexpected number of entries reported as deleted.", -existingKeys, delta);
            deleteKeys.forEach(remainingEntries::remove);

            checkPage(page, remainingEntries);
            deleteCount++;
        }

        Assert.assertEquals("Not expecting any remaining entries.", 0, remainingEntries.size());
        Assert.assertEquals("Unexpected header id.", headerId, page.getHeaderId());
    }

    /**
     * Tests the ability to use update() to update and remove entries in the same call.
     */
    @Test
    public void testUpdateDelete() {
        int iterationCount = 100;
        int insertsPerIteration = ITEM_COUNT / iterationCount;
        int updatesPerIteration = insertsPerIteration / 3;
        int deletesPerIteration = insertsPerIteration / 3;

        val page = new BTreePage(CONFIG);
        int headerId = page.getHeaderId();

        // We have a number of iterations.
        // At each iteration, we insert some items, update some existing items, and delete some existing items.
        val expectedValues = new HashMap<Integer, Long>();
        val rnd = new Random(0);
        for (int iteration = 0; iteration < iterationCount; iteration++) {
            val changes = new HashMap<Integer, Long>();
            int expectedDelta = 0;

            // Generate inserts. We want to make sure we don't insert existing values.
            for (int i = 0; i < insertsPerIteration; i++) {
                int key;
                do {
                    key = rnd.nextInt(ITEM_COUNT);
                } while (expectedValues.containsKey(key) && changes.containsKey(key));

                changes.put(key, (long) key + 1);
                if (page.searchExact(serializeInt(key)) == null) {
                    expectedDelta++;
                }
            }

            // Generate updates.
            val allKeys = new ArrayList<>(expectedValues.keySet());
            for (int i = 0; i < updatesPerIteration; i++) {
                if (allKeys.size() == 0) {
                    break;
                }

                int idx = rnd.nextInt(allKeys.size());
                int key = allKeys.get(idx);
                allKeys.remove(idx); // Remove at index.
                changes.put(key, expectedValues.get(key) + 1);
            }

            //Generate deletes.
            for (int i = 0; i < deletesPerIteration; i++) {
                if (allKeys.size() == 0) {
                    break;
                }

                int idx = rnd.nextInt(allKeys.size());
                int key = allKeys.get(idx);
                allKeys.remove(idx); // Remove at index.
                changes.put(key, null);
                expectedDelta--;
            }

            // Store changes.
            changes.forEach((key, value) -> {
                if (value == null) {
                    expectedValues.remove(key);
                } else {
                    expectedValues.put(key, value);
                }
            });

            // Apply changes.
            int delta = page.update(serialize(changes, true));
            Assert.assertEquals("Unexpected number of entries reported as changed.", expectedDelta, delta);
            checkPage(page, expectedValues);
        }

        Assert.assertEquals("Unexpected header id.", headerId, page.getHeaderId());
    }

    /**
     * Tests the ability to use search() and searchExact() to look up entries.
     */
    @Test
    public void testSearch() {
        val page = new BTreePage(CONFIG);

        // Populate the page.
        // Need to make all keys even. This helps with searching for inexistent keys (see below).
        val entries = IntStream.range(0, ITEM_COUNT).boxed().collect(Collectors.toMap(i -> i * 2, i -> (long) (i + 1) * (i + 1)));
        page.update(serialize(entries, true));

        // Serialize the keys, and THEN sort them (the tree page sorts byte arrays, not numbers).
        val searchKeys = serialize(entries.keySet(), true);
        for (int pos = 0; pos < searchKeys.size(); pos++) {
            // We only expect an exact search if the key exists in the page.
            val key = searchKeys.get(pos);
            val sr = page.search(key, 0);
            val existingKey = deserializeInt(searchKeys.get(pos));
            Assert.assertTrue("Expected exact match for key " + existingKey, sr.isExactMatch());
            Assert.assertEquals("Unexpected position for key " + existingKey, pos, sr.getPosition());
            long valueAt = deserializeLong(page.getValueAt(sr.getPosition()));
            Assert.assertEquals("Unexpected value for key " + existingKey, (long) entries.get(existingKey), valueAt);

            // Do the same lookup but with searchExact().
            long value = deserializeLong(page.searchExact(key));
            Assert.assertEquals("Unexpected result from searchExact() for key " + existingKey, valueAt, value);

            // All of our keys are serializations of even numbers, which means all their serializations have their last
            // bit set to 0. Since searching for inexistent keys returns the index where that inexistent key would have
            // been in the page, in order to test this feature we need to fetch the previous key in the list and flip its
            // last bit to 1. That will produce a search key that is larger than the previous key but smaller than the
            // current one.
            if (pos > 0) {
                // Get the last key and flip its last bit to 1.
                val inexistentKey = new ByteArraySegment(searchKeys.get(pos - 1).getCopy());
                inexistentKey.set(inexistentKey.getLength() - 1, (byte) (inexistentKey.get(inexistentKey.getLength() - 1) | 1));

                // Execute the search and verify the result (inexact match, and index the same as our existing key).
                val sr2 = page.search(inexistentKey, 0);
                Assert.assertFalse("Expected inexact match for key " + inexistentKey, sr2.isExactMatch());
                Assert.assertEquals("Unexpected position for inexistent key " + inexistentKey, sr.getPosition(), sr2.getPosition());
                Assert.assertNull("Not expecting a searchExact() value for inexistent key " + key, page.searchExact(inexistentKey));
            }
        }
    }

    /**
     * Tests the getKeyAt() and getValueAt() methods.
     */
    @Test
    public void testKeyValueAt() {
        int count = 1000;
        val page = new BTreePage(CONFIG);

        // Populate the page.
        val entries = IntStream.range(0, count).boxed().collect(Collectors.toMap(i -> i, i -> (long) (i + 1) * (i + 1)));
        val sortedEntries = serialize(entries, true);
        page.update(sortedEntries);

        // Search.
        for (int pos = 0; pos < sortedEntries.size(); pos++) {
            val e = sortedEntries.get(pos);
            val keyAt = page.getKeyAt(pos);
            val valueAt = page.getValueAt(pos);
            assertEquals("Unexpected key at position " + pos, e.getKey(), keyAt);
            assertEquals("Unexpected value at position " + pos, e.getValue(), valueAt);
        }

        // Bad arguments.
        AssertExtensions.assertThrows(
                "getKeyAt allowed negative argument",
                () -> page.getKeyAt(-1),
                ex -> ex instanceof IndexOutOfBoundsException);
        AssertExtensions.assertThrows(
                "getKeyAt allowed overflow argument",
                () -> page.getKeyAt(page.getCount()),
                ex -> ex instanceof IndexOutOfBoundsException);
        AssertExtensions.assertThrows(
                "getValueAt allowed negative argument",
                () -> page.getValueAt(-1),
                ex -> ex instanceof IndexOutOfBoundsException);
        AssertExtensions.assertThrows(
                "getValueAt allowed overflow argument",
                () -> page.getValueAt(page.getCount()),
                ex -> ex instanceof IndexOutOfBoundsException);
    }

    /**
     * Tests the setFirstKey() method.
     */
    @Test
    public void testSetFirstKey() {
        int count = 10;
        val page = new BTreePage(CONFIG);
        val entries = IntStream.range(0, count).boxed().collect(Collectors.toMap(i -> i, i -> (long) (i + 1) * (i + 1)));
        val serializedEntries = serialize(entries, true);
        page.update(serializedEntries);

        AssertExtensions.assertThrows(
                "setFirstKey allowed larger key to be replaced.",
                () -> page.setFirstKey(serializedEntries.get(1).getKey()),
                ex -> ex instanceof IllegalArgumentException);

        // Remove the first key.
        page.update(toDelete(Collections.singletonList(serializedEntries.get(0).getKey())));
        assertEquals("First key was not removed.", serializedEntries.get(1).getKey(), page.getKeyAt(0));

        // Replace the existing first key with the previously (removed) value.
        page.setFirstKey(serializedEntries.get(0).getKey());
        assertEquals("First key was not replaced.", serializedEntries.get(0).getKey(), page.getKeyAt(0));
    }

    /**
     * Tests the getEntries() method.
     */
    @Test
    public void testGetEntries() {
        int count = 20; // This test is doing O(count^2) array comparisons, so let's keep this small.
        val page = new BTreePage(CONFIG);

        // Populate the page.
        val entries = IntStream.range(0, count).boxed().collect(Collectors.toMap(i -> i, i -> (long) (i + 1) * (i + 1)));
        val serializedEntries = serialize(entries, true);
        page.update(serializedEntries);

        for (int i = 0; i < count; i++) {
            for (int j = i; j < count; j++) {
                val entryRange = page.getEntries(i, j);
                val expectedEntries = serializedEntries.subList(i, j + 1);
                AssertExtensions.assertListEquals("Unexpected result from getEntries(" + i + ", " + j + ").",
                        expectedEntries, entryRange,
                        (e, a) -> KEY_COMPARATOR.compare(e.getKey(), a.getKey()) == 0 && KEY_COMPARATOR.compare(e.getValue(), a.getValue()) == 0);
            }
        }
    }

    /**
     * Tests the ability to split as the page grows.
     */
    @Test
    public void testSplit() {
        int count = 1000;
        val page = new BTreePage(CONFIG);
        int headerId = page.getHeaderId();
        for (int item = 0; item < count; item++) {
            // Add one more entry to the page.
            page.update(Collections.singletonList(new PageEntry(serializeInt(item), serializeLong(item + 1))));

            boolean expectedSplit = page.getLength() > CONFIG.getMaxPageSize();
            val splitResult = page.splitIfNecessary();
            if (expectedSplit) {
                // Verify that the entries in the split pages are in the same order as in the original page.
                int originalPos = 0;
                int minLength = Integer.MAX_VALUE;
                int maxLength = -1;
                for (BTreePage sp : splitResult) {
                    Assert.assertNotEquals("Expecting different header ids.", headerId, sp.getHeaderId());
                    minLength = Math.min(minLength, sp.getLength());
                    maxLength = Math.min(maxLength, sp.getLength());
                    AssertExtensions.assertLessThanOrEqual("Split page size too large.", CONFIG.getMaxPageSize(), sp.getContents().getLength());
                    AssertExtensions.assertGreaterThan("Split page size too small.", CONFIG.getMaxPageSize() / 2, sp.getContents().getLength());
                    AssertExtensions.assertGreaterThan("Not expecting any empty pages.", 0, sp.getCount());
                    for (int si = 0; si < sp.getCount(); si++) {
                        val expectedKey = page.getKeyAt(originalPos);
                        val expectedValue = page.getValueAt(originalPos);
                        val actualKey = sp.getKeyAt(si);
                        val actualValue = sp.getValueAt(si);
                        assertEquals("Unexpected split page key.", expectedKey, actualKey);
                        assertEquals("Unexpected split page value.", expectedValue, actualValue);
                        originalPos++;
                    }
                }

                AssertExtensions.assertLessThanOrEqual("Too much page length variation.", CONFIG.getEntryLength(), maxLength - minLength);
            } else {
                Assert.assertNull("Not expecting any split result", splitResult);
            }
        }

        Assert.assertEquals("Unexpected header id.", headerId, page.getHeaderId());
    }

    /**
     * Tests the default constructor (empty page) and copy constructor.
     */
    @Test
    public void testConstructor() {
        int count = 1000;

        // Empty page.
        val page1 = new BTreePage(CONFIG);
        Assert.assertEquals("Expecting an empty page.", 0, page1.getCount());
        Assert.assertNull("Not expecting any items.", page1.searchExact(new ByteArraySegment(new byte[CONFIG.getKeyLength()])));
        val sr = page1.search(new ByteArraySegment(new byte[CONFIG.getKeyLength()]), 0);
        Assert.assertFalse("Expecting inexact match.", sr.isExactMatch());
        Assert.assertEquals("Unexpected position for empty page.", 0, sr.getPosition());

        // Populate page.
        val entries = IntStream.range(0, count).boxed().collect(Collectors.toMap(i -> i, i -> (long) (i + 1) * (i + 1)));
        page1.update(serialize(entries, true));

        // Copy constructor.
        val page2 = new BTreePage(CONFIG, page1.getContents());
        checkPage(page2, entries);
        Assert.assertEquals("Unexpected header id.", page1.getHeaderId(), page2.getHeaderId());

        // Update the second page with new data.
        val entries2 = IntStream.range(0, count).boxed().collect(Collectors.toMap(i -> i, i -> (long) (i + 1)));
        page2.update(serialize(entries2, true));
        checkPage(page2, entries2);

        // In this particular case (since we did not allocate a new buffer - same count) we expect the first page's buffer
        // to be reused, hence the first page should also be updated.
        checkPage(page1, entries2);
    }

    /**
     * Tests the static method isIndexPage().
     */
    @Test
    public void testIsIndexPage() {
        val indexPage = new BTreePage(new BTreePage.Config(Integer.BYTES, Long.BYTES, 512, true));
        Assert.assertTrue("Unexpected isIndexPage.", BTreePage.isIndexPage(indexPage.getContents()));
        val nonIndexPage = new BTreePage(new BTreePage.Config(Integer.BYTES, Long.BYTES, 512, false));
        Assert.assertFalse("Unexpected isIndexPage.", BTreePage.isIndexPage(nonIndexPage.getContents()));
    }

    private void checkPage(BTreePage page, Map<Integer, Long> expectedValues) {
        Assert.assertEquals("Unexpected count.", expectedValues.size(), page.getCount());
        val sortedEntries = serialize(expectedValues, true);
        for (int i = 0; i < sortedEntries.size(); i++) {
            // Lookup the keys and values by position.
            val key = page.getKeyAt(i);
            val value = page.getValueAt(i);
            assertEquals("Unexpected key at position " + i, sortedEntries.get(i).getKey(), key);
            assertEquals("Unexpected value at position " + i, sortedEntries.get(i).getValue(), value);

            // Lookup using search.
            val sr = page.search(key, 0);
            Assert.assertTrue("Expecting search to yield an exact match.", sr.isExactMatch());
            Assert.assertEquals("Unexpected search result position.", i, sr.getPosition());
        }
    }

    private void assertEquals(String message, ByteArraySegment b1, ByteArraySegment b2) {
        if (b1.getLength() != b2.getLength() || KEY_COMPARATOR.compare(b1, b2) != 0) {
            Assert.fail(message);
        }
    }

    private List<PageEntry> serialize(Map<Integer, Long> entries, boolean sorted) {
        val t1 = entries.entrySet().stream()
                        .map(e -> new PageEntry(serializeInt(e.getKey()), e.getValue() == null ? null : serializeLong(e.getValue())));

        if (sorted) {
            return t1.sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()))
                     .collect(Collectors.toList());
        } else {
            return t1.collect(Collectors.toList());
        }
    }

    private List<ByteArraySegment> serialize(Collection<Integer> keys, boolean sorted) {
        val t1 = keys.stream().map(this::serializeInt);

        if (sorted) {
            return t1.sorted(KEY_COMPARATOR::compare).collect(Collectors.toList());
        } else {
            return t1.collect(Collectors.toList());
        }
    }

    private List<PageEntry> toDelete(List<ByteArraySegment> keys) {
        return keys.stream().map(PageEntry::noValue).collect(Collectors.toList());
    }

    private ByteArraySegment serializeInt(int value) {
        ByteArraySegment r = new ByteArraySegment(new byte[Integer.BYTES]);
        r.setInt(0, value);
        return r;
    }

    private ByteArraySegment serializeLong(long value) {
        ByteArraySegment r = new ByteArraySegment(new byte[Long.BYTES]);
        r.setLong(0, value);
        return r;
    }

    private int deserializeInt(ByteArraySegment serialized) {
        return serialized.getInt(0);
    }

    private long deserializeLong(ByteArraySegment serialized) {
        return serialized.getLong(0);
    }

    private int pickUnusedKey(int maxValue, HashSet<Integer> pickedKeys) {
        int key;
        do {
            key = rnd.nextInt(maxValue);
        }
        while (pickedKeys.contains(key));
        return key;
    }
}

