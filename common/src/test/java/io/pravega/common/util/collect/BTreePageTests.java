/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.collect;

import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the BTreePage class.
 */
public class BTreePageTests {
    private static final BTreePage.Config CONFIG = new BTreePage.Config(Integer.BYTES, Long.BYTES, 512, true);
    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private final Random rnd = new Random(0);

    /**
     * Tests the ability to use update() to insert or update values.
     */
    @Test
    public void testUpdate() {
        int maxCount = 1000;
        val page = new BTreePage(CONFIG);
        val entries = new HashMap<Integer, Long>();
        while (entries.size() < maxCount) {
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
            page.update(serialize(updateValues, false));

            // Update our expected values.
            entries.putAll(updateValues);

            // Verify.
            checkPage(page, entries);
        }
    }

    /**
     * Tests the ability to use delete() to remove entries.
     */
    @Test
    public void testDelete() {
        int initialCount = 1000;
        val page = new BTreePage(CONFIG);
        val remainingEntries = new HashMap<Integer, Long>();
        for (int i = 0; i < initialCount; i++) {
            remainingEntries.put(i, (long) (i + 1) * (i + 1));
        }

        // Initial page population.
        page.update(serialize(remainingEntries, false));
        checkPage(page, remainingEntries);

        // Begin deleting.
        int deleteCount = 0;
        while (!remainingEntries.isEmpty()) {
            val deleteKeys = new HashSet<Integer>();
            if (deleteCount % 3 == 0) {
                // Every now and then, try to delete inexistent keys.
                deleteKeys.add(initialCount + 1);
            } else {
                for (int i = 0; i < deleteCount; i++) {
                    deleteKeys.add(pickUnusedKey(initialCount, deleteKeys));
                }
            }

            page.delete(serialize(deleteKeys, false));
            deleteKeys.forEach(remainingEntries::remove);

            checkPage(page, remainingEntries);
            deleteCount++;
        }

        Assert.assertEquals("Not expecting any remaining entries.", 0, remainingEntries.size());
    }

    /**
     * Tests the ability to use search() to look up entries.
     */
    @Test
    public void testSearch() {
        int initialCount = 1000;
        val page = new BTreePage(CONFIG);
        val entries = new HashMap<Integer, Long>();
        int maxKey = 0;
        for (int i = 0; i < initialCount; i++) {
            maxKey = i * 2;
            entries.put(maxKey, (long) (i + 1) * (i + 1));
        }

        // Populate the page.
        page.update(serialize(entries, false));

        // TODO: figure out a way to search both keys that exist and keys that do not exist.
    }

    @Test
    public void testValueAt() {

    }

    @Test
    public void testKeyAt() {

    }

    @Test
    public void testSplit() {

    }

    @Test
    public void testConstructor() {
        // Config + count.
        // Config + contents.
    }

    @Test
    public void testIsIndexPage() {

    }

    private void checkPage(BTreePage page, HashMap<Integer, Long> expectedValues) {
        Assert.assertEquals("Unexpected count.", expectedValues.size(), page.getCount());
        val sortedEntries = serialize(expectedValues, true);
        for (int i = 0; i < sortedEntries.size(); i++) {
            // Lookup the keys and values by position.
            val key = page.getKeyAt(i);
            val value = page.getValueAt(i);
            assertEquals("Unexpected key at position " + i, sortedEntries.get(i).getLeft(), key);
            assertEquals("Unexpected value at position " + i, sortedEntries.get(i).getRight(), value);

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

    private List<ArrayTuple> serialize(Map<Integer, Long> entries, boolean sorted) {
        val t1 = entries.entrySet().stream()
                        .map(e -> new ArrayTuple(serializeInt(e.getKey()), serializeLong(e.getValue())));

        if (sorted) {
            return t1.sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getLeft(), e2.getLeft()))
                     .collect(Collectors.toList());
        } else {
            return t1.collect(Collectors.toList());
        }
    }

    private List<byte[]> serialize(Collection<Integer> keys, boolean sorted) {
        val t1 = keys.stream().map(this::serializeInt);

        if (sorted) {
            return t1.sorted(KEY_COMPARATOR).collect(Collectors.toList());
        } else {
            return t1.collect(Collectors.toList());
        }
    }

    private byte[] serializeInt(int value) {
        byte[] r = new byte[Integer.BYTES];
        BitConverter.writeInt(r, 0, value);
        return r;
    }

    private byte[] serializeLong(long value) {
        byte[] r = new byte[Long.BYTES];
        BitConverter.writeLong(r, 0, value);
        return r;
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
