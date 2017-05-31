/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.util.ArrayDeque;
import java.util.Deque;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the SortedDeque class.
 */
public class SortedDequeTests {
    private static final int ITEM_COUNT = 1000;

    /**
     * Tests the addLast(), size(), isEmpty(), peekFirst(), peekLast() methods.
     */
    @Test
    public void testAdd() {
        val d = new SortedDeque<TestItem>();
        Assert.assertTrue("Unexpected result from isEmpty() after creation.", d.isEmpty());
        Assert.assertEquals("Unexpected result from size() after creation.", 0, d.size());
        Assert.assertNull("Unexpected result from peekFirst() after creation.", d.peekFirst());
        Assert.assertNull("Unexpected result from peekLast() after creation.", d.peekLast());
        Assert.assertNull("Unexpected result from removeFirst() after creation.", d.removeFirst());
        Assert.assertNull("Unexpected result from removeLast() after creation.", d.removeLast());

        for (int i = 0; i < ITEM_COUNT; i++) {
            val item = new TestItem(i);
            d.addLast(item);
            Assert.assertFalse("Unexpected result from isEmpty() after adding items.", d.isEmpty());
            Assert.assertEquals("Unexpected result from size().", i + 1, d.size());
            Assert.assertEquals("Unexpected result from peekLast().", item, d.peekLast());
            Assert.assertEquals("Unexpected result from peekFirst().", 0, d.peekFirst().key());
        }
    }

    /**
     * Tests the removeLast() method.
     */
    @Test
    public void testRemoveLastSingle() {
        val d = new SortedDeque<TestItem>();

        for (int i = 0; i < ITEM_COUNT; i++) {
            val item = new TestItem(i);
            d.addLast(item);
        }

        int expectedKey = ITEM_COUNT - 1;
        while (!d.isEmpty()) {
            val peekedItem = d.peekLast();
            val item = d.removeLast();
            Assert.assertEquals("peekLast() and removeLast() did not return the same values.", peekedItem, item);
            Assert.assertEquals("Unexpected item returned by removeLast().", expectedKey, item.key());
            Assert.assertEquals("Unexpected result from size() when not empty.", expectedKey, d.size());
            expectedKey--;
        }

        Assert.assertEquals("Unexpected result from size() when empty.", 0, d.size());
        Assert.assertNull("Unexpected result from peekFirst() when empty.", d.peekFirst());
        Assert.assertNull("Unexpected result from peekLast() when empty.", d.peekLast());
        Assert.assertNull("Unexpected result from removeFirst() when empty.", d.removeFirst());
        Assert.assertNull("Unexpected result from removeLast() when empty.", d.removeLast());
    }

    /**
     * Tests the removeFirst() method.
     */
    @Test
    public void testRemoveFirstSingle() {
        val d = new SortedDeque<TestItem>();

        for (int i = 0; i < ITEM_COUNT; i++) {
            val item = new TestItem(i);
            d.addLast(item);
        }

        int expectedKey = 0;
        while (!d.isEmpty()) {
            val peekedItem = d.peekFirst();
            val item = d.removeFirst();
            Assert.assertEquals("peekFirst() and removeFirst() did not return the same values.", peekedItem, item);
            Assert.assertEquals("Unexpected item returned by removeFirst().", expectedKey, item.key());
            Assert.assertEquals("Unexpected result from size() when not empty.", ITEM_COUNT - expectedKey - 1, d.size());
            expectedKey++;
        }

        Assert.assertEquals("Unexpected result from size() when empty.", 0, d.size());
        Assert.assertNull("Unexpected result from peekFirst() when empty.", d.peekFirst());
        Assert.assertNull("Unexpected result from peekLast() when empty.", d.peekLast());
        Assert.assertNull("Unexpected result from removeFirst() when empty.", d.removeFirst());
        Assert.assertNull("Unexpected result from removeLast() when empty.", d.removeLast());
    }

    /**
     * Verifies that removeFirst(upTo) and removeLast(from) do not return anything and do not make any changes if the
     * sought item does not exist.
     */
    @Test
    public void testRemoveMultiNonExistent() {
        final int addMultiplier = 10;
        final int maxAddedValue = ITEM_COUNT * addMultiplier;
        val d = new SortedDeque<TestItem>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            val item = new TestItem(i * addMultiplier);
            d.addLast(item);
        }

        // Verify we cannot remove if we don't have an exact match.
        for (int i = 0; i < maxAddedValue; i++) {
            if (i % addMultiplier == 0) {
                // This exists; we won't test it here.
                continue;
            }
            val removedLast = d.removeFirst(new TestItem(i));
            val removedFirst = d.removeLast(new TestItem(i));
            Assert.assertNull("removeFirst() returned non-null for non-existent item.", removedFirst);
            Assert.assertNull("removeLast() returned non-null for non-existent item.", removedLast);
        }

        // Verify nothing has actually been removed.
        Assert.assertEquals("Unexpected size() when nothing should have changed.", ITEM_COUNT, d.size());
        int expectedKey = 0;
        while (!d.isEmpty()) {
            val removedItem = d.removeFirst();
            Assert.assertEquals("Item seems to have been removed.", expectedKey, removedItem.key());
            expectedKey += addMultiplier;
        }
    }

    /**
     * Tests the removeLast() method when multiple items are expected to be removed.
     */
    @Test
    public void testRemoveLastMulti() {
        final int removeAtOnce = 5; // Remove 5 items at a time.
        val d = new SortedDeque<TestItem>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            val item = new TestItem(i);
            d.addLast(item);
        }

        int toRemove = ITEM_COUNT - 1;
        int expectedSize = d.size() - 1;
        while (!d.isEmpty()) {
            val removed = d.removeLast(new TestItem(toRemove));
            Assert.assertNotNull("Expected non-null result from removeLast() when removing " + toRemove, removed);
            Assert.assertEquals("Unexpected removed item when removing " + toRemove, toRemove, removed.key());
            Assert.assertEquals("Unexpected size when removing " + toRemove, expectedSize, d.size());
            toRemove = Math.max(0, toRemove - removeAtOnce);
            expectedSize = Math.max(0, expectedSize - removeAtOnce);
        }
    }

    /**
     * Tests the removeFirst() method when multiple items are expected to be removed.
     */
    @Test
    public void testRemoveFirstMulti() {
        final int removeAtOnce = 5; // Remove 5 items at a time.
        val d = new SortedDeque<TestItem>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            val item = new TestItem(i);
            d.addLast(item);
        }

        int toRemove = 0;
        int expectedSize = d.size() - 1;
        while (!d.isEmpty()) {
            val removed = d.removeFirst(new TestItem(toRemove));
            Assert.assertNotNull("Expected non-null result from removeFirst() when removing " + toRemove, removed);
            Assert.assertEquals("Unexpected removed item when removing " + toRemove, toRemove, removed.key());
            Assert.assertEquals("Unexpected size when removing " + toRemove, expectedSize, d.size());
            toRemove = Math.min(ITEM_COUNT - 1, toRemove + removeAtOnce);
            expectedSize = Math.max(0, expectedSize - removeAtOnce);
        }
    }

    /**
     * Tests mixed operations: addLast(), removeFirst(), removeLast(), removeFirst(upTo), removeLast(from).
     */
    @Test
    public void testMixed() {
        final int removeFirstEvery = 5;
        final int removeLastEvery = 7;
        final int removeFirstMultiEvery = 11;
        final int removeLastMultiEvery = 13;
        final int removeMultiCount = 3;
        val d = new SortedDeque<TestItem>();
        val items = new ArrayDeque<Long>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            val item = new TestItem(i);
            d.addLast(item);
            items.addLast(item.key());
            checkFirstLast(d, items);
            if (i == 0) {
                // Only one item; don't remove anything.
                continue;
            }
            if (i % removeFirstEvery == 0) {
                val first = d.removeFirst();
                Assert.assertEquals("Unexpected first item removed.", (long) items.peekFirst(), first.key());
                items.removeFirst();
                checkFirstLast(d, items);
            }

            if (i % removeLastEvery == 0) {
                val last = d.removeLast();
                Assert.assertEquals("Unexpected last item removed.", (long) items.peekLast(), last.key());
                items.removeLast();
                checkFirstLast(d, items);
            }

            if (i % removeLastMultiEvery == 0) {
                long removeKey = -1;
                for (int j = 0; j < removeMultiCount; j++) {
                    removeKey = items.removeLast();
                }

                val last = d.removeLast(new TestItem(removeKey));
                Assert.assertNotNull("Expecting non-null result when removing " + removeKey, last);
                Assert.assertEquals("Unexpected last item removed.", removeKey, last.key());
                checkFirstLast(d, items);
            }

            if (i % removeFirstMultiEvery == 0) {
                long removeKey = -1;
                for (int j = 0; j < removeMultiCount; j++) {
                    removeKey = items.removeFirst();
                }

                val first = d.removeFirst(new TestItem(removeKey));
                Assert.assertNotNull("Expecting non-null result when removing " + removeKey, first);
                Assert.assertEquals("Unexpected last item removed.", removeKey, first.key());
                checkFirstLast(d, items);
            }
        }
    }

    private void checkFirstLast(SortedDeque<TestItem> d, Deque<Long> items) {
        if (items.isEmpty()) {
            Assert.assertTrue("Expected an empty Deque.", d.isEmpty());
            return;
        }

        Assert.assertEquals("Unexpected first item.", (long) items.peekFirst(), d.peekFirst().key());
        Assert.assertEquals("Unexpected last item.", (long) items.peekLast(), d.peekLast().key());
    }

    @RequiredArgsConstructor
    private static class TestItem implements SortedIndex.IndexEntry {
        private final long key;

        @Override
        public long key() {
            return this.key;
        }
    }
}
