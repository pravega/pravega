/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for SequencedItemList class.
 */
public class SequencedItemListTests {
    private static final int ITEM_COUNT = 100;
    private static final long START = Long.MIN_VALUE;
    private static final long END = Integer.MAX_VALUE;

    /**
     * Tests the combination of the basic append() method and read().
     */
    @Test
    public void testAddRead() {
        SequencedItemList<Item> list = new SequencedItemList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(new Item(i));
        }

        //Read 1/2 items
        Iterator<Item> readResult = list.read(START, ITEM_COUNT / 2);
        checkRange("Read first 50%", 0, ITEM_COUNT / 2 - 1, readResult);

        // Read all items
        readResult = list.read(START, ITEM_COUNT);
        checkRange("Read all items", 0, ITEM_COUNT - 1, readResult);

        // Try to read more items.
        readResult = list.read(START, ITEM_COUNT * 2);
        checkRange("Read more items than list has", 0, ITEM_COUNT - 1, readResult);

        // Read 25% of items, starting at the middle point.
        readResult = list.read(ITEM_COUNT / 2 - 1, ITEM_COUNT / 4);
        checkRange("Read 25% starting at 50%", ITEM_COUNT / 2, ITEM_COUNT / 2 + ITEM_COUNT / 4 - 1, readResult);
    }

    /**
     * Tests the functionality of the addIf() method.
     */
    @Test
    public void testAddIf() {
        SequencedItemList<Item> list = new SequencedItemList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            final Item currentValue = new Item(i);

            // Happy case.
            boolean resultValue = list.add(currentValue);
            Assert.assertTrue("Unexpected return value from addIf for successful append.", resultValue);

            // Unhappy case
            resultValue = list.add(currentValue);
            Assert.assertFalse("Unexpected return value from addIf for unsuccessful append.", resultValue);
        }

        Iterator<Item> readResult = list.read(START, ITEM_COUNT * 2);
        checkRange("AddIf", 0, ITEM_COUNT - 1, readResult);
    }

    /**
     * Tests the functionality of the truncate() method.
     */
    @Test
    public void testTruncate() {
        SequencedItemList<Item> list = new SequencedItemList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(new Item(i));
        }

        // Truncate 25% of items.
        list.truncate(ITEM_COUNT / 4 - 1);
        checkRange("Truncate 25%", ITEM_COUNT / 4, ITEM_COUNT - 1, list.read(START, ITEM_COUNT));

        // Truncate the same 25% of items - verify no change.
        list.truncate(ITEM_COUNT / 4 - 1);
        checkRange("Re-truncate 25%", ITEM_COUNT / 4, ITEM_COUNT - 1, list.read(START, ITEM_COUNT));

        // Truncate all items.
        list.truncate(END);
        Iterator<Item> readResult = list.read(START, ITEM_COUNT * 2);
        Assert.assertFalse("List should be empty.", readResult.hasNext());
    }

    /**
     * Tests the iterator while concurrently adding or truncating items.
     */
    @Test
    public void testConcurrentIterator() {
        SequencedItemList<Item> list = new SequencedItemList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(new Item(i));
        }

        // Test with additions.
        Iterator<Item> addIterator = list.read(START, ITEM_COUNT * 2);
        Item firstValue = addIterator.next();
        Assert.assertEquals("Unexpected first value, pre-modification.", 0, firstValue.getSequenceNumber());
        list.add(new Item(ITEM_COUNT));
        checkRange("Post-addition.", 1, ITEM_COUNT, addIterator);

        // Test with truncation.
        Iterator<Item> truncateIterator = list.read(START, ITEM_COUNT * 2);
        truncateIterator.next(); // Read 1 value
        list.truncate(10);
        Assert.assertFalse("Unexpected value from hasNext when list has been truncated.", truncateIterator.hasNext());
        AssertExtensions.assertThrows(
                "Unexpected behavior from next() when current element has been truncated.",
                truncateIterator::next,
                ex -> ex instanceof NoSuchElementException);

        // Test when we do a truncation between calls to hasNext() and next().
        // This is always a possibility. If the list gets truncated between calls to hasNext() and next(), we cannot
        // return a truncated element. This means we expect a NoSuchElementException to be thrown from next(), even though
        // the previous call to hasNext() returned true (now it should return false).
        Iterator<Item> midTruncateIterator = list.read(START, ITEM_COUNT * 2);
        Assert.assertTrue("Unexpected value from hasNext when not been truncated.", midTruncateIterator.hasNext());
        list.truncate(20);
        AssertExtensions.assertThrows(
                "Unexpected behavior from next() when current element has been truncated (after hasNext() and before next()).",
                midTruncateIterator::next,
                ex -> ex instanceof NoSuchElementException);
    }

    /**
     * Tests the functionality of the clear() method.
     */
    @Test
    public void testClear() {
        SequencedItemList<Item> list = new SequencedItemList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(new Item(i));
        }

        list.clear();
        Iterator<Item> readResult = list.read(START, ITEM_COUNT * 2);
        Assert.assertFalse("List should be empty.", readResult.hasNext());
    }

    private void checkRange(String testDescription, int startElement, int endElement, Iterator<Item> readResult) {
        for (int i = startElement; i <= endElement; i++) {
            Assert.assertTrue(testDescription + ": Unexpected value from hasNext when more elements are expected.", readResult.hasNext());
            Item nextItem = readResult.next();
            Assert.assertEquals(testDescription + ": Unexpected next value from next.", i, nextItem.getSequenceNumber());
        }

        Assert.assertFalse(testDescription + ": Unexpected value from hasNext when no more elements are expected.", readResult.hasNext());
    }

    @Data
    private static class Item implements SequencedItemList.Element {
        final long sequenceNumber;
    }
}

