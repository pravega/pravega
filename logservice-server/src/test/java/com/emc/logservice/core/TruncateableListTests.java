package com.emc.logservice.core;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Unit tests for TruncateableList class.
 */
public class TruncateableListTests {
    private static final int ItemCount = 100;

    /**
     * Tests the combination of the basic add() method and read().
     */
    @Test
    public void testAddRead() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ItemCount; i++) {
            list.add(i);
            Assert.assertEquals("Unexpected value from getSize.", i + 1, list.getSize());
        }

        //Read 1/2 items
        Iterator<Integer> readResult = list.read(i -> true, ItemCount / 2);
        checkRange("Read first 50%", 0, ItemCount / 2 - 1, readResult);

        // Read all items
        readResult = list.read(i -> true, ItemCount);
        checkRange("Read all items", 0, ItemCount - 1, readResult);

        // Try to read more items.
        readResult = list.read(i -> true, ItemCount * 2);
        checkRange("Read more items than list has", 0, ItemCount - 1, readResult);

        // Read 25% of items, starting at the middle point.
        readResult = list.read(i -> i >= ItemCount / 2, ItemCount / 4);
        checkRange("Read 25% starting at 50%", ItemCount / 2, ItemCount / 2 + ItemCount / 4 - 1, readResult);
    }

    /**
     * Tests the functionality of the addIf() method.
     */
    @Test
    public void testAddIf() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ItemCount; i++) {
            final int currentValue = i;

            // Happy case.
            boolean resultValue = list.addIf(currentValue, prev -> prev < currentValue);
            Assert.assertTrue("Unexpected return value from addIf for successful add.", resultValue);
            Assert.assertEquals("Unexpected value from getSize after successful add.", i + 1, list.getSize());

            // Unhappy case
            resultValue = list.addIf(currentValue, prev -> prev > currentValue);
            Assert.assertFalse("Unexpected return value from addIf for unsuccessful add.", resultValue);
            Assert.assertEquals("Unexpected value from getSize after unsuccessful add.", i + 1, list.getSize());
        }

        Iterator<Integer> readResult = list.read(i -> true, ItemCount * 2);
        checkRange("AddIf", 0, ItemCount - 1, readResult);
    }

    /**
     * Tests the functionality of the truncate() method.
     */
    @Test
    public void testTruncate() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ItemCount; i++) {
            list.add(i);
        }

        // Truncate 25% of items.
        list.truncate(i -> i < ItemCount / 4);
        Assert.assertEquals("Unexpected value for getSize after truncating 25% items.", ItemCount - ItemCount / 4, list.getSize());
        checkRange("Truncate 25%", ItemCount / 4, ItemCount - 1, list.read(i -> true, ItemCount));

        // Truncate the same 25% of items - verify no change.
        list.truncate(i -> i < ItemCount / 4);
        Assert.assertEquals("Unexpected value for getSize after re-truncating first 25% items.", ItemCount - ItemCount / 4, list.getSize());
        checkRange("Re-truncate 25%", ItemCount / 4, ItemCount - 1, list.read(i -> true, ItemCount));

        // Truncate all items.
        list.truncate(i -> true);
        Assert.assertEquals("Unexpected value for getSize after truncating all items.", 0, list.getSize());
        Iterator<Integer> readResult = list.read(i -> true, ItemCount * 2);
        Assert.assertFalse("List should be empty.", readResult.hasNext());
    }

    /**
     * Tests the functionality of the clear() method.
     */
    @Test
    public void testClear() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ItemCount; i++) {
            list.add(i);
        }

        list.clear();
        Iterator<Integer> readResult = list.read(i -> true, ItemCount * 2);
        Assert.assertFalse("List should be empty.", readResult.hasNext());
    }

    private void checkRange(String testDescription, int startElement, int endElement, Iterator<Integer> readResult) {
        for (int i = startElement; i <= endElement; i++) {
            Assert.assertTrue(testDescription + ": Unexpected value from hasNext when more elements are expected.", readResult.hasNext());
            int nextItem = readResult.next();
            Assert.assertEquals(testDescription + ": Unexpected next value from next.", i, nextItem);
        }

        Assert.assertFalse(testDescription + ": Unexpected value from hasNext when no more elements are expected.", readResult.hasNext());
    }
}

