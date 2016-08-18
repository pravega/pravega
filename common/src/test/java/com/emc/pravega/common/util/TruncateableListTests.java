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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.Assert;
import org.junit.Test;

import com.emc.pravega.testcommon.AssertExtensions;


/**
 * Unit tests for TruncateableList class.
 */
public class TruncateableListTests {
    private static final int ITEM_COUNT = 100;

    /**
     * Tests the combination of the basic append() method and read().
     */
    @Test
    public void testAddRead() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(i);
            Assert.assertEquals("Unexpected value from size.", i + 1, list.size());
        }

        //Read 1/2 items
        Iterator<Integer> readResult = list.read(i -> true, ITEM_COUNT / 2);
        checkRange("Read first 50%", 0, ITEM_COUNT / 2 - 1, readResult);

        // Read all items
        readResult = list.read(i -> true, ITEM_COUNT);
        checkRange("Read all items", 0, ITEM_COUNT - 1, readResult);

        // Try to read more items.
        readResult = list.read(i -> true, ITEM_COUNT * 2);
        checkRange("Read more items than list has", 0, ITEM_COUNT - 1, readResult);

        // Read 25% of items, starting at the middle point.
        readResult = list.read(i -> i >= ITEM_COUNT / 2, ITEM_COUNT / 4);
        checkRange("Read 25% starting at 50%", ITEM_COUNT / 2, ITEM_COUNT / 2 + ITEM_COUNT / 4 - 1, readResult);
    }

    /**
     * Tests the functionality of the addIf() method.
     */
    @Test
    public void testAddIf() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            final int currentValue = i;

            // Happy case.
            boolean resultValue = list.addIf(currentValue, prev -> prev < currentValue);
            Assert.assertTrue("Unexpected return value from addIf for successful append.", resultValue);
            Assert.assertEquals("Unexpected value from size after successful append.", i + 1, list.size());

            // Unhappy case
            resultValue = list.addIf(currentValue, prev -> prev > currentValue);
            Assert.assertFalse("Unexpected return value from addIf for unsuccessful append.", resultValue);
            Assert.assertEquals("Unexpected value from size after unsuccessful append.", i + 1, list.size());
        }

        Iterator<Integer> readResult = list.read(i -> true, ITEM_COUNT * 2);
        checkRange("AddIf", 0, ITEM_COUNT - 1, readResult);
    }

    /**
     * Tests the functionality of the truncate() method.
     */
    @Test
    public void testTruncate() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(i);
        }

        // Truncate 25% of items.
        list.truncate(i -> i < ITEM_COUNT / 4);
        Assert.assertEquals("Unexpected value for size after truncating 25% items.", ITEM_COUNT - ITEM_COUNT / 4, list.size());
        checkRange("Truncate 25%", ITEM_COUNT / 4, ITEM_COUNT - 1, list.read(i -> true, ITEM_COUNT));

        // Truncate the same 25% of items - verify no change.
        list.truncate(i -> i < ITEM_COUNT / 4);
        Assert.assertEquals("Unexpected value for size after re-truncating first 25% items.", ITEM_COUNT - ITEM_COUNT / 4, list.size());
        checkRange("Re-truncate 25%", ITEM_COUNT / 4, ITEM_COUNT - 1, list.read(i -> true, ITEM_COUNT));

        // Truncate all items.
        list.truncate(i -> true);
        Assert.assertEquals("Unexpected value for size after truncating all items.", 0, list.size());
        Iterator<Integer> readResult = list.read(i -> true, ITEM_COUNT * 2);
        Assert.assertFalse("List should be empty.", readResult.hasNext());
    }

    /**
     * Tests the iterator while concurrently adding or truncating items.
     */
    @Test
    public void testConcurrentIterator() {
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(i);
        }

        // Test with additions.
        Iterator<Integer> addIterator = list.read(i -> true, ITEM_COUNT * 2);
        int firstValue = addIterator.next();
        Assert.assertEquals("Unexpected first value, pre-modification.", 0, firstValue);
        list.add(ITEM_COUNT);
        checkRange("Post-addition.", 1, ITEM_COUNT, addIterator);

        // Test with truncation.
        Iterator<Integer> truncateIterator = list.read(i -> true, ITEM_COUNT * 2);
        truncateIterator.next(); // Read 1 value
        list.truncate(i -> i < 10);
        Assert.assertFalse("Unexpected value from hasNext when list has been truncated.", truncateIterator.hasNext());
        AssertExtensions.assertThrows(
                "Unexpected behavior from next() when current element has been truncated.",
                truncateIterator::next,
                ex -> ex instanceof NoSuchElementException);

        // Test when we do a truncation between calls to hasNext() and next().
        // This is always a possibility. If the list gets truncated between calls to hasNext() and next(), we cannot
        // return a truncated element. This means we expect a NoSuchElementException to be thrown from next(), even though
        // the previous call to hasNext() returned true (now it should return false).
        Iterator<Integer> midTruncateIterator = list.read(i -> true, ITEM_COUNT * 2);
        Assert.assertTrue("Unexpected value from hasNext when not been truncated.", midTruncateIterator.hasNext());
        list.truncate(i -> i < 20);
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
        TruncateableList<Integer> list = new TruncateableList<>();
        for (int i = 0; i < ITEM_COUNT; i++) {
            list.add(i);
        }

        list.clear();
        Iterator<Integer> readResult = list.read(i -> true, ITEM_COUNT * 2);
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

