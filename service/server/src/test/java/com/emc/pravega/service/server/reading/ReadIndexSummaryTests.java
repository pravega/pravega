/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.reading;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ReadIndexSummary class.
 */
public class ReadIndexSummaryTests {
    private static final int GENERATION_COUNT = 100;
    private static final int ITEMS_PER_GENERATION = 100;
    private static final int MAX_ITEM_SIZE = 1000;

    /**
     * Tests the basic functionality of adding elements and then removing them.
     */
    @Test
    public void testAddRemove() {
        ReadIndexSummary s = new ReadIndexSummary();
        long totalSize = 0;
        Random random = new Random();
        Queue<Integer> addedSizes = new LinkedList<>();

        // Add a few.
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            s.setCurrentGeneration(generation);
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                int size = random.nextInt(MAX_ITEM_SIZE);
                addedSizes.add(size);
                totalSize += size;
                int returnedGeneration = s.add(size);
                Assert.assertEquals("Unexpected return value from add().", generation, returnedGeneration);

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
                Assert.assertEquals("Unexpected total size.", totalSize, currentStatus.getSize());
                Assert.assertEquals("Not expecting a change in oldest generation after just adding.", 0, currentStatus.getOldestGeneration());
                Assert.assertEquals("Unexpected newest generation.", generation, currentStatus.getNewestGeneration());
            }
        }

        // Remove them all.
        testRemove(addedSizes, totalSize, ITEMS_PER_GENERATION - 1, s);
    }

    /**
     * Tests the basic functionality of adding elements with explicit generations.
     */
    @Test
    public void testAddExplicitGeneration() {
        ReadIndexSummary s = new ReadIndexSummary();
        long totalSize = 0;
        Random random = new Random(0);
        Queue<Integer> addedSizes = new LinkedList<>();
        s.setCurrentGeneration(GENERATION_COUNT + 1);

        // Add a few.
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                // All sizes are chosen at random, except for the last one. We want to make sure this works with a zero value for size.
                int size = 0;
                if (i < ITEMS_PER_GENERATION - 1) {
                    size = random.nextInt(MAX_ITEM_SIZE);
                }

                addedSizes.add(size);
                totalSize += size;
                s.add(size, generation);

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
                Assert.assertEquals("Unexpected total size.", totalSize, currentStatus.getSize());
                Assert.assertEquals("Not expecting a change in oldest generation after just adding.", 0, currentStatus.getOldestGeneration());
                Assert.assertEquals("Unexpected newest generation.", generation, currentStatus.getNewestGeneration());
            }
        }

        testRemove(addedSizes, totalSize, GENERATION_COUNT - 1, s);
    }

    private void testRemove(Queue<Integer> addedSizes, long totalSize, int maxGeneration, ReadIndexSummary s) {
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                int size = addedSizes.poll();
                totalSize -= size;
                s.remove(size, generation);

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
                Assert.assertEquals("Unexpected total size.", totalSize, currentStatus.getSize());
                if (i < ITEMS_PER_GENERATION - 1) {
                    Assert.assertEquals("Not expecting a change in oldest generation when there are elements still in that generation.", generation, currentStatus.getOldestGeneration());
                } else {
                    Assert.assertNotEquals("Expected a change in oldest generation when all elements in that generation were removed.", generation, currentStatus.getOldestGeneration());
                }

                if (addedSizes.size() > 0) {
                    Assert.assertEquals("Unexpected newest generation.", maxGeneration, currentStatus.getNewestGeneration());
                } else {
                    // We are done; newest generation doesn't make any sense, so expect 0.
                    Assert.assertEquals("Unexpected newest generation.", 0, currentStatus.getNewestGeneration());
                }
            }
        }
    }

    /**
     * Tests the functionality of touchOne - moving an item from one generation to the newest generation.
     */
    @Test
    public void testTouchOne() {
        ReadIndexSummary s = new ReadIndexSummary();
        long totalSize = 0;
        Random random = new Random();
        Queue<Integer> addedSizes = new LinkedList<>();
        int maxGeneration = ITEMS_PER_GENERATION - 1;

        // Add a few.
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            s.setCurrentGeneration(generation);
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                int size = random.nextInt(MAX_ITEM_SIZE);
                addedSizes.add(size);
                totalSize += size;
                s.add(size);
            }
        }

        // Touch each item, one by one, in each generation.
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                int returnedGeneration = s.touchOne(generation);
                Assert.assertEquals("Unexpected return value from touchOne().", maxGeneration, returnedGeneration);

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
                Assert.assertEquals("Not expecting a change in total size.", totalSize, currentStatus.getSize());

                if (i < ITEMS_PER_GENERATION - 1) {
                    Assert.assertEquals(
                            "Not expecting a change in oldest generation when there are elements still in that generation.",
                            generation,
                            currentStatus.getOldestGeneration());
                } else if (generation != maxGeneration) {
                    Assert.assertNotEquals(
                            "Expected a change in oldest generation when all elements in that generation were removed.",
                            generation,
                            currentStatus.getOldestGeneration());
                }

                Assert.assertEquals("Not expecting a change in newest generation.", maxGeneration, currentStatus.getNewestGeneration());
            }
        }

        CacheManager.CacheStatus currentStatus = s.toCacheStatus();
        Assert.assertEquals("Unexpected total size after touching all items.", totalSize, currentStatus.getSize());
        Assert.assertEquals("Unexpected newest generation after touching all items.", maxGeneration, currentStatus.getNewestGeneration());
        Assert.assertEquals("Unexpected oldest generation after touching all items.", maxGeneration, currentStatus.getOldestGeneration());

        // Now remove all items
        for (int size : addedSizes) {
            s.remove(size, maxGeneration);
        }

        currentStatus = s.toCacheStatus();
        Assert.assertEquals("Unexpected total size after removing all items.", 0, currentStatus.getSize());
        Assert.assertEquals("Unexpected newest generation after removing all items.", 0, currentStatus.getNewestGeneration());
        Assert.assertEquals("Unexpected oldest generation after removing all items.", 0, currentStatus.getOldestGeneration());
    }
}
