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
package io.pravega.segmentstore.server.reading;

import io.pravega.segmentstore.server.CacheManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the ReadIndexSummary class.
 */
public class ReadIndexSummaryTests {
    private static final int GENERATION_COUNT = 100;
    private static final int ITEMS_PER_GENERATION = 100;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the basic functionality of adding elements and then removing them.
     */
    @Test
    public void testAddRemove() {
        ReadIndexSummary s = new ReadIndexSummary();

        // Add a few.
        int count = 0;
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            s.setCurrentGeneration(generation);
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                int returnedGeneration = s.addOne();
                count++;
                Assert.assertEquals("Unexpected return value from addOne().", generation, returnedGeneration);

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
                Assert.assertEquals("Not expecting a change in oldest generation after just adding.", 0, currentStatus.getOldestGeneration());
                Assert.assertEquals("Unexpected newest generation.", generation, currentStatus.getNewestGeneration());
            }
        }

        // Remove them all.
        testRemove(count, ITEMS_PER_GENERATION - 1, s);
    }

    /**
     * Tests the basic functionality of adding elements with explicit generations.
     */
    @Test
    public void testAddExplicitGeneration() {
        ReadIndexSummary s = new ReadIndexSummary();
        s.setCurrentGeneration(GENERATION_COUNT + 1);

        // Add a few.
        int count = 0;
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                // All sizes are chosen at random, except for the last one. We want to make sure this works with a zero value for size.
                s.addOne(generation);
                count++;

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
                Assert.assertEquals("Not expecting a change in oldest generation after just adding.", 0, currentStatus.getOldestGeneration());
                Assert.assertEquals("Unexpected newest generation.", generation, currentStatus.getNewestGeneration());
            }
        }

        testRemove(count, GENERATION_COUNT - 1, s);
    }

    private void testRemove(int count, int maxGeneration, ReadIndexSummary s) {
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                s.removeOne(generation);
                count--;

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
                if (i < ITEMS_PER_GENERATION - 1) {
                    Assert.assertEquals("Not expecting a change in oldest generation when there are elements still in that generation.", generation, currentStatus.getOldestGeneration());
                } else {
                    Assert.assertNotEquals("Expected a change in oldest generation when all elements in that generation were removed.", generation, currentStatus.getOldestGeneration());
                }

                if (count > 0) {
                    Assert.assertEquals("Unexpected newest generation.", maxGeneration, currentStatus.getNewestGeneration());
                } else {
                    // We are done; newest generation doesn't make any sense, so expect 0.
                    Assert.assertTrue("Expecting an empty status.", currentStatus.isEmpty());
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
        int maxGeneration = ITEMS_PER_GENERATION - 1;

        // Add a few.
        int count = 0;
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            s.setCurrentGeneration(generation);
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                s.addOne();
                count++;
            }
        }

        // Touch each item, one by one, in each generation.
        for (int generation = 0; generation < GENERATION_COUNT; generation++) {
            for (int i = 0; i < ITEMS_PER_GENERATION; i++) {
                int returnedGeneration = s.touchOne(generation);
                Assert.assertEquals("Unexpected return value from touchOne().", maxGeneration, returnedGeneration);

                CacheManager.CacheStatus currentStatus = s.toCacheStatus();
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
        Assert.assertEquals("Unexpected newest generation after touching all items.", maxGeneration, currentStatus.getNewestGeneration());
        Assert.assertEquals("Unexpected oldest generation after touching all items.", maxGeneration, currentStatus.getOldestGeneration());

        // Now remove all items
        for (int i = 0; i < count; i++) {
            s.removeOne(maxGeneration);
        }

        currentStatus = s.toCacheStatus();
        Assert.assertTrue("Expected cache to be empty after removing all items.", currentStatus.isEmpty());
    }
}
