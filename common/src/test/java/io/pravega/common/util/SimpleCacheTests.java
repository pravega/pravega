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
import io.pravega.test.common.IntentionalException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link SimpleCache} class.
 */
public class SimpleCacheTests {
    private static final int INFINITE_SIZE = Integer.MAX_VALUE;
    private static final Duration INFINITE_TIME = Duration.ofNanos(Long.MAX_VALUE);
    private static final int DEFAULT_COUNT = 1000;
    private final Random random = new Random(0);

    /**
     * Tests {@link SimpleCache} when there is no eviction expected (size or time) with all keys in the same order.
     */
    @Test
    public void testNoEvictionInOrder() {
        testNoEviction(this::inOrder);
    }

    /**
     * Tests {@link SimpleCache} when there is no eviction expected (size or time) with keys in arbitrary order.
     */
    @Test
    public void testNoEvictionShuffled() {
        testNoEviction(this::shuffled);
    }

    private void testNoEviction(BiFunction<Integer, Integer, List<Long>> getKeys) {
        val count = DEFAULT_COUNT;
        val evictions = new ArrayList<Map.Entry<Long, Long>>();
        val c = new SimpleCache<Long, Long>(INFINITE_SIZE, INFINITE_TIME, (k, v) -> evictions.add(new AbstractMap.SimpleImmutableEntry<>(k, v)));
        Assert.assertEquals(INFINITE_SIZE, c.getMaxSize());

        // PutIfAbsent.
        int expectedSize = 0;
        val expectedOrder = new ArrayDeque<Map.Entry<Long, Long>>();
        for (val key : getKeys.apply(0, count)) {
            val value = value(key);
            expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            expectedSize++;

            val r1 = c.putIfAbsent(key, value);
            Assert.assertNull(r1);

            val r2 = c.putIfAbsent(key, value + 1);
            Assert.assertEquals(value, (long) r2);

            val getResult = c.get(key);
            Assert.assertEquals(value, (long) getResult);
            Assert.assertEquals(expectedSize, c.size());
        }

        checkExpectedOrder(c, expectedOrder);

        // Put.
        expectedOrder.clear();
        for (val key : getKeys.apply(0, count)) {
            val oldValue = value(key);
            val newValue = oldValue + 1;
            val r1 = c.put(key, newValue);
            Assert.assertEquals(oldValue, (long) r1);

            val getResult = c.get(key);
            Assert.assertEquals(newValue, (long) getResult);
            expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, newValue));
        }

        Assert.assertEquals(expectedSize, c.size());
        checkExpectedOrder(c, expectedOrder);

        // Get.
        expectedOrder.clear();
        for (val key : getKeys.apply(0, count)) {
            val expectedValue = value(key) + 1;
            val getResult = c.get(key);
            Assert.assertEquals(expectedValue, (long) getResult);
            expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, expectedValue));
        }

        checkExpectedOrder(c, expectedOrder);

        // Remove.
        expectedOrder.clear();
        for (val key : getKeys.apply(0, count)) {
            val expectedValue = value(key) + 1;
            val r1 = c.remove(key);
            Assert.assertEquals(expectedValue, (long) r1);
            expectedSize--;
            Assert.assertEquals(expectedSize, c.size());

            val getResult = c.get(key);
            Assert.assertNull(getResult);

            val r2 = c.remove(key);
            Assert.assertNull(r2);
        }

        checkExpectedOrder(c, expectedOrder);

        // Put (after removal).
        expectedOrder.clear();
        for (val key : getKeys.apply(0, count)) {
            val value = value(key);
            val r1 = c.put(key, value);
            Assert.assertNull(r1);

            val getResult = c.get(key);
            Assert.assertEquals(value, (long) getResult);
            expectedSize++;
            Assert.assertEquals(expectedSize, c.size());
            expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
        }

        checkExpectedOrder(c, expectedOrder);
        Assert.assertEquals("Not expecting evictions.", 0, evictions.size());
    }

    /**
     * Tests {@link SimpleCache} when the only eviction expected is size-based with all keys in the same order.
     */
    @Test
    public void testSizeBasedEvictionInOrder() {
        testSizeBasedEviction(this::inOrder);
    }

    /**
     * Tests {@link SimpleCache} when the only eviction expected is size-based with all keys in arbitrary order.
     */
    @Test
    public void testSizeBasedEvictionShuffled() {
        testSizeBasedEviction(this::shuffled);
    }

    private void testSizeBasedEviction(BiFunction<Integer, Integer, List<Long>> getKeys) {
        val count = DEFAULT_COUNT;
        val maxSize = count / 10;
        val evictions = new ArrayList<Map.Entry<Long, Long>>();
        val c = new SimpleCache<Long, Long>(maxSize, INFINITE_TIME, (k, v) -> evictions.add(new AbstractMap.SimpleImmutableEntry<>(k, v)));
        Assert.assertEquals(maxSize, c.getMaxSize());

        // PutIfAbsent.
        int expectedSize = 0;
        val expectedOrder = new ArrayDeque<Map.Entry<Long, Long>>();
        val expectedEvictions = new ArrayList<Map.Entry<Long, Long>>();
        val remainingKeys = new HashSet<Long>();
        for (val key : getKeys.apply(0, count)) {
            val value = value(key);
            expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            if (expectedSize == maxSize) {
                expectedEvictions.add(expectedOrder.removeFirst());
            } else {
                expectedSize++;
            }

            val r1 = c.putIfAbsent(key, value);
            Assert.assertNull(r1);

            val r2 = c.putIfAbsent(key, value + 1);
            Assert.assertEquals(value, (long) r2);
        }

        // Validate size.
        Assert.assertEquals(expectedSize, c.size());

        // Validate evictions were in the right order.
        checkEvictions(expectedEvictions, evictions);
        checkExpectedOrder(c, expectedOrder);
        expectedOrder.stream().map(Map.Entry::getKey).forEach(remainingKeys::add);

        // Put.
        evictions.clear();
        expectedEvictions.clear();
        for (val key : getKeys.apply(0, count)) {
            val oldValue = value(key);
            val newValue = oldValue + 1;
            val isCached = remainingKeys.contains(key);
            val putResult = c.put(key, newValue);
            if (isCached) {
                // Value still in the cache; update its order.
                Assert.assertEquals(oldValue, (long) putResult);
                expectedOrder.removeIf(e -> e.getKey().equals(key));
            } else {
                // This value has been evicted already and is now going to be re-inserted.
                Assert.assertNull(putResult);
                remainingKeys.add(key);
                val expectedEvicted = expectedOrder.removeFirst();
                expectedEvictions.add(expectedEvicted);
                remainingKeys.remove(expectedEvicted.getKey());
            }

            expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, newValue));
        }

        Assert.assertEquals(expectedSize, c.size());
        checkEvictions(expectedEvictions, evictions);
        checkExpectedOrder(c, expectedOrder);

        // Get.
        evictions.clear();
        expectedEvictions.clear();
        expectedOrder.clear();
        for (val key : getKeys.apply(0, count)) {
            val expectedValue = value(key) + 1;
            val isCached = remainingKeys.contains(key);
            val getResult = c.get(key);
            if (isCached) {
                Assert.assertEquals(expectedValue, (long) getResult);
                expectedOrder.removeIf(e -> e.getKey().equals(key));
                expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, expectedValue));
            } else {
                Assert.assertNull(getResult);
            }
        }

        Assert.assertEquals(expectedSize, c.size());
        checkEvictions(expectedEvictions, evictions);
        checkExpectedOrder(c, expectedOrder);

        // Remove.
        evictions.clear();
        expectedEvictions.clear();
        expectedOrder.clear();
        for (val key : getKeys.apply(0, count)) {
            val expectedValue = value(key) + 1;
            val isCached = remainingKeys.contains(key);

            val r1 = c.remove(key);
            if (isCached) {
                Assert.assertEquals(expectedValue, (long) r1);
                expectedSize--;
            } else {
                Assert.assertNull(r1);
            }

            val getResult = c.get(key);
            Assert.assertNull(getResult);

            val r2 = c.remove(key);
            Assert.assertNull(r2);
        }

        Assert.assertEquals(expectedSize, c.size());
        Assert.assertEquals(0, evictions.size());
        Assert.assertEquals(0, expectedOrder.size());

        // Put (after removal).
        evictions.clear();
        expectedEvictions.clear();
        expectedOrder.clear();
        for (val key : getKeys.apply(0, count)) {
            val value = value(key);
            val r1 = c.put(key, value);
            Assert.assertNull(r1);

            val r2 = c.put(key, value + 1);
            Assert.assertEquals(value, (long) r2);

            expectedOrder.add(new AbstractMap.SimpleImmutableEntry<>(key, value + 1));
            if (expectedSize == maxSize) {
                expectedEvictions.add(expectedOrder.removeFirst());
            } else {
                expectedSize++;
            }
        }

        Assert.assertEquals(expectedSize, c.size());
        checkEvictions(expectedEvictions, evictions);
        checkExpectedOrder(c, expectedOrder);
    }

    /**
     * Tests {@link SimpleCache} when the only eviction based is time based.
     */
    @Test
    public void testTimeBasedEviction() {
        val expirationTimeNanos = 100;
        val evictions = new HashMap<Long, Long>();
        val currentTime = new AtomicLong(0);
        val key1 = 1L;
        val value1 = value(key1);
        val c = new SimpleCache<>(INFINITE_SIZE, Duration.ofNanos(expirationTimeNanos), evictions::put, currentTime::get);

        // PutIfAbsent.
        currentTime.set(0);
        val p1 = c.putIfAbsent(key1, value1);
        Assert.assertNull(p1);
        Assert.assertEquals(value1, (long) c.putIfAbsent(key1, value1 + 1));
        currentTime.addAndGet(expirationTimeNanos + 1); // This will expire key1, but not evict it.
        val value2 = value1 + 2;
        val p2 = c.putIfAbsent(key1, value2); // This will reinsert key1 and trigger the eviction of the expired one.
        Assert.assertNull(p2);
        Assert.assertEquals(1, evictions.size());
        Assert.assertEquals(value1, (long) evictions.get(key1));
        Assert.assertEquals(value2, (long) c.get(key1));
        Assert.assertEquals(1, c.size());

        // Put.
        currentTime.addAndGet(expirationTimeNanos + 1); // This will expire key1, but not evict it.
        evictions.clear();
        val value3 = value2 + 1;
        val p3 = c.put(key1, value3); // This should reinsert key1 and trigger the eviction of the expired one.
        Assert.assertNull(p3);
        Assert.assertEquals(1, evictions.size());
        Assert.assertEquals(value2, (long) evictions.get(key1));
        Assert.assertEquals(value3, (long) c.get(key1));
        currentTime.addAndGet(expirationTimeNanos + 1); // This will expire key1, but not evict it.
        evictions.clear();
        val value4 = value3 + 1;
        val p4 = c.put(key1, value4); // This should reinsert key1 and trigger the eviction of the expired one.
        Assert.assertNull(p4);
        Assert.assertEquals(1, evictions.size());
        Assert.assertEquals(value3, (long) evictions.get(key1));
        Assert.assertEquals(value4, (long) c.get(key1));
        Assert.assertEquals(1, c.size());

        // Get.
        currentTime.addAndGet(expirationTimeNanos + 1); // This will expire key1, but not evict it.
        evictions.clear();
        val g1 = c.get(key1);
        Assert.assertNull(g1);
        Assert.assertEquals(1, evictions.size());
        Assert.assertEquals(value4, (long) evictions.get(key1));
        Assert.assertEquals(0, c.size());

        // Remove.
        evictions.clear();
        val value5 = value4 + 1;
        val p5 = c.putIfAbsent(key1, value5);
        Assert.assertNull(p5);
        currentTime.addAndGet(expirationTimeNanos + 1); // This will expire key1, but not evict it.
        val r1 = c.remove(key1);
        Assert.assertNull(r1);
        Assert.assertEquals(0, evictions.size()); // No eviction expected. We manually removed this item.
        Assert.assertEquals(0, c.size());
    }

    /**
     * Tests {@link SimpleCache} cleanup.
     */
    @Test
    public void testCleanup() {
        val timeIncreaseNanosDelta = 100;
        val expirationTimeNanos = DEFAULT_COUNT * 1000;
        val count = DEFAULT_COUNT;
        val evictions = new HashMap<Long, Long>();
        val currentTime = new AtomicLong(0);
        val c = new SimpleCache<>(INFINITE_SIZE, Duration.ofNanos(expirationTimeNanos), evictions::put, currentTime::get);

        // First we insert the keys, in order.
        int nextTimeIncrease = 1;
        int i = 0;
        for (val key : inOrder(0, count)) {
            val value = value(key);
            c.put(key, value);

            if (++i == nextTimeIncrease) {
                // We increase the time in "increasing batches". First one has 1 key, second has 2, then 4, 8, ...
                currentTime.addAndGet(timeIncreaseNanosDelta);
                nextTimeIncrease *= 2;
            }
        }

        val evictionBatches = new ArrayList<HashMap<Long, Long>>();
        HashMap<Long, Long> currentBatch = new HashMap<>();
        val initialTime = currentTime.get();

        // Shuffle the keys.
        nextTimeIncrease = 1;
        i = 0;
        for (val key : shuffled(0, count)) {
            val expectedValue = value(key);
            val actualValue = c.get(key);
            currentBatch.put(key, expectedValue);
            Assert.assertEquals(expectedValue, (long) actualValue);

            if (++i == nextTimeIncrease) {
                // We increase the time in "increasing batches". First one has 1 key, second has 2, then 4, 8, ...
                currentTime.addAndGet(timeIncreaseNanosDelta);
                nextTimeIncrease *= 2;
                evictionBatches.add(currentBatch);
                currentBatch = new HashMap<>();
            }
        }

        evictionBatches.add(currentBatch); // Add the last one.

        currentTime.set(initialTime + expirationTimeNanos + 1);
        for (val evictionBatch : evictionBatches) {
            evictions.clear();
            c.cleanUp();
            AssertExtensions.assertMapEquals("", evictionBatch, evictions);

            evictions.clear();
            currentTime.addAndGet(timeIncreaseNanosDelta);
        }

        Assert.assertEquals("Expected everything to be evicted.", 0, c.size());
    }

    /**
     * Tests the case when the onExpiration callback passed to the {@link SimpleCache} throws an exception.
     */
    @Test
    public void testOnExpirationFailure() {
        val expirationTimeNanos = 1000;
        val entryCount = 3;
        val failOnEntryId = 1L; // 0-based, so 1 is the entry in the middle.
        val evictions = new HashMap<Long, Long>();
        val evictionCount = new AtomicInteger(0);
        BiConsumer<Long, Long> evictionConsumer = (key, value) -> {
            evictions.put(key, value);
            evictionCount.incrementAndGet();
            if (key == failOnEntryId) {
                throw new IntentionalException();
            }
        };

        val currentTime = new AtomicLong(0);
        val c = new SimpleCache<>(INFINITE_SIZE, Duration.ofNanos(expirationTimeNanos), evictionConsumer, currentTime::get);

        for (long key = 0; key < entryCount; key++) {
            c.put(key, value(key));
        }

        Assert.assertEquals(0, evictions.size());

        // Force an eviction by increasing the time.
        currentTime.set(expirationTimeNanos + 1);
        c.cleanUp();

        // Verify that we have the correct number of evictions and that they have been recorded with the correct value.
        Assert.assertEquals(entryCount, evictionCount.get());
        for (int i = 0; i < entryCount; i++) {
            Assert.assertEquals(value(i), (long) evictions.get((long) i));
        }
    }

    private void checkEvictions(List<Map.Entry<Long, Long>> expected, List<Map.Entry<Long, Long>> actual) {
        AssertExtensions.assertListEquals("", expected, actual, Map.Entry::equals);
    }

    private void checkExpectedOrder(SimpleCache<Long, Long> c, ArrayDeque<Map.Entry<Long, Long>> expected) {
        val actual = c.getUnexpiredEntriesInOrder();
        val expectedList = new ArrayList<>(expected);
        AssertExtensions.assertListEquals("", expectedList, actual, Map.Entry::equals);
    }

    private long value(long key) {
        return -key - 1;
    }

    private List<Long> inOrder(int start, int end) {
        return LongStream.range(start, end).boxed().collect(Collectors.toList());
    }

    private List<Long> shuffled(int start, int end) {
        val list = inOrder(start, end);
        Collections.shuffle(list, random);
        return list;
    }
}
