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
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongFunction;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.common.util.SearchUtils.binarySearch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SearchUtilsTest {
    
    protected Entry<Long, Long> newtonianSearch(LongFunction<Long> getValue, long fromIdx, long toIdx, long target, boolean greater) {
        return SearchUtils.newtonianSearch(getValue, fromIdx, toIdx, target, greater);
    }
    
    public static class AsyncSearchUtilsTest extends SearchUtilsTest {
        protected Entry<Long, Long> newtonianSearch(LongFunction<Long> getValue, long fromIdx, long toIdx, long target, boolean greater) {
            return SearchUtils.asyncNewtonianSearch((long idx) -> {
                return CompletableFuture.completedFuture(getValue.apply(idx));
            }, fromIdx, toIdx, target, greater).join();
        }
    }

    @Test
    public void targetValuePresent() {
        LongFunction<Long> getValue = i -> (long) (i * 2); // Custom data structure where values are
        // twice the index
        int start = 0;
        int end = 9;
        long target = 8;

        Entry<Long, Long> result = newtonianSearch(getValue, start, end, target, false);

        assertNotNull(result);
        assertEquals(4, result.getKey().intValue()); // Expected index of the target
        assertEquals(8, result.getValue().longValue()); // Expected value at the target index

        result = newtonianSearch(getValue, start, end, target, true);

        assertNotNull(result);
        assertEquals(4, result.getKey().intValue());
        assertEquals(8, result.getValue().longValue());
    }

    @Test
    public void twoElementList() {
        LongFunction<Long> getValue = i -> (long) (i * 2);

        int start = 0;
        int end = 1;

        Entry<Long, Long> result = newtonianSearch(getValue, start, end, 3, false);
        assertEquals(1, result.getKey().intValue());
        assertEquals(2, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, -1, false);
        assertEquals(0, result.getKey().intValue());
        assertEquals(0, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 0, false);
        assertEquals(0, result.getKey().intValue());
        assertEquals(0, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 1, false);
        assertEquals(0, result.getKey().intValue());
        assertEquals(0, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 2, false);
        assertEquals(1, result.getKey().intValue());
        assertEquals(2, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 3, false);
        assertEquals(1, result.getKey().intValue());
        assertEquals(2, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 3, true);
        assertEquals(1, result.getKey().intValue());
        assertEquals(2, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, -1, true);
        assertEquals(0, result.getKey().intValue());
        assertEquals(0, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 0, true);
        assertEquals(0, result.getKey().intValue());
        assertEquals(0, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 1, true);
        assertEquals(1, result.getKey().intValue());
        assertEquals(2, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 2, true);
        assertEquals(1, result.getKey().intValue());
        assertEquals(2, result.getValue().longValue());

        result = newtonianSearch(getValue, start, end, 3, true);
        assertEquals(1, result.getKey().intValue());
        assertEquals(2, result.getValue().longValue());
    }

    @Test
    public void testNewtonianLengthsAndPositions() {
        for (int length = 1; length <= 10; length++) {
            for (int multiple = 1; multiple <= 5; multiple++) {
                for (int target = -1; target <= length * multiple; target++) {
                    testNewtonianSearchForLengthAndPosition(multiple, length, target);
                }
            }
        }
    }

    private void testNewtonianSearchForLengthAndPosition(int multiple, int length, int target) {
        CountingFunction getValue = new CountingFunction(i -> i * multiple);

        long fromIdx = 0;
        long toIdx = length - 1;
        int maxExpectedInvocationCount;

        Map.Entry<Long, Long> expected;
        if (target <= 0) {
            maxExpectedInvocationCount = Math.min(length, 2);
            expected = new AbstractMap.SimpleEntry<>(fromIdx, getValue.apply(fromIdx));
        } else if (target >= (length - 1) * multiple ) {
            maxExpectedInvocationCount = Math.min(length, 2);
            expected = new AbstractMap.SimpleEntry<>(toIdx, getValue.apply(toIdx));
        } else {
            maxExpectedInvocationCount = Math.min(length, 4);
            long expectedIdx = target / multiple;
            expected = new AbstractMap.SimpleEntry<>(expectedIdx, getValue.apply(expectedIdx));
        }

        getValue.resetInvocationCount();

        Entry<Long, Long> actual = newtonianSearch(getValue, fromIdx, toIdx, target, false);

        assertEquals("Incorrect index for length " + length + " multiple " + multiple + " and target " + target,
                expected.getKey(), actual.getKey());
        assertEquals("Incorrect value for length " + length + " multiple " + multiple + " and target " + target,
                expected.getValue(), actual.getValue());
        assertTrue("Incorrect number of invocations for length " + length + " multiple " + multiple + " and target " + target,
                getValue.getInvocationCount() <= maxExpectedInvocationCount);
    }

    @Test
    public void testBinaryLengthsAndPositions() {
        for (int length = 1; length <= 10; length++) {
            for (int position = -1; position <= length; position++) {
                testBinarySearchForLengthAndPosition(length, position);
            }
        }
    }

    private void testBinarySearchForLengthAndPosition(int length, int position) {
        CountingFunction getValue = new CountingFunction(i -> i);

        int fromIdx = 0;
        int toIdx = length - 1;
        long target = position;

        Map.Entry<Integer, Long> expected;
        if (position <= 0) {
            expected = new AbstractMap.SimpleEntry<>(fromIdx, getValue.apply(fromIdx));
        } else if (position >= length - 1) {
            expected = new AbstractMap.SimpleEntry<>(toIdx, getValue.apply(toIdx));
        } else {
            expected = new AbstractMap.SimpleEntry<>(position, getValue.apply(position));
        }

        Map.Entry<Integer, Long> actual = binarySearch(getValue, fromIdx, toIdx, target);

        assertEquals("Incorrect index for length " + length + " and position " + position,
                expected.getKey(), actual.getKey());
        assertEquals("Incorrect value for length " + length + " and position " + position,
                expected.getValue(), actual.getValue());
    }

    @Data
    private static class CountingFunction implements LongFunction<Long> {
        private int invocationCount;
        private final LongFunction<Long> call;

        @Override
        public Long apply(long value) {
            invocationCount++;
            return call.apply(value);
        }

        public void resetInvocationCount() {
            invocationCount = 0;
        }
    }

    @Test
    public void testNonUniformDistrobution() {
        CountingFunction getValue = new CountingFunction(i -> i * i);
        binarySearch(getValue, 0, 10_000, 1234);
        int baseline = getValue.getInvocationCount();
        getValue.resetInvocationCount();
        newtonianSearch(getValue, 0, 10_000, 1234, false);
        assertTrue("Implementation should be more efficent that binary search", getValue.invocationCount <= baseline);
    }

    @Test
    public void targetEfficency() {
        // Create a sorted array of random numbers
        int arraySize = 100000;
        long[] sortedArray = new long[arraySize];
        Random random = new Random(0);
        for (int i = 0; i < arraySize; i++) {
            sortedArray[i] = random.nextLong();
        }
        Arrays.sort(sortedArray);

        // Create an instance of the counting function
        CountingFunction countingFunction = new CountingFunction(countingValue -> sortedArray[(int) countingValue]);

        // Test the efficiency of the search

        long target = random.nextLong();
        countingFunction.resetInvocationCount();
        newtonianSearch(countingFunction, 0, arraySize - 1, target, true);
        long newtonianCount = countingFunction.invocationCount;
        countingFunction.resetInvocationCount();
        binarySearch(countingFunction, 0, arraySize - 1, target);
        long binaryCount = countingFunction.invocationCount;

        double ratio = (double) newtonianCount / (double) binaryCount;
        System.out.println("Cost: " + newtonianCount + " compared to binary search: " + binaryCount);
        assertTrue(ratio < 0.5);

        // Test the efficiency of the search
        int iterations = 100;
        long totalInvocationCount = 0;
        for (int i = 0; i < iterations; i++) {
            countingFunction.resetInvocationCount();
            target = random.nextLong();
            newtonianSearch(countingFunction, 0, arraySize - 1, target, true);
            System.out.println("Count: " + countingFunction.invocationCount);
            totalInvocationCount += countingFunction.invocationCount;
        }

        double averageInvocationCount = (double) totalInvocationCount / iterations;
        System.out.println("Average invocation count: " + averageInvocationCount);

    }

    @Test
    public void targetValueNotPresent() {
        LongFunction<Long> getValue = i -> (long) (i * 2); // Custom data structure where values are
        // twice the index
        int start = 0;
        int end = 9;
        long target = 7;

        Entry<Long, Long> result = newtonianSearch(getValue, start, end, target, false);

        Assert.assertNotNull(result);
        assertEquals(3, result.getKey().intValue()); // Expected index of the closest value
        assertEquals(6, result.getValue().longValue()); // Expected value at the closest index

        result = newtonianSearch(getValue, start, end, target, true);

        Assert.assertNotNull(result);
        assertEquals(4, result.getKey().intValue()); // Expected index of the closest value
        assertEquals(8, result.getValue().longValue()); // Expected value at the closest index
    }

    @Test
    public void exactValue() {
        LongFunction<Long> getValue = idx -> (long) idx;
        int fromIdx = 0;
        int toIdx = 9;
        long target = 7;

        Entry<Long, Long> expected = new AbstractMap.SimpleEntry<>(7L, 7L);
        Entry<Long, Long> actual = newtonianSearch(getValue, fromIdx, toIdx, target, false);

        assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        assertEquals("Incorrect value", expected.getValue(), actual.getValue());

        actual = newtonianSearch(getValue, fromIdx, toIdx, target, true);

        assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        assertEquals("Incorrect value", expected.getValue(), actual.getValue());
    }

    @Test
    public void testRoundDown() {
        LongFunction<Long> getValue = idx -> (long) (idx * 10);
        int fromIdx = 0;
        int toIdx = 9;
        long target = 75;

        Entry<Long, Long> expected = new AbstractMap.SimpleEntry<>(7L, 70L);
        Entry<Long, Long> actual = newtonianSearch(getValue, fromIdx, toIdx, target, false);

        assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        assertEquals("Incorrect value", expected.getValue(), actual.getValue());

        getValue = idx -> (long) (idx * 5);
        fromIdx = 0;
        toIdx = 5;
        target = 18;

        expected = new AbstractMap.SimpleEntry<>(3L, 15L);
        actual = newtonianSearch(getValue, fromIdx, toIdx, target, false);

        assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        assertEquals("Incorrect value", expected.getValue(), actual.getValue());
    }

    @Test
    public void testRoundUp() {
        LongFunction<Long> getValue = idx -> (long) (idx * 10);
        int fromIdx = 0;
        int toIdx = 9;
        long target = 75;

        Entry<Long, Long> expected = new AbstractMap.SimpleEntry<>(8L, 80L);
        Entry<Long, Long> actual = newtonianSearch(getValue, fromIdx, toIdx, target, true);

        Assert.assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        Assert.assertEquals("Incorrect value", expected.getValue(), actual.getValue());

        getValue = idx -> (long) (idx * 5);
        fromIdx = 0;
        toIdx = 5;
        target = 18;

        expected = new AbstractMap.SimpleEntry<>(4L, 20L);
        actual = newtonianSearch(getValue, fromIdx, toIdx, target, true);

        Assert.assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        Assert.assertEquals("Incorrect value", expected.getValue(), actual.getValue());
    }

    @Test
    public void emptyList() {
        LongFunction<Long> getValue = idx -> (long) idx;
        int fromIdx = 0;
        int toIdx = -1; // Empty list, invalid index range
        long target = 7;
        AssertExtensions.assertThrows(
                IllegalArgumentException.class, () -> newtonianSearch(getValue, fromIdx, toIdx, target, false));
    }

    @Test
    public void singleItem() {
        LongFunction<Long> getValue = idx -> (long) idx;
        int fromIdx = 0;
        int toIdx = 0;
        long target = 7;
        Entry<Long, Long> expected = new AbstractMap.SimpleEntry<>(0L, 0L);
        Entry<Long, Long> actual = newtonianSearch(getValue, fromIdx, toIdx, target, false);

        assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        assertEquals("Incorrect value", expected.getValue(), actual.getValue());
    }

    @Test
    public void targetBeforeRange() {
        LongFunction<Long> getValue = idx -> (long) idx;
        int fromIdx = 5;
        int toIdx = 10;
        long target = 3; // Target is before the range of values

        Entry<Long, Long> expected = new AbstractMap.SimpleEntry<>(5L, 5L);
        Entry<Long, Long> actual = newtonianSearch(getValue, fromIdx, toIdx, target, false);

        assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        assertEquals("Incorrect value", expected.getValue(), actual.getValue());
    }

    @Test
    public void targetAfterRange() {
        LongFunction<Long> getValue = idx -> (long) idx;
        int fromIdx = 0;
        int toIdx = 5;
        long target = 10; // Target is after the range of values

        Entry<Long, Long> expected = new AbstractMap.SimpleEntry<>(5L, 5L);
        Entry<Long, Long> actual = newtonianSearch(getValue, fromIdx, toIdx, target, true);

        assertEquals("Incorrect index", expected.getKey(), actual.getKey());
        assertEquals("Incorrect value", expected.getValue(), actual.getValue());
    }

    @Test
    public void testLargeRange() {
        // Create a large range of values from 1 to 1 million
        int rangeSize = 1_000;
        LongFunction<Long> getValue = idx -> idx * idx;

        // Test a target value within the range
        long target = 10_000;
        Entry<Long, Long> result = newtonianSearch(getValue, 0, rangeSize, target, false);
        assertEquals(100, result.getKey().intValue());
        assertEquals(10_000, result.getValue().longValue());

        // Test a target value within the range
        target = 10_001;
        result = newtonianSearch(getValue, 0, rangeSize, target, false);
        assertEquals(100, result.getKey().intValue());
        assertEquals(10_000, result.getValue().longValue());

        // Test a target value before the range
        target = 0;
        result = newtonianSearch(getValue, 0, rangeSize, target, false);
        assertEquals(0, result.getKey().intValue());
        assertEquals(0, result.getValue().longValue());

        target = 0;
        result = newtonianSearch(getValue, 10, rangeSize, target, false);
        assertEquals(10, result.getKey().intValue());
        assertEquals(100, result.getValue().longValue());

        // Test a target value after the range
        target = 1_000_000 + 100;
        result = newtonianSearch(getValue, 0, rangeSize, target, false);
        assertEquals(rangeSize, result.getKey().intValue());
        assertEquals((long) rangeSize * rangeSize, result.getValue().longValue());
    }

}