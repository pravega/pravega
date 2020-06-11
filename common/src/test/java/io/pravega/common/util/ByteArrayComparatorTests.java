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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ByteArrayComparator class.
 */
public class ByteArrayComparatorTests {
    private static final int COUNT = 2000;

    /**
     * Tests comparing raw byte arrays.
     */
    @Test
    public void testCompareByteArray() {
        val sortedData = generateSortedData();
        test(sortedData, new ByteArrayComparator()::compare);
    }

    /**
     * Tests comparing equal-length {@link ArrayView} instances.
     */
    @Test
    public void testCompareArrayView() {
        val sortedData = generateSortedData().stream().map(ByteArraySegment::new).collect(Collectors.toList());
        test(sortedData, new ByteArrayComparator()::compare);
    }

    /**
     * Tests comparing different-length {@link ArrayView} instances
     */
    @Test
    public void testCompareArrayViewVariableSize() {
        val c = new ByteArrayComparator();
        val sortedData = generateSortedVariableData().stream().map(ByteArraySegment::new).collect(Collectors.toList());
        sortedData.add(0, new ByteArraySegment(new byte[0])); // Empty.
        test(sortedData, c::compare);
        for (val s : sortedData) {
            int compareResult = c.compare(new ByteArraySegment(c.getMinValue()), s);
            if (compareResult == 0) {
                // Only equal to itself.
                Assert.assertTrue(s.getLength() == 1 && s.get(0) == ByteArrayComparator.MIN_VALUE);
            } else if (compareResult > 0) {
                // Only empty array is smaller than it.
                Assert.assertEquals(0, s.getLength());
            }
        }
    }

    /**
     * Tests the {@link  ByteArrayComparator#getNextItemOfSameLength}.
     */
    @Test
    public void testGetNextItemOfSameLength() {
        val c = new ByteArrayComparator();
        val max = ByteArrayComparator.MAX_VALUE;
        val almostMax = (byte) (max - 1);
        List<Map.Entry<byte[], byte[]>> tests = Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{}, null),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{almostMax}, new byte[]{max}),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{max}, null),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{0, 1, 2, 3, 4}, new byte[]{0, 1, 2, 3, 5}),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{max, almostMax}, new byte[]{max, max}),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{max, max}, null));

        for (val e : tests) {
            val actual = ByteArrayComparator.getNextItemOfSameLength(new ByteArraySegment(e.getKey()));
            if (e.getValue() == null) {
                Assert.assertNull(actual);
            } else {
                val expected = new ByteArraySegment(e.getValue());
                Assert.assertEquals(0, c.compare(expected, actual));
            }
        }
    }

    private ArrayList<byte[]> generateSortedData() {
        val sortedData = new ArrayList<byte[]>();
        int maxValue = COUNT / 2;
        for (int i = -maxValue; i < maxValue; i++) {
            byte[] data = new byte[Long.BYTES];
            BitConverter.writeUnsignedLong(data, 0, i);
            sortedData.add(data);
        }
        return sortedData;
    }

    private List<byte[]> generateSortedVariableData() {
        // We use String sort to mimic our comparison, then parse the string putting each digit into its own byte array index.
        return LongStream.range(0, COUNT)
                .mapToObj(Long::toString)
                .sorted()
                .map(this::parseDigits)
                .collect(Collectors.toList());
    }

    private byte[] parseDigits(String digits) {
        byte[] result = new byte[digits.length()];
        for (int i = 0; i < digits.length(); i++) {
            result[i] = Byte.parseByte(digits.substring(i, i + 1));
        }
        return result;
    }

    private <T> void test(List<T> sortedData, BiFunction<T, T, Integer> comparator) {
        for (int i = 0; i < sortedData.size(); i++) {
            for (int j = 0; j < sortedData.size(); j++) {
                int expectedResult = (int) Math.signum(Integer.compare(i, j));
                int actualResult = (int) Math.signum(comparator.apply(sortedData.get(i), sortedData.get(j)));
                Assert.assertEquals("Unexpected comparison value.", expectedResult, actualResult);
            }
        }
    }
}
