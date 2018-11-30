/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
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
     * Tests comparing ByteArraySegments.
     */
    @Test
    public void testCompareByteArraySegment() {
        val sortedData = generateSortedData().stream().map(ByteArraySegment::new).collect(Collectors.toList());
        test(sortedData, new ByteArrayComparator()::compare);
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
