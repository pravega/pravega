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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BufferViewComparator} class.
 */
public abstract class BufferViewComparatorTests {
    private static final int COUNT = 2000;

    abstract BufferViewComparator getComparator();

    //region Tests

    /**
     * Tests comparing raw byte arrays.
     */
    @Test
    public void testCompareByteArray() {
        val sortedData = generateSortedData().stream().map(ArrayView::array).collect(Collectors.toList());
        test(sortedData, getComparator()::compare);
    }

    /**
     * Tests comparing equal-length {@link ArrayView} instances.
     */
    @Test
    public void testCompareArrayView() {
        val sortedData = generateSortedData();
        test(sortedData, getComparator()::compare);
    }

    /**
     * Tests comparing different-length {@link ArrayView} instances.
     */
    @Test
    public void testCompareArrayViewVariableSize() {
        testCompareBufferView(a -> a, getComparator()::compare);
    }

    /**
     * Tests comparing different-length {@link BufferView} instances.
     */
    @Test
    public void testCompareBufferViewVariableSize() {
        testCompareBufferView(a -> {
                    val builder = BufferView.builder();
                    for (int i = 0; i < a.getLength(); i++) {
                        builder.add(a.slice(i, 1));
                    }
                    return builder.build();
                },
                getComparator()::compare);
    }

    private <T extends BufferView> void testCompareBufferView(Function<ArrayView, T> toBufferView, BiFunction<T, T, Integer> comparator) {
        val sortedData = generateSortedVariableData().stream()
                .map(ByteArraySegment::new)
                .map(toBufferView)
                .collect(Collectors.toList());
        sortedData.add(0, toBufferView.apply(new ByteArraySegment(new byte[0]))); // Empty.
        test(sortedData, comparator);
        for (val s : sortedData) {
            int compareResult = comparator.apply(toBufferView.apply(new ByteArraySegment(BufferViewComparator.getMinValue())), s);
            if (compareResult == 0) {
                // Only equal to itself.
                Assert.assertTrue(s.getLength() == 1 && s.getBufferViewReader().readByte() == BufferViewComparator.MIN_VALUE);
            } else if (compareResult > 0) {
                // Only empty array is smaller than it.
                Assert.assertEquals(0, s.getLength());
            }
        }
    }

    /**
     * Tests the {@link  BufferViewComparator#getNextItemOfSameLength}.
     */
    @Test
    public void testGetNextItemOfSameLength() {
        val c = getComparator();
        val max = BufferViewComparator.MAX_VALUE;
        val almostMax = (byte) (max - 1);
        List<Map.Entry<byte[], byte[]>> tests = Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{}, null),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{almostMax}, new byte[]{max}),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{max}, null),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{0, 1, 2, 3, 4}, new byte[]{0, 1, 2, 3, 5}),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{max, almostMax}, new byte[]{max, max}),
                new AbstractMap.SimpleImmutableEntry<>(new byte[]{max, max}, null));

        for (val e : tests) {
            val actual = BufferViewComparator.getNextItemOfSameLength(new ByteArraySegment(e.getKey()));
            if (e.getValue() == null) {
                Assert.assertNull(actual);
            } else {
                val expected = new ByteArraySegment(e.getValue());
                Assert.assertEquals(0, c.compare(expected, actual));
            }
        }
    }

    /**
     * Tests static methods {@link BufferViewComparator#getMaxValue(int)} and {@link BufferViewComparator#getMinValue(int)}.
     */
    @Test
    public void testGetMinMaxValue() {
        for (int i = 0; i < 16; i++) {
            val expectedMin = new byte[i];
            val expectedMax = new byte[i];
            for (int j = 0; j < i; j++) {
                expectedMin[j] = BufferViewComparator.MIN_VALUE;
                expectedMax[j] = BufferViewComparator.MAX_VALUE;
            }

            Assert.assertArrayEquals(expectedMin, BufferViewComparator.getMinValue(i));
            Assert.assertArrayEquals(expectedMax, BufferViewComparator.getMaxValue(i));
        }
    }

    private ArrayList<ByteArraySegment> generateSortedData() {
        val sortedData = new ArrayList<ByteArraySegment>();
        int maxValue = COUNT / 2;
        for (int i = -maxValue; i < maxValue; i++) {
            val data = new ByteArraySegment(new byte[Long.BYTES]);
            data.setUnsignedLong(0, i);
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
                if (expectedResult != actualResult) {
                    actualResult = (int) Math.signum(comparator.apply(sortedData.get(i), sortedData.get(j)));
                }
                Assert.assertEquals("Unexpected comparison value for " + i + " <-> " + j, expectedResult, actualResult);
            }
        }
    }

    //endregion

    /**
     * Unit tests for the {@link BufferViewComparator.LegacyComparator} class.
     */
    public static class Legacy extends BufferViewComparatorTests {
        @Override
        BufferViewComparator getComparator() {
            return new BufferViewComparator.LegacyComparator();
        }
    }

    /**
     * Unit tests for the {@link BufferViewComparator.IntrinsicComparator} class.
     */
    public static class Intrinsic extends BufferViewComparatorTests {
        @Override
        BufferViewComparator getComparator() {
            return new BufferViewComparator.IntrinsicComparator();
        }
    }
}
