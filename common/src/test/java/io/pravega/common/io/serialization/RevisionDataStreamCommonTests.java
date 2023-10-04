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
package io.pravega.common.io.serialization;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for data encoding-decoding using RevisionDataOutputStream and RevisionDataInputStream.
 * Please refer to RevisionDataInputStreamTests and RevisionDataOutputStreamTests for tests relating to functionality that
 * is specific to those classes.
 */
public class RevisionDataStreamCommonTests {
    private static final int CHAR_SKIP = 79;

    /**
     * Tests the getUTFLength() method with various inputs.
     */
    @Test
    public void testGetUTFLength() throws Exception {
        val pairs = ImmutableMap.of(0, 200, 1, 4000, 2048, 10000);
        for (val e : pairs.entrySet()) {
            val string = getUTFString(e.getKey(), e.getValue());
            testLength(RevisionDataOutputStream::writeUTF, RevisionDataOutputStream::getUTFLength, string);
        }
    }

    /**
     * Tests the getCollectionLength() method with constant input
     */
    @Test
    public void testGetCollectionLengthConstant() throws Exception {
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(new ByteArrayOutputStream());
        Assert.assertEquals("Unexpected length for empty collection.",
                rdos.getCompactIntLength(0), rdos.getCollectionLength(0, 123));
        Assert.assertEquals("Unexpected length for non-empty collection with zero-length element.",
                rdos.getCompactIntLength(123), rdos.getCollectionLength(123, 0));
        Assert.assertEquals("Unexpected length for non-empty collection.",
                rdos.getCompactIntLength(123) + 123 * 8, rdos.getCollectionLength(123, 8));
    }

    /**
     * Tests the getCollectionLength() method with variable input
     */
    @Test
    public void testGetCollectionLengthStatic() throws Exception {
        val data = Arrays.asList(1, 2, 3, 4, 5);
        val sum = data.stream().mapToInt(i -> i).sum();
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(new ByteArrayOutputStream());
        Assert.assertEquals("Unexpected length.",
                rdos.getCompactIntLength(data.size()) + sum, rdos.getCollectionLength(data, i -> i));
    }

    /**
     * Tests the getMapLength() method with constant input.
     */
    @Test
    public void testGetMapLength() throws Exception {
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(new ByteArrayOutputStream());
        Assert.assertEquals("Unexpected length for empty map.",
                rdos.getCompactIntLength(0), rdos.getMapLength(0, 123, 123));
        Assert.assertEquals("Unexpected length for non-empty map with zero-length key.",
                rdos.getCompactIntLength(123) + 123 * 17, rdos.getMapLength(123, 0, 17));
        Assert.assertEquals("Unexpected length for non-empty map with zero-length value.",
                rdos.getCompactIntLength(123) + 123 * 8, rdos.getMapLength(123, 8, 0));
        Assert.assertEquals("Unexpected length for non-empty map with zero-length key and value.",
                rdos.getCompactIntLength(123), rdos.getMapLength(123, 0, 0));
        Assert.assertEquals("Unexpected length for non-empty map.",
                rdos.getCompactIntLength(123) + 123 * (8 + 17), rdos.getMapLength(123, 8, 17));
    }

    /**
     * Tests the getMapLength() method with variable input.
     */
    @Test
    public void testGetMapLengthStatic() throws Exception {
        val data = ImmutableMap.of(1, 10, 2, 20, 3, 30, 4, 40, 5, 50);
        val sum = data.entrySet().stream().mapToInt(e -> e.getKey() + e.getValue()).sum();
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(new ByteArrayOutputStream());
        Assert.assertEquals("Unexpected length.",
                rdos.getCompactIntLength(data.size()) + sum, rdos.getMapLength(data, i -> i, i -> i));
    }

    /**
     * Tests the getCompactLongLength() method.
     */
    @Test
    public void testGetCompactLongLength() throws Exception {
        val expectedValues = ImmutableMap.<Long, Integer>builder()
                .put(RevisionDataOutput.COMPACT_LONG_MIN - 1, -1)
                .put(RevisionDataOutput.COMPACT_LONG_MAX + 1, -1)
                .put(0L, 1).put(0x3FL, 1)
                .put(0x3FL + 1, 2).put(0x3FFFL, 2)
                .put(0x3FFFL + 1, 4).put(0x3FFF_FFFFL, 4)
                .put(0x3FFF_FFFFL + 1, 8).build();
        testGetCompactLength(expectedValues, RevisionDataOutputStream::getCompactLongLength, RevisionDataOutputStream::writeCompactLong);
    }

    /**
     * Tests the getCompactSignedLongLength() method.
     */
    @Test
    public void testGetCompactSignedLongLength() throws Exception {
        val expectedValues = ImmutableMap.<Long, Integer>builder()
                .put(RevisionDataOutput.COMPACT_SIGNED_LONG_MIN - 1, -1)
                .put(RevisionDataOutput.COMPACT_SIGNED_LONG_MAX + 1, -1)
                .put(-0x1FL - 1, 1).put(0x1FL, 1)
                .put(-0x1FL - 2, 2).put(0x1FFFL, 2)
                .put(-0x1FFFL - 2, 4).put(0x1FFF_FFFFL, 4)
                .put(-0x1FFF_FFFFL - 2, 8).put(0x1FFF_FFFFL + 1, 8)
                .put(RevisionDataOutput.COMPACT_SIGNED_LONG_MIN, 8)
                .put(RevisionDataOutput.COMPACT_SIGNED_LONG_MAX, 8)
                .put(0L, 1).put(-1L, 1)
                .build();
        testGetCompactLength(expectedValues, RevisionDataOutputStream::getCompactSignedLongLength, RevisionDataOutputStream::writeCompactSignedLong);
    }

    /**
     * Tests the getCompactIntLength() method.
     */
    @Test
    public void testGetCompactIntLength() throws Exception {
        val expectedValues = ImmutableMap.<Integer, Integer>builder()
                .put(RevisionDataOutput.COMPACT_INT_MIN - 1, -1)
                .put(RevisionDataOutput.COMPACT_INT_MAX + 1, -1)
                .put(0, 1).put(0x7F, 1)
                .put(0x7F + 1, 2).put(0x3FFF, 2)
                .put(0x3FFF + 1, 4).build();
        testGetCompactLength(expectedValues, RevisionDataOutputStream::getCompactIntLength, RevisionDataOutputStream::writeCompactInt);
    }

    /**
     * Tests the ability to encode and decode a Compact Long.
     */
    @Test
    public void testCompactLong() throws Exception {
        val toTest = new ArrayList<Long>();
        // Boundary tests.
        toTest.addAll(Arrays.asList(RevisionDataOutput.COMPACT_LONG_MIN, RevisionDataOutput.COMPACT_LONG_MAX,
                0x3FL, 0x3FL + 1, 0x3FFFL, 0x3FFFL + 1, 0x3FFF_FFFFL, 0x3FFF_FFFFL + 1));

        // We want to test that when we split up the Long into smaller numbers, we won't be tripping over unsigned bytes/shorts/ints.
        toTest.addAll(getAllOneBitNumbers(Long.SIZE - 2));

        val shouldFail = Arrays.asList(RevisionDataOutput.COMPACT_LONG_MIN - 1, RevisionDataOutput.COMPACT_LONG_MAX + 1);
        testCompact(RevisionDataOutputStream::writeCompactLong, RevisionDataInputStream::readCompactLong,
                RevisionDataOutputStream::getCompactLongLength, toTest, shouldFail, Long::equals);
    }

    /**
     * Tests the ability to encode and decode a Compact Signed Long.
     */
    @Test
    public void testCompactSignedLong() throws Exception {
        val toTest = new ArrayList<Long>();

        // Boundary tests.
        toTest.addAll(Arrays.asList(-0x1FL - 1, 0x1FL, -0x1FL - 2, 0x1FFFL, -0x1FFFL - 2, 0x1FFF_FFFFL, -0x1FFF_FFFFL - 2, 0x1FFF_FFFFL + 1, 0L, -1L));

        // We want to test that when we split up the Long into smaller numbers, we won't be tripping over unsigned bytes/shorts/ints.
        toTest.addAll(getAllOneBitNumbers(Long.SIZE - 3));

        // Add the negatives of all the numbers so far.
        toTest.addAll(toTest.stream().mapToLong(l -> -l).boxed().collect(Collectors.toList()));

        // Add extremes.
        toTest.addAll(Arrays.asList(RevisionDataOutput.COMPACT_SIGNED_LONG_MIN, RevisionDataOutput.COMPACT_SIGNED_LONG_MAX));

        val shouldFail = Arrays.asList(RevisionDataOutput.COMPACT_SIGNED_LONG_MIN - 1, RevisionDataOutput.COMPACT_SIGNED_LONG_MAX + 1);
        testCompact(RevisionDataOutputStream::writeCompactSignedLong, RevisionDataInputStream::readCompactSignedLong,
                RevisionDataOutputStream::getCompactSignedLongLength, toTest, shouldFail, Long::equals);
    }

    /**
     * Tests the ability to encode and decode a Compact Int.
     */
    @Test
    public void testCompactInt() throws Exception {
        val toTest = new ArrayList<Integer>();
        // Boundary tests.
        toTest.addAll(Arrays.asList(RevisionDataOutput.COMPACT_INT_MIN, RevisionDataOutput.COMPACT_INT_MAX,
                0x7F, 0x7F + 1, 0x3FFF, 0x3FFF + 1, 0x3F_FFFF, 0x3F_FFFF + 1));

        // We want to test that when we split up the Long into smaller numbers, we won't be tripping over unsigned bytes/shorts/ints.
        getAllOneBitNumbers(Integer.SIZE - 2).forEach(n -> toTest.add((int) (long) n));

        val shouldFail = Arrays.asList(RevisionDataOutput.COMPACT_INT_MIN - 1, RevisionDataOutput.COMPACT_INT_MAX + 1);
        testCompact(RevisionDataOutputStream::writeCompactInt, RevisionDataInputStream::readCompactInt, RevisionDataOutputStream::getCompactIntLength,
                toTest, shouldFail, Integer::equals);
    }

    /**
     * Tests the ability to encode and decode a UUID.
     */
    @Test
    public void testUUID() throws Exception {
        val toTest = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());
        for (val value : toTest) {
            testEncodeDecode(RevisionDataOutputStream::writeUUID, RevisionDataInputStream::readUUID, (s, v) -> RevisionDataOutput.UUID_BYTES, value, UUID::equals);
        }
    }

    /**
     * Tests the ability to encode and decode a generic array.
     */
    @Test
    public void testGenericArrays() throws Exception {
        val numbers = getAllOneBitNumbers(Long.SIZE);
        val toTest = Arrays.<Long[]>asList(
                null,
                new Long[0],
                numbers.toArray(new Long[numbers.size()]));
        for (Long[] value : toTest) {
            testEncodeDecode(
                    (os, v) -> os.writeArray(v, RevisionDataOutput::writeLong),
                    is -> is.readArray(DataInput::readLong, Long[]::new),
                    (s, v) -> s.getCollectionLength(v, e -> Long.BYTES),
                    value,
                    (s, t) -> Arrays.equals(s == null ? new Long[0] : s, t));
        }
    }

    /**
     * Tests the ability to encode and decode {@link BufferView}s.
     */
    @Test
    public void testByteArrays() throws Exception {
        byte[] numbers = new byte[Byte.MAX_VALUE];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = (byte) (i % Byte.MAX_VALUE);
        }

        val toTest = Arrays.<byte[]>asList(
                null,
                new byte[0],
                numbers);
        for (byte[] value : toTest) {
            // Raw byte arrays.
            testEncodeDecode(
                    RevisionDataOutput::writeArray,
                    RevisionDataInput::readArray,
                    (s, v) -> s.getCollectionLength(v == null ? 0 : v.length, 1),
                    value,
                    (s, t) -> Arrays.equals(s == null ? new byte[0] : s, t));

            // Buffer Views.
            testEncodeDecode(
                    (RevisionDataOutputStream s, byte[] t) -> s.writeBuffer(t == null ? null : new ByteArraySegment(t)),
                    RevisionDataInput::readArray,
                    (s, v) -> s.getCollectionLength(v == null ? 0 : v.length, 1),
                    value,
                    (s, t) -> Arrays.equals(s == null ? new byte[0] : s, t));
        }
    }

    /**
     * Tests the ability to encode and decode a Collection.
     */
    @Test
    public void testCollections() throws Exception {
        val toTest = Arrays.<Collection<Long>>asList(
                null,
                Collections.emptyList(),
                getAllOneBitNumbers(Long.SIZE));
        for (val value : toTest) {
            testEncodeDecode(
                    (os, v) -> os.writeCollection(v, RevisionDataOutput::writeLong),
                    is -> is.readCollection(DataInput::readLong),
                    (s, v) -> s.getCollectionLength(v, e -> Long.BYTES),
                    value,
                    this::collectionsEqual);
        }
    }

    /**
     * Tests the ability to encode and decode a Map.
     */
    @Test
    public void testMaps() throws Exception {
        val toTest = Arrays.<Map<Long, String>>asList(
                null,
                Collections.emptyMap(),
                getAllOneBitNumbers(Long.SIZE).stream().collect(Collectors.toMap(i -> i, Object::toString)));
        for (val value : toTest) {
            testEncodeDecode(
                    (os, v) -> os.writeMap(v, RevisionDataOutput::writeLong, RevisionDataOutput::writeUTF),
                    is -> is.readMap(DataInput::readLong, DataInput::readUTF),
                    (s, v) -> s.getMapLength(v, e -> Long.BYTES, s::getUTFLength),
                    value,
                    this::mapsEqual);
        }
    }

    /**
     * Tests {@link RevisionDataInput#getRemaining()}.
     */
    @Test
    public void testGetRemaining() throws Exception {
        @Cleanup
        val os = new ByteBufferOutputStream();
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(os);
        rdos.writeInt(1);
        rdos.writeLong(2L);
        rdos.writeBuffer(new ByteArraySegment(new byte[3]));
        rdos.flush();
        rdos.close();
        int expectedRemaining = os.getData().getLength() - Integer.BYTES; // BoundedInputStream header.

        // Use a SequenceInputStream - this will always have available() set to 0.
        @Cleanup
        val rdis = RevisionDataInputStream.wrap(os.getData().getReader());
        Assert.assertEquals(expectedRemaining, rdis.getRemaining());

        Assert.assertEquals(1, rdis.readInt());
        expectedRemaining -= Integer.BYTES;
        Assert.assertEquals(expectedRemaining, rdis.getRemaining());

        Assert.assertEquals(2L, rdis.readLong());
        expectedRemaining -= Long.BYTES;
        Assert.assertEquals(expectedRemaining, rdis.getRemaining());

        Assert.assertEquals(3, rdis.readArray().length);
        expectedRemaining = 0;
        Assert.assertEquals(expectedRemaining, rdis.getRemaining());
    }

    private <T> void testGetCompactLength(Map<T, Integer> expectedValues,
                                          BiFunction<RevisionDataOutputStream, T, Integer> getLength,
                                          BiConsumerWithException<RevisionDataOutputStream, T> writeNumber) throws Exception {
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(new ByteArrayOutputStream());
        for (val e : expectedValues.entrySet()) {
            if (e.getValue() < 0) {
                AssertExtensions.assertThrows(
                        "getCompactIntLength accepted invalid input: " + e.getKey(),
                        () -> getLength.apply(rdos, e.getKey()),
                        ex -> ex instanceof IllegalArgumentException);
            } else {
                // Verify what it should be.
                int actualValue = getLength.apply(rdos, e.getKey());
                Assert.assertEquals("Unexpected result for " + e.getKey(), (int) e.getValue(), actualValue);

                // Verify that it is the case in practice.
                testLength(writeNumber, getLength, e.getKey());
            }
        }
    }

    private <T> void testLength(BiConsumerWithException<RevisionDataOutputStream, T> write,
                                BiFunction<RevisionDataOutputStream, T, Integer> getLength, T value) throws Exception {
        @Cleanup
        val os = new ByteBufferOutputStream();
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(os);
        val initialLength = os.getData().getLength();
        write.accept(rdos, value);
        rdos.flush();
        val expectedValue = os.getData().getLength() - initialLength;
        val actualValue = getLength.apply(rdos, value);
        Assert.assertEquals(String.format("Unexpected length for '%s'.", value), expectedValue, (int) actualValue);
    }

    private <T> void testCompact(BiConsumerWithException<RevisionDataOutputStream, T> write, FunctionWithException<RevisionDataInputStream, T> read,
                                 BiFunction<RevisionDataOutputStream, T, Integer> getLength, Collection<T> toTest, Collection<T> invalid,
                                 BiPredicate<T, T> areEqual) throws Exception {
        for (val value : toTest) {
            testEncodeDecode(write, read, getLength, value, areEqual);
        }

        @Cleanup
        val rdos2 = RevisionDataOutputStream.wrap(new ByteArrayOutputStream());
        for (T value : invalid) {
            AssertExtensions.assertThrows(
                    "Encoding accepted invalid value: " + value,
                    () -> write.accept(rdos2, value),
                    ex -> ex instanceof IllegalArgumentException);
        }
    }

    private <T> void testEncodeDecode(BiConsumerWithException<RevisionDataOutputStream, T> write, FunctionWithException<RevisionDataInputStream, T> read,
                                      BiFunction<RevisionDataOutputStream, T, Integer> getLength, T value, BiPredicate<T, T> equalityTester) throws Exception {
        @Cleanup
        val os = new ByteBufferOutputStream();
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(os);
        write.accept(rdos, value);
        rdos.close();
        os.close();

        val actualLength = os.size() - Integer.BYTES; // Subtract 4 because this is the Length being encoded.
        Assert.assertEquals("Unexpected length for value " + value, (int) getLength.apply(rdos, value), actualLength);

        @Cleanup
        val rdis = RevisionDataInputStream.wrap(os.getData().getReader());
        val actualValue = read.apply(rdis);
        Assert.assertTrue(String.format("Encoding/decoding failed for %s (decoded %s).", value, actualValue), equalityTester.test(value, actualValue));
    }

    private String getUTFString(int minChar, int maxChar) {
        StringBuilder sb = new StringBuilder();
        for (int i = minChar; i < maxChar; i += CHAR_SKIP) {
            sb.append((char) i);
        }
        return sb.toString();
    }

    private <T> boolean collectionsEqual(Collection<T> c1, Collection<T> c2) {
        int s1 = c1 == null ? 0 : c1.size();
        int s2 = c2 == null ? 0 : c2.size();
        if (s1 != s2) {
            return false;
        } else if (s1 == 0) {
            // Both empty collections; they could be null so don't proceed.
            return true;
        }

        val h2 = new HashSet<T>(c2);
        for (val e1 : c1) {
            if (!h2.contains(e1)) {
                return false;
            }
        }

        return true;
    }

    private <K, V> boolean mapsEqual(Map<K, V> m1, Map<K, V> m2) {
        int s1 = m1 == null ? 0 : m1.size();
        int s2 = m2 == null ? 0 : m2.size();
        if (s1 != s2) {
            return false;
        } else if (s1 == 0) {
            // Both empty collections; they could be null so don't proceed.
            return true;
        }

        for (val e1 : m1.entrySet()) {
            if (!m2.containsKey(e1.getKey()) || !m2.get(e1.getKey()).equals(e1.getValue())) {
                return false;
            }
        }

        return true;
    }

    private ArrayList<Long> getAllOneBitNumbers(int bitCount) {
        val result = new ArrayList<Long>();
        result.add(0L);
        long value = 1;
        for (int i = 0; i < bitCount; i++) {
            result.add(value);
            value = value << 1;
        }

        return result;
    }

    @FunctionalInterface
    public interface BiConsumerWithException<T, U> {
        void accept(T var1, U var2) throws Exception;
    }

    @FunctionalInterface
    public interface FunctionWithException<T, R> {
        R apply(T var1) throws Exception;
    }
}
