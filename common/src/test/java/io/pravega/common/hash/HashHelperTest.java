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
package io.pravega.common.hash;

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.val;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class HashHelperTest {

    @Test
    public void testLongBits() {
        assertEquals(1.0, HashHelper.longToDoubleFraction(-1), 0.0000001);
        assertEquals(0.0, HashHelper.longToDoubleFraction(0), 0.0000001);
        assertEquals(0.5, HashHelper.longToDoubleFraction(Long.reverse(1)), 0.0000001);
        assertEquals(0.25, HashHelper.longToDoubleFraction(Long.reverse(2)), 0.0000001);
        assertEquals(0.75, HashHelper.longToDoubleFraction(Long.reverse(3)), 0.0000001);
        assertEquals(0.125, HashHelper.longToDoubleFraction(Long.reverse(4)), 0.0000001);
        assertEquals(0.625, HashHelper.longToDoubleFraction(Long.reverse(5)), 0.0000001);
        assertEquals(0.375, HashHelper.longToDoubleFraction(Long.reverse(6)), 0.0000001);
        assertEquals(0.875, HashHelper.longToDoubleFraction(Long.reverse(7)), 0.0000001);
    }

    @Test
    public void testToUUID() {
        UUID uuid = HashHelper.seededWith("Test").toUUID("toUUID");
        assertNotEquals(0, uuid.getMostSignificantBits()); 
        assertNotEquals(0, uuid.getLeastSignificantBits());
        assertNotEquals(Long.MAX_VALUE, uuid.getMostSignificantBits()); 
        assertNotEquals(Long.MAX_VALUE, uuid.getLeastSignificantBits());
        assertNotEquals(-1, uuid.getMostSignificantBits()); 
        assertNotEquals(-1, uuid.getLeastSignificantBits());
        assertNotEquals(Long.MIN_VALUE, uuid.getMostSignificantBits()); 
        assertNotEquals(Long.MIN_VALUE, uuid.getLeastSignificantBits());
    }
    
    @Test
    public void testBytesToUUID() {
        byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        UUID uuid = HashHelper.bytesToUUID(bytes);
        assertEquals(uuid.getMostSignificantBits(), 0x0001020304050607L);
        assertEquals(uuid.getLeastSignificantBits(), 0x08090A0B0C0D0E0FL);
    }

    @Test
    public void testHashToBucketString() {
        testBucketUniformity(HashHelper::hashToBucket, () -> RandomStringUtils.randomAlphanumeric(16));
    }

    @Test
    public void testHashToBucketByteArray() {
        val rnd = new Random(0);
        testBucketUniformity(HashHelper::hashToBucket, () -> {
            byte[] r = new byte[16];
            rnd.nextBytes(r);
            return r;
        });
    }

    @Test
    public void testHashToBucketByteArrayView() {
        val rnd = new Random(0);
        testBucketUniformity(HashHelper::hashToBucket, () -> {
            byte[] r = new byte[16];
            rnd.nextBytes(r);
            return new ByteArraySegment(r);
        });
    }

    @Test
    public void testHashToBucketUUID() {
        val r = new Random(0);
        testBucketUniformity(HashHelper::hashToBucket, () -> new UUID(r.nextLong(), r.nextLong()));
    }

    @Test
    public void testHashToRangeStringUniformity() {
        val r = new Random(0);
        testHashToRangeUniformity(HashHelper::hashToRange,
                () -> {
                    byte[] array = new byte[16];
                    r.nextBytes(array);
                    return new String(array);
                });
    }

    @Test
    public void testHashToRangeByteBufUniformity() {
        val r = new Random(0);
        testHashToRangeUniformity(HashHelper::hashToRange,
                () -> {
                    byte[] array1 = new byte[8];
                    byte[] array2 = new byte[8];
                    r.nextBytes(array1);
                    r.nextBytes(array2);
                    return new ByteBuffer[]{ByteBuffer.wrap(array1), ByteBuffer.wrap(array2)};
                });
    }

    @Test
    public void testHashToRangeByteBuf() {
        val r = new Random(0);
        final int totalCount = 1000;
        val h = HashHelper.seededWith("Test");
        for (int i = 0; i < totalCount; i++) {
            byte[] array1 = new byte[r.nextInt(100)];
            byte[] array2 = new byte[r.nextInt(100)];
            r.nextBytes(array1);
            r.nextBytes(array2);
            byte[] array3 = new byte[array1.length + array2.length];
            System.arraycopy(array1, 0, array3, 0, array1.length);
            System.arraycopy(array2, 0, array3, array1.length, array2.length);

            double range1 = h.hashToRange(ByteBuffer.wrap(array1), ByteBuffer.wrap(array2));
            double range2 = h.hashToRange(ByteBuffer.wrap(array3));
            Assert.assertEquals(range2, range1, 0.01);
        }
    }
    
    @Test
    public void testMultipleParts() {
       val r = new Random(0);
       val bytes = new byte[1000];
       r.nextBytes(bytes);
       BufferView buffer = BufferView.wrap(bytes);
       BufferView part1 = buffer.slice(0, 10);
       BufferView part2 = buffer.slice(10, 100);
       BufferView part3 = buffer.slice(110, 500);
       BufferView part4 = buffer.slice(610, 380);
       BufferView part5 = buffer.slice(990, 10);
       List<BufferView> components = new ArrayList<>();
       components.add(part1);
       components.add(part2);
       components.add(part3);
       components.add(part4);
       components.add(part5);
       BufferView recombined = BufferView.wrap(components);
       assertEquals(buffer.hash(), recombined.hash());
       assertEquals(312908254654539963L, buffer.hash()); //Asserts that algorithm does not change.
    }
    
    @Test
    public void testSlicedInput() {
        val r = new Random(0);
        val bytes = new byte[100];
        r.nextBytes(bytes);
        for (int i = 0; i < bytes.length; i++) {
            BufferView buffer = BufferView.wrap(bytes);
            BufferView part1 = buffer.slice(0, i);
            BufferView part2 = buffer.slice(i, bytes.length - i);
            List<BufferView> components = new ArrayList<>();
            components.add(part1);
            components.add(part2);
            BufferView combined = BufferView.wrap(components);
            assertEquals(buffer.hash(), combined.hash());
        }
    }
    
    private <T> void testHashToRangeUniformity(HashToRangeFunction<T> toTest, Supplier<T> generator) {
        testBucketUniformity(
                (hh, value, bucketCount) -> {
                    double d = toTest.apply(hh, value);
                    return (int) Math.floor(d * bucketCount);
                },
                generator);
    }

    private <T> void testBucketUniformity(HashBucketFunction<T> toTest, Supplier<T> generator) {
        final int elementsPerBucket = 100000;
        final int acceptedDeviation = (int) (0.05 * elementsPerBucket);
        final int bucketCount = 16;
        final int totalCount = bucketCount * elementsPerBucket;
        val h = HashHelper.seededWith("Test");
        val result = new HashMap<Integer, Integer>();
        for (int i = 0; i < totalCount; i++) {
            val u = generator.get();
            int b = toTest.apply(h, u, bucketCount);
            result.put(b, result.getOrDefault(b, 0) + 1);
        }

        result.forEach((bucket, count) ->
                AssertExtensions.assertLessThan("Too many or too few elements in bucket " + bucket,
                        acceptedDeviation, Math.abs(count - elementsPerBucket)));
    }

    @FunctionalInterface
    interface HashBucketFunction<T> {
        int apply(HashHelper hashHelper, T value, int bucketCount);
    }

    @FunctionalInterface
    interface HashToRangeFunction<T> extends BiFunction<HashHelper, T, Double> {
    }
}
