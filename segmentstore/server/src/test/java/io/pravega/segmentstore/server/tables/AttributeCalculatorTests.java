/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.test.common.AssertExtensions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the AttributeCalculator class.
 */
public class AttributeCalculatorTests {
    /**
     * Tests the getPrimaryHashAttributeKey() method.
     */
    @Test
    public void testPrimaryHashAttributeKey() {
        int count = 100000;
        val r = new Random(0);
        val ac = new AttributeCalculator();
        val hashBuffer = new byte[AttributeCalculator.PRIMARY_HASH_LENGTH];
        val shiftBits = Long.SIZE - 1;
        for (int i = 0; i < count; i++) {
            if (i == 0) {
                // We need to verify Core Attribute collision resolution.
                Arrays.fill(hashBuffer, (byte) 0);
            } else {
                r.nextBytes(hashBuffer);
            }

            UUID result = ac.getPrimaryHashAttributeKey(new ByteArraySegment(hashBuffer));
            Assert.assertEquals("Unexpected first bit.", 1, result.getMostSignificantBits() >>> shiftBits);
            Assert.assertFalse("Not expecting Core Attribute collisions.", Attributes.isCoreAttribute(result));

            // MSB is the first 8 bytes in the hash, with the first bit replaced by 1.
            long expectedMsb = BitConverter.readLong(hashBuffer, 0) & AttributeCalculator.ONE_BIT_MASK | (1L << shiftBits);
            if (expectedMsb == Long.MIN_VALUE) {
                // Core Attribute collision.
                expectedMsb++;
            }

            // LSB is the remaining 8 bytes in the hash.
            long expectedLsb = BitConverter.readLong(hashBuffer, Long.BYTES);
            Assert.assertEquals("Unexpected MSB.", expectedMsb, result.getMostSignificantBits());
            Assert.assertEquals("Unexpected LSB.", expectedLsb, result.getLeastSignificantBits());

            // Test the reverse: UUID -> buffer.
            val primaryHash = ac.getPrimaryHash(result);
            val key2 = ac.getPrimaryHashAttributeKey(primaryHash);
            Assert.assertEquals("Unexpected result after deconstructing and reconstructing Primary Hash", result, key2);
            val primaryHash2 = ac.getPrimaryHash(result);
            AssertExtensions.assertArrayEquals("Unexpected hash result after Primary Hash reconstruction.",
                    primaryHash.array(), primaryHash.arrayOffset(), primaryHash2.array(), primaryHash2.arrayOffset(), primaryHash.getLength());
        }

        // Verify we cannot accept bad arguments.
        AssertExtensions.assertThrows(
                "getPrimaryHashAttributeKey accepted an input of the wrong length.",
                () -> ac.getPrimaryHashAttributeKey(new ByteArraySegment(hashBuffer, 0, AttributeCalculator.PRIMARY_HASH_LENGTH - 1)),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Test the {@link AttributeCalculator#getNextPrimaryHashAttributeKey} method.
     */
    @Test
    public void testGetNextPrimaryHashAttributeKey() {
        val testData = ImmutableMap.<UUID, UUID>builder()
                // LSB is not MAX, so that is all that needs to be incremented.
                .put(new UUID(toPH(1L), 0L), new UUID(toPH(1L), 1L))
                // LSB is not MAX.
                .put(new UUID(toPH(-1L), -1L), new UUID(toPH(-1L), 0L))
                // MSB is MAX, so all that needs changing is LSB.
                .put(new UUID(toPH(Long.MAX_VALUE), Long.MIN_VALUE), new UUID(toPH(Long.MAX_VALUE), Long.MIN_VALUE + 1))
                .build();
        val noNextValue = Collections.singletonList(new UUID(toPH(Long.MAX_VALUE), Long.MAX_VALUE));
        val invalidValues = Arrays.asList(new UUID(Long.MIN_VALUE, Long.MIN_VALUE), new UUID(0L, 0L));

        val ac = new AttributeCalculator();
        for (val id : noNextValue) {
            val actual = ac.getNextPrimaryHashAttributeKey(id);
            Assert.assertNull("Not expecting a next value for " + id, actual);
        }

        for (val e : testData.entrySet()) {
            val expected = e.getValue();
            val actual = ac.getNextPrimaryHashAttributeKey(e.getKey());
            Assert.assertEquals("Unexpected next value for " + e.getKey(), expected, actual);
        }

        for (val e : invalidValues) {
            AssertExtensions.assertThrows(
                    "Invalid input was accepted.",
                    () -> ac.getNextPrimaryHashAttributeKey(e),
                    ex -> ex instanceof IllegalArgumentException);
        }
    }

    private long toPH(long v) {
        return v | AttributeCalculator.PRIMARY_HASH_SET;
    }

    /**
     * Tests the getSecondaryHashAttributeKey() method.
     */
    @Test
    public void testSecondaryHashAttributeKey() {
        val nodeIds = IntStream.concat(IntStream.of(1, 100000), IntStream.of(0x3FFF_FFFF));
        val r = new Random(0);
        val ac = new AttributeCalculator();
        val hashBuffer = new byte[AttributeCalculator.SECONDARY_HASH_LENGTH];
        val shiftBits = Long.SIZE - 2;
        nodeIds.forEach(nodeId -> {
            r.nextBytes(hashBuffer);
            UUID result = ac.getSecondaryHashAttributeKey(new ByteArraySegment(hashBuffer), nodeId);
            Assert.assertEquals("Unexpected first 2 bits.", 1, result.getMostSignificantBits() >>> shiftBits);
            // MSB is a concatenation of nodeId and first 4 bytes of hash, all prefixed with 01.
            long expectedMsb = BitConverter.readInt(hashBuffer, 0) + ((long) nodeId << Integer.SIZE);
            expectedMsb |= 1L << shiftBits;
            // LSB is the remaining 8 bytes in the hash.
            long expectedLsb = BitConverter.readLong(hashBuffer, Integer.BYTES);
            Assert.assertEquals("Unexpected MSB.", expectedMsb, result.getMostSignificantBits());
            Assert.assertEquals("Unexpected LSB.", expectedLsb, result.getLeastSignificantBits());
        });
        // Verify we cannot accept bad arguments.
        AssertExtensions.assertThrows(
                "getSecondaryHashAttributeKey accepted an input with a hash of the wrong length.",
                () -> ac.getSecondaryHashAttributeKey(new ByteArraySegment(hashBuffer, 0, AttributeCalculator.SECONDARY_HASH_LENGTH - 1), 1),
                ex -> ex instanceof IllegalArgumentException);
        Arrays.asList(-1, 0, AttributeCalculator.MAX_NODE_ID + 1)
              .forEach(nodeId -> AssertExtensions.assertThrows(
                      "getPrimaryHashAttributeKey accepted an input with an invalid node id: " + nodeId,
                      () -> ac.getSecondaryHashAttributeKey(new ByteArraySegment(hashBuffer), nodeId),
                      ex -> ex instanceof IllegalArgumentException));
    }

    /**
     * Tests the getBackpointerAttributeKey() method.
     */
    @Test
    public void testBackpointerAttributeKey() {
        val ac = new AttributeCalculator();
        Arrays.asList(0L, 1L, 12346L, Long.MAX_VALUE)
              .forEach(offset -> {
                  UUID result = ac.getBackpointerAttributeKey(offset);
                  Assert.assertEquals("Expecting MSB to be 0.", 0, result.getMostSignificantBits());
                  Assert.assertEquals("Expecting LSB to equal offset.", (long) offset, result.getLeastSignificantBits());
              });
        AssertExtensions.assertThrows(
                "getBackpointerAttributeKey accepted bad input.",
                () -> ac.getBackpointerAttributeKey(-1),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests getIndexNodeAttributeValue(), getSegmentOffsetAttributeValue(), extractValue() and isIndexNodePointer().
     */
    @Test
    public void testNodeAttributeValues() {
        // Index Nodes (getIndexNodeAttributeValue).
        testNodeAttributeValues(
                Arrays.asList(0L, 1L, 12345L, Long.MAX_VALUE),
                Arrays.asList(Long.MIN_VALUE, -1L),
                AttributeCalculator::getIndexNodeAttributeValue,
                true);
        // Non-Index (data) Nodes (getSegmentOffsetAttributeValue).
        testNodeAttributeValues(
                Arrays.asList(0L, 1L, 12345L, Long.MAX_VALUE),
                Arrays.asList(Long.MIN_VALUE, -1L),
                AttributeCalculator::getSegmentOffsetAttributeValue,
                false);
    }

    private void testNodeAttributeValues(Collection<Long> goodValues, Collection<Long> badValues, BiFunction<AttributeCalculator, Long, Long> function, boolean indexNodes) {
        val ac = new AttributeCalculator();
        // Good values.
        for (long v : goodValues) {
            val nodeValue = function.apply(ac, v);
            Assert.assertEquals("Unexpected value for isIndexNodePointer() for " + v, indexNodes, ac.isIndexNodePointer(nodeValue));
            val reversed = ac.extractValue(nodeValue);
            Assert.assertEquals("Unexpected value from extractValue() for " + v, v, reversed);
        }
        // Bad values.
        for (long v : badValues) {
            AssertExtensions.assertThrows(
                    "Bad value accepted: " + v,
                    () -> function.apply(ac, v),
                    ex -> ex instanceof IllegalArgumentException);
        }
    }
}