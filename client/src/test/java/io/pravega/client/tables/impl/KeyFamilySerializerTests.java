/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.common.util.BitConverter;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import lombok.val;
import org.apache.commons.lang3.SerializationException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyFamilySerializer} class.
 */
public class KeyFamilySerializerTests {
    private final KeyFamilySerializer serializer = new KeyFamilySerializer();

    /**
     * Tests the cases when the provided Key Family is null or empty.
     */
    @Test
    public void testNullOrEmpty() {
        val nullSerialization = this.serializer.serialize(null);
        val emptySerialization = this.serializer.serialize("");
        Assert.assertNull("Null Key Family.", this.serializer.deserialize(nullSerialization));
        Assert.assertNull("Empty Key Family.", this.serializer.deserialize(emptySerialization));
    }

    /**
     * Tests normal cases.
     */
    @Test
    public void testNormalSerialization() {
        val toTest = Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>("", ""),
                new AbstractMap.SimpleImmutableEntry<>("", "abc"),
                new AbstractMap.SimpleImmutableEntry<>("abc", ""),
                new AbstractMap.SimpleImmutableEntry<>("abc", "def"));
        for (val e : toTest) {
            String expectedKF = e.getKey().length() == 0 ? null : e.getKey();
            String expectedValue = e.getValue().length() == 0 ? null : e.getValue();

            ByteBuffer s1 = this.serializer.serialize(expectedKF);
            ByteBuffer s2 = expectedValue == null ? ByteBuffer.allocate(0) : KeyFamilySerializer.ENCODING.encode(expectedValue);
            ByteBuffer serialized = (ByteBuffer) ByteBuffer.allocate(s1.remaining() + s2.remaining()).put(s1).put(s2).position(0);

            String actualKF = this.serializer.deserialize(serialized);
            Assert.assertEquals("Unexpected KeyFamily deserialized.", expectedKF, actualKF);
        }
    }

    /**
     * Tests various invalid scenarios for both serialization and deserialization.
     */
    @Test
    public void testInvalidSerialization() {
        // When key family too long.
        AssertExtensions.assertThrows(
                "Serialized worked with long key family.",
                () -> this.serializer.serialize(new String(new byte[KeyFamilySerializer.MAX_KEY_FAMILY_LENGTH + 1])),
                ex -> ex instanceof IllegalArgumentException);

        // If buffer is shorter than prefix.
        AssertExtensions.assertThrows(
                "Deserialized worked with short prefix.",
                () -> this.serializer.deserialize(ByteBuffer.wrap(new byte[KeyFamilySerializer.PREFIX_LENGTH - 1])),
                ex -> ex instanceof SerializationException);

        // When buffer can't accommodate declared key family length.
        val kf = "KeyFamily";
        val s1 = (ByteBuffer) this.serializer.serialize(kf);
        AssertExtensions.assertThrows(
                "Deserialized worked with invalid key family length.",
                () -> this.serializer.deserialize(ByteBuffer.wrap(s1.array(), 0, s1.remaining() - 1)),
                ex -> ex instanceof SerializationException);

        // When deserialized key length is invalid.
        val s2 = (ByteBuffer) this.serializer.serialize(kf);
        BitConverter.writeShort(s2.array(), 0, (short) -1);
        AssertExtensions.assertThrows(
                "Deserialized worked with negative key family length.",
                () -> this.serializer.deserialize(s2),
                ex -> ex instanceof SerializationException);

        s2.position(0);
        BitConverter.writeShort(s2.array(), 0, (short) (KeyFamilySerializer.MAX_KEY_FAMILY_LENGTH + 1));
        AssertExtensions.assertThrows(
                "Deserialized worked with overflowing key family length.",
                () -> this.serializer.deserialize(s2),
                ex -> ex instanceof SerializationException);
    }
}
