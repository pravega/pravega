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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.test.common.AssertExtensions;
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

            ByteBuf s1 = this.serializer.serialize(expectedKF);
            ByteBuf s2 = expectedValue == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(KeyFamilySerializer.ENCODING.encode(expectedValue));
            ByteBuf serialized = Unpooled.wrappedBuffer(s1, s2).readerIndex(0);

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
                () -> this.serializer.deserialize(Unpooled.wrappedBuffer(new byte[KeyFamilySerializer.PREFIX_LENGTH - 1])),
                ex -> ex instanceof SerializationException);

        // When buffer can't accommodate declared key family length.
        val kf = "KeyFamily";
        val s1 = this.serializer.serialize(kf);
        AssertExtensions.assertThrows(
                "Deserialized worked with invalid key family length.",
                () -> this.serializer.deserialize(s1.slice(0, s1.readableBytes() - 1)),
                ex -> ex instanceof SerializationException);

        // When deserialized key length is invalid.
        val s2 = this.serializer.serialize(kf);
        s2.setShort(0, -1);
        AssertExtensions.assertThrows(
                "Deserialized worked with negative key family length.",
                () -> this.serializer.deserialize(s2),
                ex -> ex instanceof SerializationException);

        s2.readerIndex(0);
        s2.setShort(0, KeyFamilySerializer.MAX_KEY_FAMILY_LENGTH + 1);
        AssertExtensions.assertThrows(
                "Deserialized worked with overflowing key family length.",
                () -> this.serializer.deserialize(s2),
                ex -> ex instanceof SerializationException);
    }
}
