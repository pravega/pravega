/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.collect.ImmutableMap;
import io.pravega.test.common.AssertExtensions;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the CacheEntryLayout class.
 */
public class CacheEntryLayoutTests {
    private static final List<UUID> ATTRIBUTE_IDS = IntStream.range(0, 100)
                                                             .mapToObj(i -> new UUID(i + 1, (i + 1) * (i + 1)))
                                                             .collect(Collectors.toList());

    /**
     * Tests the serialization and deserialization of attributes.
     */
    @Test
    public void testSerialization() {
        Function<UUID, Long> getValue = id -> id.getMostSignificantBits() + id.getLeastSignificantBits();
        Function<UUID, Long> getVersion = id -> id.getLeastSignificantBits() - id.getMostSignificantBits();
        val values = ATTRIBUTE_IDS.stream()
                                  .map(id -> (Map.Entry<UUID, VersionedValue>) new AbstractMap.SimpleImmutableEntry<>(id, new VersionedValue(getVersion.apply(id), getValue.apply(id))))
                                  .collect(Collectors.toList());
        val expectedValues = ImmutableMap.<UUID, VersionedValue>builder().putAll(values).build();

        // Serialize the data.
        byte[] buffer = new byte[ATTRIBUTE_IDS.size() * 1000];
        val data = CacheEntryLayout.setValues(buffer, values.iterator(), values.size());
        Assert.assertSame("New buffer was created when none was expected.", buffer, data);

        // Verify the count was correctly encoded, even if we passed a larger than needed array.
        val l = CacheEntryLayout.wrap(data);
        Assert.assertEquals("Unexpected number of attributes encoded.", values.size(), l.getCount());

        // Verify we can get all the values back.
        val actualData = l.getAllValues();
        AssertExtensions.assertMapEquals("Unexpected values returned", expectedValues, actualData);

        // Verify that the values were encoded in order.
        for (int i = 0; i < values.size(); i++) {
            val expected = values.get(i);
            val actualId = l.getKey(i);
            val actualValue = l.getValue(i);
            Assert.assertEquals("Attribute Ids out of order.", expected.getKey(), actualId);
            Assert.assertEquals("Attribute Values out of order.", expected.getValue().value, (long) actualValue);
        }
    }
}
