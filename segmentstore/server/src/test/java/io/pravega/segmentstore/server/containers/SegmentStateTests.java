/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.test.common.AssertExtensions;
import java.util.HashMap;
import java.util.UUID;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the SegmentState class.
 */
public class SegmentStateTests {
    /**
     * Tests the serialization/deserialization of the class.
     */
    @Test
    public void testSerialization() throws Exception {
        final int maxAttributeCount = 10;
        for (int attributeCount = 0; attributeCount < maxAttributeCount; attributeCount++) {
            SegmentState original = create(attributeCount);
            val s = SegmentState.SERIALIZER.serialize(original);

            SegmentState deserialized = SegmentState.SERIALIZER.deserialize(s);
            Assert.assertEquals("Unexpected segment id", original.getSegmentId(), deserialized.getSegmentId());
            Assert.assertEquals("Unexpected segment name.", original.getSegmentName(), deserialized.getSegmentName());
            Assert.assertEquals("Unexpected start offset.", original.getStartOffset(), deserialized.getStartOffset());
            AssertExtensions.assertMapEquals("Unexpected attributes.", original.getAttributes(), deserialized.getAttributes());
        }
    }

    private SegmentState create(int attributeCount) {
        HashMap<UUID, Long> attributes = new HashMap<>();
        for (int i = 0; i < attributeCount; i++) {
            attributes.put(UUID.randomUUID(), (long) i);
        }

        return new SegmentState(attributeCount, StreamSegmentInformation
                .builder()
                .name(Integer.toString(attributeCount))
                .startOffset(attributeCount)
                .length(attributeCount + 1)
                .attributes(attributes)
                .build());
    }
}
