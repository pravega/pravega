/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server.containers;

import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ImmutableDate;
import io.pravega.service.contracts.StreamSegmentInformation;
import io.pravega.test.common.AssertExtensions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.UUID;
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
            EnhancedByteArrayOutputStream innerStream = new EnhancedByteArrayOutputStream();
            DataOutputStream stream = new DataOutputStream(innerStream);
            original.serialize(stream);
            stream.flush();

            ByteArraySegment serialization = innerStream.getData();
            SegmentState deserialized = SegmentState.deserialize(new DataInputStream(serialization.getReader()));
            Assert.assertEquals("Unexpected segment name.", original.getSegmentName(), deserialized.getSegmentName());
            AssertExtensions.assertMapEquals("Unexpected attributes.", original.getAttributes(), deserialized.getAttributes());
        }
    }

    private SegmentState create(int attributeCount) {
        HashMap<UUID, Long> attributes = new HashMap<>();
        for (int i = 0; i < attributeCount; i++) {
            attributes.put(UUID.randomUUID(), (long) i);
        }

        return new SegmentState(
                new StreamSegmentInformation(Integer.toString(attributeCount), 0, false, false, attributes, new ImmutableDate()));
    }
}
