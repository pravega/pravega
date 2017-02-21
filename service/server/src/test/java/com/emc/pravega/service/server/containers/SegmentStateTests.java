/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.io.EnhancedByteArrayOutputStream;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.testcommon.AssertExtensions;
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
