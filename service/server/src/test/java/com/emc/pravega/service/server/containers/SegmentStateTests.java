/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.io.EnhancedByteArrayOutputStream;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.testcommon.AssertExtensions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Date;
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
                new StreamSegmentInformation(Integer.toString(attributeCount), 0, false, false, attributes, new Date()));
    }
}
