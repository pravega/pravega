/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.controller.stream.api.v1.ScalingPolicyType;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ModelHelperTest {

    private static Segment createSegmentId(String streamName, int number) {
        return new Segment("scope", streamName, number);
    }

    private static ScalingPolicy createScalingPolicy() {
        ScalingPolicy policy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        return policy;
    }

    private static StreamConfiguration createStreamConfig(String name) {
        return new StreamConfiguration() {
            @Override
            public String getScope() {
                return "scope";
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public ScalingPolicy getScalingPolicy() {
                return createScalingPolicy();
            }

            @Override
            public RetentionPolicy getRetentionPolicy() {
                return new RetentionPolicy(Long.MAX_VALUE);
            }
        };
    }

    private static PositionInternal createPosition() {
        Map<Segment, Long> ownedLogs = new HashMap<>();
        ownedLogs.put(createSegmentId("stream", 1), 1L);
        return new PositionImpl(ownedLogs);
    }

    @Test(expected = NullPointerException.class)
    public void decodeSegmentIdNullTest() {
        ModelHelper.decode((Segment) null);
    }

    @Test
    public void decodeSegmentId() {
        final String streamName = "stream1";

        com.emc.pravega.controller.stream.api.v1.SegmentId segmentID = ModelHelper.decode(createSegmentId(streamName, 2));
        assertEquals(streamName, segmentID.getStreamName());
        assertEquals("scope", segmentID.getScope());
        assertEquals(2, segmentID.getNumber());
    }

    @Test(expected = NullPointerException.class)
    public void encodeSegmentIdNullInput() {
        ModelHelper.encode((com.emc.pravega.controller.stream.api.v1.SegmentId) null);
    }

    @Test
    public void encodeSegmentId() {
        Segment segment = ModelHelper.encode(ModelHelper.decode(createSegmentId("stream1", 2)));
        assertEquals("stream1", segment.getStreamName());
        assertEquals("scope", segment.getScope());
        assertEquals(2, segment.getSegmentNumber());
    }

    @Test(expected = NullPointerException.class)
    public void decodeScalingPolicyNullInput() throws Exception {
        ModelHelper.decode((ScalingPolicy) null);
    }

    @Test
    public void decodeScalingPolicy() {
        com.emc.pravega.controller.stream.api.v1.ScalingPolicy policy = ModelHelper.decode(createScalingPolicy());
        assertEquals(ScalingPolicyType.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
    }

    @Test(expected = NullPointerException.class)
    public void encodeScalingPolicyNullInput() {
        ModelHelper.encode((com.emc.pravega.controller.stream.api.v1.ScalingPolicy) null);
    }

    @Test
    public void encodeScalingPolicy() {
        ScalingPolicy policy = ModelHelper.encode(ModelHelper.decode(createScalingPolicy()));
        assertEquals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
    }

    @Test(expected = NullPointerException.class)
    public void decodeStreamConfigNullInput() {
        ModelHelper.decode((StreamConfiguration) null);
    }

    @Test
    public void decodeStreamConfig() {
        StreamConfig config = ModelHelper.decode(createStreamConfig("test"));
        assertEquals("test", config.getName());
        com.emc.pravega.controller.stream.api.v1.ScalingPolicy policy = config.getPolicy();
        assertEquals(ScalingPolicyType.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
    }

    @Test(expected = NullPointerException.class)
    public void encodeStreamConfigNullInput() {
        ModelHelper.encode((StreamConfig) null);
    }

    @Test
    public void encodeStreamConfig() {
        StreamConfiguration config = ModelHelper.encode(ModelHelper.decode(createStreamConfig("test")));
        assertEquals("test", config.getName());
        ScalingPolicy policy = config.getScalingPolicy();
        assertEquals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
    }

    @Test(expected = NullPointerException.class)
    public void decodePositionNullInput() {
        ModelHelper.decode((PositionInternal) null);
    }

    @Test
    public void decodePosition() {
        com.emc.pravega.controller.stream.api.v1.Position position = ModelHelper.decode(createPosition());
        assertEquals(1, position.getOwnedSegments().size());
        SegmentId id = ModelHelper.decode(createSegmentId("stream", 1));
        assertEquals(1L, position.getOwnedSegments().get(id).longValue());
        SegmentId id2 = ModelHelper.decode(createSegmentId("stream", 2));
        assertEquals(2L, id2.getNumber());
    }

    @Test(expected = NullPointerException.class)
    public void encodePositionNullInput() {
        ModelHelper.encode((com.emc.pravega.controller.stream.api.v1.Position) null);
    }

    @Test
    public void encodePosition() {
        PositionInternal position = ModelHelper.encode(ModelHelper.decode(createPosition()));
        Map<Segment, Long> ownedLogs = position.getOwnedSegmentsWithOffsets();
        assertEquals(1, ownedLogs.size());
        assertEquals(1L, ownedLogs.get(createSegmentId("stream", 1)).longValue());
    }
}
