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
package com.emc.pravega.stream.impl.model;

import com.emc.pravega.controller.stream.api.v1.FutureSegment;
import com.emc.pravega.controller.stream.api.v1.ScalingPolicyType;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.PositionImpl;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ModelHelperTest {

    private static Segment createSegmentId(String streamName, int number) {
        return new Segment("scope", streamName, number);
    }

    private static com.emc.pravega.stream.impl.FutureSegment createFutureSegmentId(String streamName, int number, int
            previous) {
        return new com.emc.pravega.stream.impl.FutureSegment("scope", streamName, number, previous);
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
        };
    }

    private static PositionInternal createPosition() {
        Map<Segment, Long> ownedLogs = new HashMap<>();
        ownedLogs.put(createSegmentId("stream", 1), 1L);

        Map<com.emc.pravega.stream.impl.FutureSegment, Long> futureOwnedLogs = new HashMap<>();
        futureOwnedLogs.put(createFutureSegmentId("stream", 2, 1), 2L);

        return new PositionImpl(ownedLogs, futureOwnedLogs);
    }

    @Test(expected = NullPointerException.class)
    public void decodeSegmentIdNullTest() {
        ModelHelper.decode((Segment) null);
    }

    @Test
    public void decodeSegmentId() {
        final String streamName = "stream1";

        com.emc.pravega.controller.stream.api.v1.SegmentId segmentID = ModelHelper.decode(createSegmentId(streamName,
                2));
        assertEquals(streamName, segmentID.getStreamName());
        assertEquals("scope", segmentID.getScope());
        assertEquals(2, segmentID.getNumber());
    }

    @Test(expected = NullPointerException.class)
    public void encodeSegmentIdNullInput() {
        ModelHelper.encode(null, 0);
    }

    @Test
    public void encodeSegmentId() {
        Segment segment = ModelHelper.encode(ModelHelper.decode(createSegmentId("stream1", 2)), 1);
        assertEquals("stream1", segment.getStreamName());
        assertEquals("scope", segment.getScope());
        assertEquals(2, segment.getSegmentNumber());
    }

    @Test // Preceding 
    public void encodeFutureSegmentId() {
        com.emc.pravega.stream.impl.FutureSegment segment = ModelHelper.encode(
                ModelHelper.decode(createFutureSegmentId("stream1", 2, 1)), 1);
        assertEquals("stream1", segment.getStreamName());
        assertEquals("scope", segment.getScope());
        assertEquals(2, segment.getSegmentNumber());
        assertEquals(1, segment.getPrecedingNumber());
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
        assertEquals(1, position.getFutureOwnedSegments().size());
        SegmentId id = ModelHelper.decode(createSegmentId("stream", 1));
        assertEquals(1L, position.getOwnedSegments().get(id).longValue());
        SegmentId future = ModelHelper.decode(createSegmentId("stream", 2));
        Long val = position.getFutureOwnedSegments().get(new FutureSegment(future, id));
        assertEquals(2L, val.longValue());
    }

    @Test(expected = NullPointerException.class)
    public void encodePositionNullInput() {
        ModelHelper.encode((com.emc.pravega.controller.stream.api.v1.Position) null);
    }

    @Test
    public void encodePosition() {
        PositionInternal position = ModelHelper.encode(ModelHelper.decode(createPosition()));
        Map<Segment, Long> ownedLogs = position.getOwnedSegmentsWithOffsets();
        Map<com.emc.pravega.stream.impl.FutureSegment, Long> futureOwnedLogs = position
                .getFutureOwnedSegmentsWithOffsets();
        assertEquals(1, ownedLogs.size());
        assertEquals(1, futureOwnedLogs.size());
        assertEquals(1L, ownedLogs.get(createSegmentId("stream", 1)).longValue());
        assertEquals(2L, futureOwnedLogs.get(createFutureSegmentId("stream", 2, 1)).longValue());
    }
}
