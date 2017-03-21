/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Position;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
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

    private static PositionInternal createPosition() {
        Map<Segment, Long> ownedLogs = new HashMap<>();
        ownedLogs.put(createSegmentId("stream", 1), 1L);
        ownedLogs.put(createSegmentId("stream", 2), 2L);
        return new PositionImpl(ownedLogs);
    }

    @Test(expected = NullPointerException.class)
    public void decodeSegmentIdNullTest() {
        ModelHelper.decode((Segment) null);
    }

    @Test
    public void decodeSegmentId() {
        final String streamName = "stream1";

        SegmentId segmentID = ModelHelper.decode(createSegmentId(streamName, 2));
        assertEquals(streamName, segmentID.getStreamInfo().getStream());
        assertEquals("scope", segmentID.getStreamInfo().getScope());
        assertEquals(2, segmentID.getSegmentNumber());
    }

    @Test(expected = NullPointerException.class)
    public void encodeSegmentIdNullInput() {
        ModelHelper.encode((SegmentId) null);
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
        Controller.ScalingPolicy policy = ModelHelper.decode(ScalingPolicy.byEventRate(100, 2, 3));
        assertEquals(Controller.ScalingPolicy.ScalingPolicyType.BY_RATE_IN_EVENTS_PER_SEC, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
    }

    @Test(expected = NullPointerException.class)
    public void encodeScalingPolicyNullInput() {
        ModelHelper.encode((Controller.ScalingPolicy) null);
    }

    @Test
    public void encodeScalingPolicy() {
        ScalingPolicy policy = ModelHelper.encode(ModelHelper.decode(ScalingPolicy.byEventRate(100, 2, 3)));
        assertEquals(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC, policy.getType());
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
        StreamConfig config = ModelHelper.decode(StreamConfiguration.builder()
                                                 .scope("scope")
                                                 .streamName("test")
                                                 .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 3))
                                                 .build());
        assertEquals("test", config.getStreamInfo().getStream());
        Controller.ScalingPolicy policy = config.getPolicy();
        assertEquals(Controller.ScalingPolicy.ScalingPolicyType.BY_RATE_IN_EVENTS_PER_SEC, policy.getType());
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
        StreamConfiguration config = ModelHelper.encode(ModelHelper.decode(StreamConfiguration.builder()
          .scope("scope")
          .streamName("test")
          .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 3))
          .build()));
        assertEquals("test", config.getStreamName());
        ScalingPolicy policy = config.getScalingPolicy();
        assertEquals(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC, policy.getType());
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
        Position position = ModelHelper.decode(createPosition());
        assertEquals(2, position.getOwnedSegmentsCount());
        assertEquals(1L, position.getOwnedSegments(0).getValue());
        assertEquals(2L, position.getOwnedSegments(1).getValue());
    }

    @Test(expected = NullPointerException.class)
    public void encodePositionNullInput() {
        ModelHelper.encode((Position) null);
    }

    @Test
    public void encodePosition() {
        PositionInternal position = ModelHelper.encode(ModelHelper.decode(createPosition()));
        Map<Segment, Long> ownedLogs = position.getOwnedSegmentsWithOffsets();
        assertEquals(2, ownedLogs.size());
        assertEquals(1L, ownedLogs.get(createSegmentId("stream", 1)).longValue());
        assertEquals(2L, ownedLogs.get(createSegmentId("stream", 2)).longValue());
    }
}
