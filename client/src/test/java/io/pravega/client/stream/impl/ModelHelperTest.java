/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.client.stream.RetentionPolicy;

import java.util.Arrays;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ModelHelperTest {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

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

    @Test
    public void encodeRetentionPolicy() {
        RetentionPolicy policy1 = ModelHelper.encode(ModelHelper.decode(RetentionPolicy.bySizeBytes(1000L)));
        assertEquals(RetentionPolicy.Type.SIZE, policy1.getType());
        assertEquals(1000L, (long) policy1.getValue());

        RetentionPolicy policy2 = ModelHelper.encode(ModelHelper.decode(RetentionPolicy.byTime(Duration.ofDays(100L))));
        assertEquals(RetentionPolicy.Type.TIME, policy2.getType());
        assertEquals(Duration.ofDays(100L).toMillis(), (long) policy2.getValue());

        RetentionPolicy policy3 = ModelHelper.encode(ModelHelper.decode((RetentionPolicy) null));
        assertNull(policy3);
    }

    @Test
    public void decodeRetentionPolicy() {
        Controller.RetentionPolicy policy1 = ModelHelper.decode(RetentionPolicy.bySizeBytes(1000L));
        assertEquals(Controller.RetentionPolicy.RetentionPolicyType.SIZE, policy1.getType());
        assertEquals(1000L, policy1.getValue());

        Controller.RetentionPolicy policy2 = ModelHelper.decode(RetentionPolicy.byTime(Duration.ofDays(100L)));
        assertEquals(Controller.RetentionPolicy.RetentionPolicyType.TIME, policy2.getType());
        assertEquals(Duration.ofDays(100L).toMillis(), policy2.getValue());

        Controller.RetentionPolicy policy3 = ModelHelper.decode((RetentionPolicy) null);
        assertNull(policy3);
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
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(100L)))
                .build());
        assertEquals("test", config.getStreamInfo().getStream());
        Controller.ScalingPolicy policy = config.getScalingPolicy();
        assertEquals(Controller.ScalingPolicy.ScalingPolicyType.BY_RATE_IN_EVENTS_PER_SEC, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
        Controller.RetentionPolicy retentionPolicy = config.getRetentionPolicy();
        assertEquals(Controller.RetentionPolicy.RetentionPolicyType.TIME, retentionPolicy.getType());
        assertEquals(Duration.ofDays(100L).toMillis(), retentionPolicy.getValue());
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
          .retentionPolicy(RetentionPolicy.bySizeBytes(1000L))
          .build()));
        assertEquals("test", config.getStreamName());
        ScalingPolicy policy = config.getScalingPolicy();
        assertEquals(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC, policy.getType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
        RetentionPolicy retentionPolicy = config.getRetentionPolicy();
        assertEquals(RetentionPolicy.Type.SIZE, retentionPolicy.getType());
        assertEquals(1000L, (long) retentionPolicy.getValue());
    }

    @Test
    public void createSuccessorResponse() {
        Controller.SegmentRange segmentRange = createSegmentRange(0.1, 0.5);

        Map<Controller.SegmentRange, List<Integer>> inputMap = new HashMap<>(1);
        inputMap.put(segmentRange, Arrays.asList(1));

        Controller.SuccessorResponse successorResponse = ModelHelper.createSuccessorResponse(inputMap);
        Assert.assertEquals(1, successorResponse.getSegmentsCount());
        final SegmentId resultSegmentID = successorResponse.getSegments(0).getSegment().getSegmentId();
        assertEquals("testScope", resultSegmentID.getStreamInfo().getScope());
        assertEquals("testStream", resultSegmentID.getStreamInfo().getStream());
    }

    @Test
    public void encodeSegmentRange() {
        Controller.SegmentRange range = createSegmentRange(0.1, 0.5);
        SegmentWithRange result = ModelHelper.encode(range);
        assertEquals(0, result.getSegment().getSegmentNumber());
        assertEquals("testScope", result.getSegment().getScope());
        assertEquals("testStream", result.getSegment().getStreamName());

        final Controller.SegmentRange invalidMinSegrange = createSegmentRange(-0.1, 0.5);
        AssertExtensions.assertThrows("Unexpected behaviour of invalid minkey",
                () -> ModelHelper.encode(invalidMinSegrange),
                ex -> ex instanceof IllegalArgumentException);

        final Controller.SegmentRange invalidMinSegrange1 = createSegmentRange(1.5, 0.5);
        AssertExtensions.assertThrows("Unexpected behaviour of invalid minkey",
                () -> ModelHelper.encode(invalidMinSegrange1),
                ex -> ex instanceof IllegalArgumentException);

        final Controller.SegmentRange invalidMaxSegrange = createSegmentRange(0.1, 1.5);
        AssertExtensions.assertThrows("Unexpected behaviour of invalid minkey",
                () -> ModelHelper.encode(invalidMaxSegrange),
                ex -> ex instanceof IllegalArgumentException);

        final Controller.SegmentRange invalidMaxSegrange1 = createSegmentRange(0.1, -0.5);
        AssertExtensions.assertThrows("Unexpected behaviour of invalid minkey",
                () -> ModelHelper.encode(invalidMaxSegrange1),
                ex -> ex instanceof IllegalArgumentException);

    }

    private Controller.SegmentRange createSegmentRange(double minKey, double maxKey) {
        SegmentId.Builder segment = SegmentId.newBuilder().setStreamInfo(Controller.StreamInfo.newBuilder().
                setScope("testScope").setStream("testStream")).setSegmentNumber(0);
        return Controller.SegmentRange.newBuilder().setSegmentId(segment)
                .setMinKey(minKey).setMaxKey(maxKey).build();
    }
}
