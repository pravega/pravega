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
package io.pravega.client.control.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.shared.NameUtils.getScopedStreamName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ModelHelperTest {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    private static Segment createSegmentId(String streamName, long number) {
        return new Segment("scope", streamName, number);
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
        assertEquals(2, segmentID.getSegmentId());
    }

    @Test(expected = NullPointerException.class)
    public void encodeSegmentIdNullInput() {
        ModelHelper.encode((SegmentId) null);
    }

    @Test
    public void encodeSegmentId() {
        Segment segment = ModelHelper.encode(ModelHelper.decode(createSegmentId("stream1", 2L)));
        assertEquals("stream1", segment.getStreamName());
        assertEquals("scope", segment.getScope());
        assertEquals(2L, segment.getSegmentId());
    }

    @Test
    public void encodeSegmentWithRange() {
        SegmentWithRange segment = ModelHelper.encode(createSegmentRange(.25, .75));
        assertEquals("testStream", segment.getSegment().getStreamName());
        assertEquals("testScope", segment.getSegment().getScope());
        assertEquals(.25, segment.getRange().getLow(), 0.0);
        assertEquals(.75, segment.getRange().getHigh(), 0.0);
    }

    @Test(expected = NullPointerException.class)
    public void decodeScalingPolicyNullInput() throws Exception {
        ModelHelper.decode((ScalingPolicy) null);
    }

    @Test
    public void decodeScalingPolicy() {
        Controller.ScalingPolicy policy = ModelHelper.decode(ScalingPolicy.byEventRate(100, 2, 3));
        assertEquals(Controller.ScalingPolicy.ScalingPolicyType.BY_RATE_IN_EVENTS_PER_SEC, policy.getScaleType());
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
        assertEquals(ScalingPolicy.ScaleType.BY_RATE_IN_EVENTS_PER_SEC, policy.getScaleType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
    }

    @Test
    public void encodeRetentionPolicy() {
        RetentionPolicy policy1 = ModelHelper.encode(ModelHelper.decode(RetentionPolicy.bySizeBytes(1000L)));
        assertEquals(RetentionPolicy.RetentionType.SIZE, policy1.getRetentionType());
        assertEquals(1000L, (long) policy1.getRetentionParam());

        RetentionPolicy policy2 = ModelHelper.encode(ModelHelper.decode(RetentionPolicy.byTime(Duration.ofDays(100L))));
        assertEquals(RetentionPolicy.RetentionType.TIME, policy2.getRetentionType());
        assertEquals(Duration.ofDays(100L).toMillis(), (long) policy2.getRetentionParam());

        RetentionPolicy policy3 = ModelHelper.encode(ModelHelper.decode((RetentionPolicy) null));
        assertNull(policy3);
    }

    @Test
    public void decodeRetentionPolicy() {
        Controller.RetentionPolicy policy1 = ModelHelper.decode(RetentionPolicy.bySizeBytes(1000L));
        assertEquals(Controller.RetentionPolicy.RetentionPolicyType.SIZE, policy1.getRetentionType());
        assertEquals(1000L, policy1.getRetentionParam());

        Controller.RetentionPolicy policy2 = ModelHelper.decode(RetentionPolicy.byTime(Duration.ofDays(100L)));
        assertEquals(Controller.RetentionPolicy.RetentionPolicyType.TIME, policy2.getRetentionType());
        assertEquals(Duration.ofDays(100L).toMillis(), policy2.getRetentionParam());

        Controller.RetentionPolicy policy3 = ModelHelper.decode((RetentionPolicy) null);
        assertNull(policy3);
    }

    @Test(expected = NullPointerException.class)
    public void decodeStreamConfigNullInput() {
        ModelHelper.decode("", "", (StreamConfiguration) null);
    }

    @Test
    public void decodeStreamConfig() {
        StreamConfig config = ModelHelper.decode("scope", "test", StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 3))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(100L)))
                .build());
        assertEquals("test", config.getStreamInfo().getStream());
        Controller.ScalingPolicy policy = config.getScalingPolicy();
        assertEquals(Controller.ScalingPolicy.ScalingPolicyType.BY_RATE_IN_EVENTS_PER_SEC, policy.getScaleType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
        Controller.RetentionPolicy retentionPolicy = config.getRetentionPolicy();
        assertEquals(Controller.RetentionPolicy.RetentionPolicyType.TIME, retentionPolicy.getRetentionType());
        assertEquals(Duration.ofDays(100L).toMillis(), retentionPolicy.getRetentionParam());
    }

    @Test(expected = NullPointerException.class)
    public void encodeStreamConfigNullInput() {
        ModelHelper.encode((StreamConfig) null);
    }

    @Test
    public void encodeStreamConfig() {
        StreamConfiguration config = ModelHelper.encode(ModelHelper.decode("scope", "test", StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 3))
                .retentionPolicy(RetentionPolicy.bySizeBytes(1000L))
                .build()));
        ScalingPolicy policy = config.getScalingPolicy();
        assertEquals(ScalingPolicy.ScaleType.BY_RATE_IN_EVENTS_PER_SEC, policy.getScaleType());
        assertEquals(100L, policy.getTargetRate());
        assertEquals(2, policy.getScaleFactor());
        assertEquals(3, policy.getMinNumSegments());
        RetentionPolicy retentionPolicy = config.getRetentionPolicy();
        assertEquals(RetentionPolicy.RetentionType.SIZE, retentionPolicy.getRetentionType());
        assertEquals(1000L, (long) retentionPolicy.getRetentionParam());
    }

    @Test
    public void createSuccessorResponse() {
        Controller.SegmentRange segmentRange = createSegmentRange(0.1, 0.5);

        Map<Controller.SegmentRange, List<Long>> inputMap = new HashMap<>(1);
        inputMap.put(segmentRange, Arrays.asList(1L));

        Controller.SuccessorResponse successorResponse = ModelHelper.createSuccessorResponse(inputMap).build();
        Assert.assertEquals(1, successorResponse.getSegmentsCount());
        final SegmentId resultSegmentID = successorResponse.getSegments(0).getSegment().getSegmentId();
        assertEquals("testScope", resultSegmentID.getStreamInfo().getScope());
        assertEquals("testStream", resultSegmentID.getStreamInfo().getStream());
    }

    @Test
    public void testStreamCutRequestAndResponse() {
        List<SegmentId> segments = Collections.singletonList(SegmentId.newBuilder().setStreamInfo(Controller.StreamInfo.newBuilder().
                setScope("testScope").setStream("testStream")).build());
        AssertExtensions.assertThrows("invalid scope and stream", () -> ModelHelper.createStreamCutRangeResponse("scope",
                "stream", segments, ""), e -> e instanceof IllegalArgumentException);

        Controller.StreamCutRangeResponse response = ModelHelper.createStreamCutRangeResponse("testScope", "testStream", segments, "");
        Assert.assertEquals(1, response.getSegmentsCount());
        final SegmentId resultSegmentID = response.getSegments(0);
        assertEquals("testScope", resultSegmentID.getStreamInfo().getScope());
        assertEquals("testStream", resultSegmentID.getStreamInfo().getStream());
        assertEquals(0L, resultSegmentID.getSegmentId());
    }

    @Test
    public void testStreamCutRange() {
        Map<Long, Long> from = Collections.singletonMap(0L, 0L);
        Map<Long, Long> to = Collections.singletonMap(1L, 0L);
        Controller.StreamCutRange response = ModelHelper.decode("scope", "stream", from, to);
        assertTrue(response.getFromMap().containsKey(0L));
        assertTrue(response.getToMap().containsKey(1L));
    }

    @Test
    public void encodeSegmentRange() {
        Controller.SegmentRange range = createSegmentRange(0.1, 0.5);
        SegmentWithRange result = ModelHelper.encode(range);
        assertEquals(0, result.getSegment().getSegmentId());
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

    @Test
    public void encodeKeyValueTableConfig() {
        Controller.KeyValueTableConfig config = Controller.KeyValueTableConfig.newBuilder()
                .setScope("scope").setKvtName("kvtable").setPartitionCount(2).build();
        KeyValueTableConfiguration configuration = ModelHelper.encode(config);
        assertEquals(config.getPartitionCount(), configuration.getPartitionCount());
    }

    @Test
    public void createStreamInfoWithMissingAccessOperation() {
        Controller.StreamInfo streamInfo = ModelHelper.createStreamInfo("testScope", "testStream");
        assertEquals("testScope", streamInfo.getScope());
        assertEquals("testStream", streamInfo.getStream());
        assertEquals(Controller.StreamInfo.AccessOperation.UNSPECIFIED, streamInfo.getAccessOperation());
    }

    @Test
    public void createStreamInfoWithAccessOperation() {
        assertEquals(Controller.StreamInfo.AccessOperation.READ,
                ModelHelper.createStreamInfo("testScope", "testStream", AccessOperation.READ).getAccessOperation());
        assertEquals(Controller.StreamInfo.AccessOperation.WRITE,
                ModelHelper.createStreamInfo("testScope", "testStream", AccessOperation.WRITE).getAccessOperation());
        assertEquals(Controller.StreamInfo.AccessOperation.READ_WRITE,
                ModelHelper.createStreamInfo("testScope", "testStream", AccessOperation.READ_WRITE).getAccessOperation());
    }

    @Test
    public void testReaderGroupConfig() {
        String scope = "test";
        String stream = "test";
        ImmutableMap<Segment, Long> positions = ImmutableMap.<Segment, Long>builder().put(new Segment(scope, stream, 0), 90L).build();
        StreamCut sc = new StreamCutImpl(Stream.of(scope, stream), positions);
        ReaderGroupConfig config = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(getScopedStreamName(scope, stream), StreamCut.UNBOUNDED, sc).build();
        Controller.ReaderGroupConfiguration decodedConfig = ModelHelper.decode(scope, "group", config);
        assertEquals(config, ModelHelper.encode(decodedConfig));
    }

    private Controller.SegmentRange createSegmentRange(double minKey, double maxKey) {
        SegmentId.Builder segment = SegmentId.newBuilder().setStreamInfo(Controller.StreamInfo.newBuilder().
                setScope("testScope").setStream("testStream")).setSegmentId(0);
        return Controller.SegmentRange.newBuilder().setSegmentId(segment)
                .setMinKey(minKey).setMaxKey(maxKey).build();
    }
}
