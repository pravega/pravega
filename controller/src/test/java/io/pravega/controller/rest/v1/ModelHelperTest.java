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
package io.pravega.controller.rest.v1;

import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.controller.server.rest.generated.model.TimeBasedRetention;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

import static io.pravega.controller.server.rest.ModelHelper.encodeStreamResponse;
import static io.pravega.controller.server.rest.ModelHelper.getCreateStreamConfig;
import static io.pravega.controller.server.rest.ModelHelper.getUpdateStreamConfig;

/**
 * Test cases for rest/ModelHelper.java.
 */
public class ModelHelperTest {

    @Test(timeout = 10000)
    public void testGetCreateStreamConfig() {
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingConfig.setMinSegments(2);
        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);

        // Stream with Fixed Scaling Policy and no Retention Policy and default rolloverSize
        StreamConfiguration streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(2, streamConfig.getScalingPolicy().getMinNumSegments());
        Assert.assertNull(streamConfig.getRetentionPolicy());
        Assert.assertEquals(streamConfig.getTimestampAggregationTimeout(), 0);
        Assert.assertEquals(streamConfig.getRolloverSizeBytes(), 0);

        // Stream with Fixed Scaling Policy and no Retention Policy and positive rolloverSize
        createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setTimestampAggregationTimeout(1000L);
        createStreamRequest.setRolloverSizeBytes(1024L);

        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(streamConfig.getTimestampAggregationTimeout(), 1000L);
        Assert.assertEquals(streamConfig.getRolloverSizeBytes(), 1024L);

        // Stream with Fixed Scaling Policy & Size based Retention Policy with min & max limits
        RetentionConfig retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_SIZE_MB);
        retentionConfig.setValue(1234L);
        retentionConfig.setMaxValue(4567L);
        createStreamRequest.setRetentionPolicy(retentionConfig);
        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(2, streamConfig.getScalingPolicy().getMinNumSegments());
        Assert.assertNotNull(streamConfig.getRetentionPolicy());
        Assert.assertEquals(RetentionPolicy.RetentionType.SIZE, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(1234L * 1024 * 1024, streamConfig.getRetentionPolicy().getRetentionParam());
        Assert.assertEquals(4567L * 1024 * 1024, streamConfig.getRetentionPolicy().getRetentionMax());

        scalingConfig.setType(ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC);
        scalingConfig.setTargetRate(123);
        scalingConfig.setScaleFactor(2);
        retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(1234L);
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);

        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.BY_RATE_IN_EVENTS_PER_SEC, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(2, streamConfig.getScalingPolicy().getScaleFactor());
        Assert.assertEquals(123, streamConfig.getScalingPolicy().getTargetRate());
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(Duration.ofDays(1234L).toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());
        Assert.assertEquals(Long.MAX_VALUE, streamConfig.getRetentionPolicy().getRetentionMax());

        retentionConfig.setValue(0L);
        TimeBasedRetention tr = new TimeBasedRetention();
        tr.days(10L).hours(4L).minutes(7L);
        retentionConfig.setTimeBasedRetention(tr);
        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Duration retentionDuration = Duration.ofDays(10L).plusHours(4L).plusMinutes(7L);
        Assert.assertEquals(retentionDuration.toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());
        Assert.assertEquals(Long.MAX_VALUE, streamConfig.getRetentionPolicy().getRetentionMax());

        retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(3L);
        tr = new TimeBasedRetention();
        tr.days(200L).hours(2L).minutes(5L);
        retentionConfig.setMaxTimeBasedRetention(tr);
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);
        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(Duration.ofDays(3L).toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());
        retentionDuration = Duration.ofDays(200L).plusHours(2L).plusMinutes(5L);
        Assert.assertEquals(retentionDuration.toMillis(), streamConfig.getRetentionPolicy().getRetentionMax());

        retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(3L);
        tr = new TimeBasedRetention();
        tr.days(200L).hours(2L).minutes(5L);
        retentionConfig.setMaxTimeBasedRetention(tr);
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);
        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(Duration.ofDays(3L).toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());
        retentionDuration = Duration.ofDays(200L).plusHours(2L).plusMinutes(5L);
        Assert.assertEquals(retentionDuration.toMillis(), streamConfig.getRetentionPolicy().getRetentionMax());

        retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(0L);
        tr = new TimeBasedRetention();
        tr.days(2L).hours(2L).minutes(20L);
        retentionConfig.setTimeBasedRetention(tr);
        TimeBasedRetention trMax = new TimeBasedRetention();
        trMax.days(300L).hours(3L).minutes(30L);
        retentionConfig.setMaxTimeBasedRetention(trMax);
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);
        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Duration retentionDurationMin = Duration.ofDays(2L).plusHours(2L).plusMinutes(20L);
        Assert.assertEquals(retentionDurationMin.toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());
        Duration retentionDurationMax = Duration.ofDays(300L).plusHours(3L).plusMinutes(30L);
        Assert.assertEquals(retentionDurationMax.toMillis(), streamConfig.getRetentionPolicy().getRetentionMax());

        retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(2L);
        tr = new TimeBasedRetention();
        tr.days(2L).hours(2L).minutes(20L);
        retentionConfig.setTimeBasedRetention(tr);
        retentionConfig.setMaxValue(100L);
        trMax = new TimeBasedRetention();
        trMax.days(400L).hours(4L).minutes(40L);
        retentionConfig.setMaxTimeBasedRetention(trMax);
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);
        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(Duration.ofDays(2L).toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());
        Assert.assertEquals(Duration.ofDays(100L).toMillis(), streamConfig.getRetentionPolicy().getRetentionMax());

        scalingConfig.setType(ScalingConfig.TypeEnum.BY_RATE_IN_KBYTES_PER_SEC);
        scalingConfig.setTargetRate(1234);
        scalingConfig.setScaleFactor(23);
        retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_SIZE_MB);
        retentionConfig.setValue(12345L);
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);

        streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(23, streamConfig.getScalingPolicy().getScaleFactor());
        Assert.assertEquals(1234, streamConfig.getScalingPolicy().getTargetRate());
        Assert.assertEquals(RetentionPolicy.RetentionType.SIZE, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(12345L * 1024 * 1024, streamConfig.getRetentionPolicy().getRetentionParam());
        Assert.assertEquals(Long.MAX_VALUE, streamConfig.getRetentionPolicy().getRetentionMax());

    }

    @Test(timeout = 10000)
    public void testGetUpdateStreamConfig() {
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingConfig.setMinSegments(2);
        UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
        updateStreamRequest.setScalingPolicy(scalingConfig);
        updateStreamRequest.setTimestampAggregationTimeout(1000L);
        updateStreamRequest.setRolloverSizeBytes(1024L);

        StreamConfiguration streamConfig = getUpdateStreamConfig(updateStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(2, streamConfig.getScalingPolicy().getMinNumSegments());
        Assert.assertNull(streamConfig.getRetentionPolicy());
        Assert.assertEquals(streamConfig.getTimestampAggregationTimeout(), 1000L);
        Assert.assertEquals(streamConfig.getRolloverSizeBytes(), 1024L);

        scalingConfig.setType(ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC);
        scalingConfig.setTargetRate(123);
        scalingConfig.setScaleFactor(2);
        RetentionConfig retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(1234L);
        updateStreamRequest.setScalingPolicy(scalingConfig);
        updateStreamRequest.setRetentionPolicy(retentionConfig);

        streamConfig = getUpdateStreamConfig(updateStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.BY_RATE_IN_EVENTS_PER_SEC, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(2, streamConfig.getScalingPolicy().getScaleFactor());
        Assert.assertEquals(123, streamConfig.getScalingPolicy().getTargetRate());
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(Duration.ofDays(1234L).toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());

        retentionConfig.setValue(0L);
        TimeBasedRetention tr = new TimeBasedRetention();
        tr.days(21L).hours(8L).minutes(25L);
        retentionConfig.setTimeBasedRetention(tr);
        streamConfig = getUpdateStreamConfig(updateStreamRequest);
        Assert.assertEquals(RetentionPolicy.RetentionType.TIME, streamConfig.getRetentionPolicy().getRetentionType());
        Duration retentionDuration = Duration.ofDays(21L).plusHours(8L).plusMinutes(25L);
        Assert.assertEquals(retentionDuration.toMillis(), streamConfig.getRetentionPolicy().getRetentionParam());

        scalingConfig.setType(ScalingConfig.TypeEnum.BY_RATE_IN_KBYTES_PER_SEC);
        scalingConfig.setTargetRate(1234);
        scalingConfig.setScaleFactor(23);
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_SIZE_MB);
        retentionConfig.setValue(12345L);
        updateStreamRequest.setScalingPolicy(scalingConfig);
        updateStreamRequest.setRetentionPolicy(retentionConfig);

        streamConfig = getUpdateStreamConfig(updateStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(23, streamConfig.getScalingPolicy().getScaleFactor());
        Assert.assertEquals(1234, streamConfig.getScalingPolicy().getTargetRate());
        Assert.assertEquals(RetentionPolicy.RetentionType.SIZE, streamConfig.getRetentionPolicy().getRetentionType());
        Assert.assertEquals(12345L * 1024 * 1024, streamConfig.getRetentionPolicy().getRetentionParam());
    }

    @Test(timeout = 10000)
    public void testEncodeStreamResponse() {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        StreamProperty streamProperty = encodeStreamResponse("scope", "stream", streamConfig);
        Assert.assertEquals("scope", streamProperty.getScopeName());
        Assert.assertEquals("stream", streamProperty.getStreamName());
        Assert.assertEquals(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS, streamProperty.getScalingPolicy().getType());
        Assert.assertEquals((Integer) 1, streamProperty.getScalingPolicy().getMinSegments());
        Assert.assertNull(streamProperty.getRetentionPolicy());
        Assert.assertEquals((long) streamProperty.getTimestampAggregationTimeout(), 0L);
        Assert.assertEquals((long) streamProperty.getRolloverSizeBytes(), 0L);

        streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byDataRate(100, 200, 1))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(100L)))
                .timestampAggregationTimeout(1000L)
                .rolloverSizeBytes(1024L)
                .build();
        streamProperty = encodeStreamResponse("scope", "stream", streamConfig);
        Assert.assertEquals(ScalingConfig.TypeEnum.BY_RATE_IN_KBYTES_PER_SEC,
                streamProperty.getScalingPolicy().getType());
        Assert.assertEquals((Integer) 1, streamProperty.getScalingPolicy().getMinSegments());
        Assert.assertEquals((Integer) 100, streamProperty.getScalingPolicy().getTargetRate());
        Assert.assertEquals((Integer) 200, streamProperty.getScalingPolicy().getScaleFactor());
        Assert.assertEquals(RetentionConfig.TypeEnum.LIMITED_DAYS,
                streamProperty.getRetentionPolicy().getType());
        Assert.assertEquals((Long) 100L, streamProperty.getRetentionPolicy().getValue());
        Assert.assertEquals((long) streamProperty.getTimestampAggregationTimeout(), 1000L);
        Assert.assertEquals((long) streamProperty.getRolloverSizeBytes(), 1024L);

        streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 200, 1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(1234L * 1024 * 1024))
                .build();
        streamProperty = encodeStreamResponse("scope", "stream", streamConfig);
        Assert.assertEquals(ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC,
                streamProperty.getScalingPolicy().getType());
        Assert.assertEquals((Integer) 1, streamProperty.getScalingPolicy().getMinSegments());
        Assert.assertEquals((Integer) 100, streamProperty.getScalingPolicy().getTargetRate());
        Assert.assertEquals((Integer) 200, streamProperty.getScalingPolicy().getScaleFactor());
        Assert.assertEquals(RetentionConfig.TypeEnum.LIMITED_SIZE_MB, streamProperty.getRetentionPolicy().getType());
        Assert.assertEquals((Long) 1234L, streamProperty.getRetentionPolicy().getValue());
    }
}
