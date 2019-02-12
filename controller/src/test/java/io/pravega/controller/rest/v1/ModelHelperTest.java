/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.rest.v1;

import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.pravega.controller.server.rest.ModelHelper.encodeStreamResponse;
import static io.pravega.controller.server.rest.ModelHelper.getCreateStreamConfig;
import static io.pravega.controller.server.rest.ModelHelper.getUpdateStreamConfig;

/**
 * Test cases for rest/ModelHelper.java.
 */
public class ModelHelperTest {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    @Test
    public void testGetCreateStreamConfig() {
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingConfig.setMinSegments(2);
        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName("stream");
        createStreamRequest.setScalingPolicy(scalingConfig);

        StreamConfiguration streamConfig = getCreateStreamConfig(createStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(2, streamConfig.getScalingPolicy().getMinNumSegments());
        Assert.assertNull(streamConfig.getRetentionPolicy());

        scalingConfig.setType(ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC);
        scalingConfig.setTargetRate(123);
        scalingConfig.setScaleFactor(2);
        RetentionConfig retentionConfig = new RetentionConfig();
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

        scalingConfig.setType(ScalingConfig.TypeEnum.BY_RATE_IN_KBYTES_PER_SEC);
        scalingConfig.setTargetRate(1234);
        scalingConfig.setScaleFactor(23);
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
    }

    @Test
    public void testGetUpdateStreamConfig() {
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingConfig.setMinSegments(2);
        UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
        updateStreamRequest.setScalingPolicy(scalingConfig);

        StreamConfiguration streamConfig = getUpdateStreamConfig(updateStreamRequest);
        Assert.assertEquals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS, streamConfig.getScalingPolicy().getScaleType());
        Assert.assertEquals(2, streamConfig.getScalingPolicy().getMinNumSegments());
        Assert.assertNull(streamConfig.getRetentionPolicy());

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

    @Test
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

        streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byDataRate(100, 200, 1))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofDays(100L)))
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
