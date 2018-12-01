/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest;

import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;

import java.time.Duration;

/**
 * Provides translation between the Model classes and its REST representation.
 */
public class ModelHelper {

    /**
     * This method translates the REST request object CreateStreamRequest into internal object StreamConfiguration.
     *
     * @param createStreamRequest An object conforming to the createStream REST API json
     * @return StreamConfiguration internal object
     */
    public static final StreamConfiguration getCreateStreamConfig(final CreateStreamRequest createStreamRequest) {
        ScalingPolicy scalingPolicy;
        if (createStreamRequest.getScalingPolicy().getType() == ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS) {
           scalingPolicy = ScalingPolicy.fixed(createStreamRequest.getScalingPolicy().getMinSegments());
        } else if (createStreamRequest.getScalingPolicy().getType() ==
                ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC) {
            scalingPolicy = ScalingPolicy.byEventRate(
                    createStreamRequest.getScalingPolicy().getTargetRate(),
                    createStreamRequest.getScalingPolicy().getScaleFactor(),
                    createStreamRequest.getScalingPolicy().getMinSegments()
            );
        } else {
            scalingPolicy = ScalingPolicy.byDataRate(
                    createStreamRequest.getScalingPolicy().getTargetRate(),
                    createStreamRequest.getScalingPolicy().getScaleFactor(),
                    createStreamRequest.getScalingPolicy().getMinSegments()
            );
        }
        RetentionPolicy retentionPolicy = null;
        if (createStreamRequest.getRetentionPolicy() != null) {
            switch (createStreamRequest.getRetentionPolicy().getType()) {
                case LIMITED_SIZE_MB:
                    retentionPolicy = RetentionPolicy.bySizeBytes(
                            createStreamRequest.getRetentionPolicy().getValue() * 1024 * 1024);
                    break;
                case LIMITED_DAYS:
                    retentionPolicy = RetentionPolicy.byTime(
                            Duration.ofDays(createStreamRequest.getRetentionPolicy().getValue()));
                    break;
            }
        }
        return StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .retentionPolicy(retentionPolicy)
                .build();
    }

    /**
     * This method translates the REST request object UpdateStreamRequest into internal object StreamConfiguration.
     *
     * @param updateStreamRequest An object conforming to the updateStreamConfig REST API json
     * @return StreamConfiguration internal object
     */
    public static final StreamConfiguration getUpdateStreamConfig(final UpdateStreamRequest updateStreamRequest) {
        ScalingPolicy scalingPolicy;
        if (updateStreamRequest.getScalingPolicy().getType() == ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS) {
            scalingPolicy = ScalingPolicy.fixed(updateStreamRequest.getScalingPolicy().getMinSegments());
        } else if (updateStreamRequest.getScalingPolicy().getType() ==
                ScalingConfig.TypeEnum.BY_RATE_IN_EVENTS_PER_SEC) {
            scalingPolicy = ScalingPolicy.byEventRate(
                    updateStreamRequest.getScalingPolicy().getTargetRate(),
                    updateStreamRequest.getScalingPolicy().getScaleFactor(),
                    updateStreamRequest.getScalingPolicy().getMinSegments()
            );
        } else {
            scalingPolicy = ScalingPolicy.byDataRate(
                    updateStreamRequest.getScalingPolicy().getTargetRate(),
                    updateStreamRequest.getScalingPolicy().getScaleFactor(),
                    updateStreamRequest.getScalingPolicy().getMinSegments()
            );
        }
        RetentionPolicy retentionPolicy = null;
        if (updateStreamRequest.getRetentionPolicy() != null) {
            switch (updateStreamRequest.getRetentionPolicy().getType()) {
                case LIMITED_SIZE_MB:
                    retentionPolicy = RetentionPolicy.bySizeBytes(
                            updateStreamRequest.getRetentionPolicy().getValue() * 1024 * 1024);
                    break;
                case LIMITED_DAYS:
                    retentionPolicy = RetentionPolicy.byTime(
                            Duration.ofDays(updateStreamRequest.getRetentionPolicy().getValue()));
                    break;
            }
        }
        return StreamConfiguration.builder()
                                  .scalingPolicy(scalingPolicy)
                                  .retentionPolicy(retentionPolicy)
                                  .build();
    }

    /**
     * The method translates the internal object StreamConfiguration into REST response object.
     *
     * @param scope the scope of the stream
     * @param streamName the name of the stream
     * @param streamConfiguration The configuration of stream
     * @return Stream properties wrapped in StreamResponse object
     */
    public static final StreamProperty encodeStreamResponse(String scope, String streamName, final StreamConfiguration streamConfiguration) {
        ScalingConfig scalingPolicy = new ScalingConfig();
        if (streamConfiguration.getScalingPolicy().getScaleType() == ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS) {
            scalingPolicy.setType(ScalingConfig.TypeEnum.valueOf(streamConfiguration.getScalingPolicy().
                    getScaleType().name()));
            scalingPolicy.setMinSegments(streamConfiguration.getScalingPolicy().getMinNumSegments());
        } else {
            scalingPolicy.setType(ScalingConfig.TypeEnum.valueOf(streamConfiguration.getScalingPolicy().
                    getScaleType().name()));
            scalingPolicy.setTargetRate(streamConfiguration.getScalingPolicy().getTargetRate());
            scalingPolicy.setScaleFactor(streamConfiguration.getScalingPolicy().getScaleFactor());
            scalingPolicy.setMinSegments(streamConfiguration.getScalingPolicy().getMinNumSegments());
        }

        RetentionConfig retentionConfig = null;
        if (streamConfiguration.getRetentionPolicy() != null) {
            retentionConfig = new RetentionConfig();
            switch (streamConfiguration.getRetentionPolicy().getRetentionType()) {
                case SIZE:
                    retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_SIZE_MB);
                    retentionConfig.setValue(streamConfiguration.getRetentionPolicy().getRetentionParam() / (1024 * 1024));
                    break;
                case TIME:
                    retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
                    retentionConfig.setValue(
                            Duration.ofMillis(streamConfiguration.getRetentionPolicy().getRetentionParam()).toDays());
                    break;
            }
        }

        StreamProperty streamProperty = new StreamProperty();
        streamProperty.setScopeName(scope);
        streamProperty.setStreamName(streamName);
        streamProperty.setScalingPolicy(scalingPolicy);
        streamProperty.setRetentionPolicy(retentionConfig);
        return streamProperty;
    }
}
