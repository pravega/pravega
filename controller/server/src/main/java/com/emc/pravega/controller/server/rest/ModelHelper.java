/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rest;

import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.RetentionConfig;
import com.emc.pravega.controller.server.rest.generated.model.ScalingConfig;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;

import java.time.Duration;

/**
 * Provides translation between the Model classes and its REST representation.
 */
public class ModelHelper {

    /**
     * This method translates the REST request object CreateStreamRequest into internal object StreamConfiguration.
     *
     * @param createStreamRequest An object conforming to the createStream REST API json
     * @param scope               The scope of stream
     * @return StreamConfiguration internal object
     */
    public static final StreamConfiguration getCreateStreamConfig(final CreateStreamRequest createStreamRequest,
                                                                  final String scope) {
        RetentionPolicy retentionPolicy;
        if (createStreamRequest.getRetentionPolicy().getType() == RetentionConfig.TypeEnum.INFINITE) {
            retentionPolicy = RetentionPolicy.INFINITE;
        } else if (createStreamRequest.getRetentionPolicy().getType() == RetentionConfig.TypeEnum.LIMITED_DAYS) {
            retentionPolicy = RetentionPolicy.byTime(Duration.ofDays(createStreamRequest.getRetentionPolicy().getValue()));
        } else {
            retentionPolicy = RetentionPolicy.bySizeBytes(createStreamRequest.getRetentionPolicy().getValue());
        }

        return StreamConfiguration.builder()
                .scope(scope)
                .streamName(createStreamRequest.getStreamName())
                .scalingPolicy(ScalingPolicy.builder()
                               .type(ScalingPolicy.Type.valueOf(createStreamRequest.getScalingPolicy().getType().name()))
                               .targetRate(createStreamRequest.getScalingPolicy().getTargetRate().intValue())
                               .scaleFactor(createStreamRequest.getScalingPolicy().getScaleFactor())
                               .minNumSegments(createStreamRequest.getScalingPolicy().getMinSegments())
                               .build())
                .retentionPolicy(retentionPolicy)
                .build();
    }

    /**
     * This method translates the REST request object UpdateStreamRequest into internal object StreamConfiguration.
     *
     * @param updateStreamRequest An object conforming to the updateStreamConfig REST API json
     * @param scope               The scope of stream
     * @param stream              The name of stream
     * @return StreamConfiguration internal object
     */
    public static final StreamConfiguration getUpdateStreamConfig(final UpdateStreamRequest updateStreamRequest,
                                                                  final String scope, final String stream) {
        RetentionPolicy retentionPolicy;
        if (updateStreamRequest.getRetentionPolicy().getType() == RetentionConfig.TypeEnum.INFINITE) {
            retentionPolicy = RetentionPolicy.INFINITE;
        } else if (updateStreamRequest.getRetentionPolicy().getType() == RetentionConfig.TypeEnum.LIMITED_DAYS) {
            retentionPolicy = RetentionPolicy.byTime(Duration.ofDays(updateStreamRequest.getRetentionPolicy().getValue()));
        } else {
            retentionPolicy = RetentionPolicy.bySizeBytes(updateStreamRequest.getRetentionPolicy().getValue());
        }

        return StreamConfiguration.builder()
                                  .scope(scope)
                                  .streamName(stream)
                                  .scalingPolicy(ScalingPolicy.builder().type(
                                          ScalingPolicy.Type.valueOf(updateStreamRequest.getScalingPolicy()
                                                                                        .getType()
                                                                                        .name())).targetRate(
                                          updateStreamRequest.getScalingPolicy().getTargetRate().intValue()).scaleFactor(
                                          updateStreamRequest.getScalingPolicy().getScaleFactor()).minNumSegments(
                                          updateStreamRequest.getScalingPolicy().getMinSegments()).build())
                                  .retentionPolicy(retentionPolicy)
                                  .build();
    }

    /**
     * The method translates the internal object StreamConfiguration into REST response object.
     *
     * @param streamConfiguration The configuration of stream
     * @return Stream properties wrapped in StreamResponse object
     */
    public static final StreamProperty encodeStreamResponse(final StreamConfiguration streamConfiguration) {

        ScalingConfig scalingPolicy = new ScalingConfig();
        scalingPolicy.setType(ScalingConfig.TypeEnum.valueOf(streamConfiguration.getScalingPolicy().getType().name()));
        scalingPolicy.setTargetRate((long) streamConfiguration.getScalingPolicy().getTargetRate());
        scalingPolicy.setScaleFactor(streamConfiguration.getScalingPolicy().getScaleFactor());
        scalingPolicy.setMinSegments(streamConfiguration.getScalingPolicy().getMinNumSegments());

        RetentionConfig retentionConfig = new RetentionConfig();
        if (streamConfiguration.getRetentionPolicy().getType() == RetentionPolicy.Type.TIME) {
            if (streamConfiguration.getRetentionPolicy().getValue() == Long.MAX_VALUE) {
                retentionConfig.setType(RetentionConfig.TypeEnum.INFINITE);
            } else {
                retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
            }
            retentionConfig.setValue(Duration.ofMillis(streamConfiguration.getRetentionPolicy().getValue()).toDays());
        } else {
            retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_SIZE_MB);
            retentionConfig.setValue(streamConfiguration.getRetentionPolicy().getValue());
        }

        StreamProperty streamProperty = new StreamProperty();
        streamProperty.setStreamName(streamConfiguration.getStreamName());
        streamProperty.setScopeName(streamConfiguration.getScope());
        streamProperty.setScalingPolicy(scalingPolicy);
        streamProperty.setRetentionPolicy(retentionConfig);

        return streamProperty;
    }
}
