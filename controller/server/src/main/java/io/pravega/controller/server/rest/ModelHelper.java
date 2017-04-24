/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server.rest;

import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.stream.RetentionPolicy;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.StreamConfiguration;

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
                .scalingPolicy(scalingPolicy)
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
                                  .scalingPolicy(scalingPolicy)
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
        if (streamConfiguration.getScalingPolicy().getType() == ScalingPolicy.Type.FIXED_NUM_SEGMENTS) {
            scalingPolicy.setType(ScalingConfig.TypeEnum.valueOf(streamConfiguration.getScalingPolicy().
                    getType().name()));
            scalingPolicy.setMinSegments(streamConfiguration.getScalingPolicy().getMinNumSegments());
        } else {
            scalingPolicy.setType(ScalingConfig.TypeEnum.valueOf(streamConfiguration.getScalingPolicy().
                    getType().name()));
            scalingPolicy.setTargetRate(streamConfiguration.getScalingPolicy().getTargetRate());
            scalingPolicy.setScaleFactor(streamConfiguration.getScalingPolicy().getScaleFactor());
            scalingPolicy.setMinSegments(streamConfiguration.getScalingPolicy().getMinNumSegments());
        }

        RetentionConfig retentionConfig = new RetentionConfig();
        if (streamConfiguration.getRetentionPolicy().getType() == RetentionPolicy.Type.TIME) {
            if (streamConfiguration.getRetentionPolicy().getValue() == Long.MAX_VALUE) {
                retentionConfig.setType(RetentionConfig.TypeEnum.INFINITE);
            } else {
                retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
                retentionConfig.setValue(Duration.ofMillis(streamConfiguration.getRetentionPolicy().getValue()).toDays());
            }
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
