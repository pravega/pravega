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
        return StreamConfiguration.builder()
                .scope(scope)
                .streamName(createStreamRequest.getStreamName())
                .scalingPolicy(ScalingPolicy.builder()
                               .type(ScalingPolicy.Type.valueOf(createStreamRequest.getScalingPolicy().getType().name()))
                               .targetRate(createStreamRequest.getScalingPolicy().getTargetRate().intValue())
                               .scaleFactor(createStreamRequest.getScalingPolicy().getScaleFactor())
                               .minNumSegments(createStreamRequest.getScalingPolicy().getMinSegments())
                               .build())
                .retentionPolicy(RetentionPolicy.byTimeMillis(createStreamRequest.getRetentionPolicy().getRetentionTimeMillis()))
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
                                  .retentionPolicy(RetentionPolicy.byTimeMillis(updateStreamRequest.getRetentionPolicy()
                                                                                .getRetentionTimeMillis()))
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

        RetentionConfig retentionPolicy = new RetentionConfig();
        if (streamConfiguration.getRetentionPolicy().getType() == RetentionPolicy.Type.TIME) {
            retentionPolicy.setRetentionTimeMillis(streamConfiguration.getRetentionPolicy().getValue());
        } else {
            throw new UnsupportedOperationException("Only time based retention is supported at the moment.");
        }

        StreamProperty streamProperty = new StreamProperty();
        streamProperty.setStreamName(streamConfiguration.getStreamName());
        streamProperty.setScopeName(streamConfiguration.getScope());
        streamProperty.setScalingPolicy(scalingPolicy);
        streamProperty.setRetentionPolicy(retentionPolicy);

        return streamProperty;
    }
}
