/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest;

import com.emc.pravega.controller.server.rest.contract.common.RetentionPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.common.ScalingPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.response.StreamProperty;
import com.emc.pravega.controller.server.rest.contract.response.StreamResponse;
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
                                  .scalingPolicy(new ScalingPolicy(
                                          ScalingPolicy.Type.valueOf(createStreamRequest.getScalingPolicy()
                                                                                        .getType()
                                                                                        .name()),
                                          createStreamRequest.getScalingPolicy().getTargetRate(),
                                          createStreamRequest.getScalingPolicy().getScaleFactor(),
                                          createStreamRequest.getScalingPolicy().getMinNumSegments()))
                                  .retentionPolicy(RetentionPolicy.builder()
                                                                  .retentionTimeMillis(createStreamRequest.getRetentionPolicy()
                                                                                                          .getRetentionTimeMillis())
                                                                  .build())
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
                                  .scalingPolicy(new ScalingPolicy(
                                          ScalingPolicy.Type.valueOf(updateStreamRequest.getScalingPolicy()
                                                                                        .getType()
                                                                                        .name()),
                                          updateStreamRequest.getScalingPolicy().getTargetRate(),
                                          updateStreamRequest.getScalingPolicy().getScaleFactor(),
                                          updateStreamRequest.getScalingPolicy().getMinNumSegments()))
                                  .retentionPolicy(RetentionPolicy.builder()
                                                                  .retentionTimeMillis(updateStreamRequest.getRetentionPolicy()
                                                                                                          .getRetentionTimeMillis())
                                                                  .build())
                                  .build();
    }

    /**
     * The method translates the internal object StreamConfiguration into REST response object.
     *
     * @param streamConfiguration The configuration of stream
     * @return Stream properties wrapped in StreamResponse object
     */
    public static final StreamResponse encodeStreamResponse(final StreamConfiguration streamConfiguration) {
        return new StreamResponse(new StreamProperty(
                streamConfiguration.getScope(),
                streamConfiguration.getStreamName(),
                new ScalingPolicyCommon(
                        ScalingPolicyCommon.Type.valueOf(streamConfiguration.getScalingPolicy().getType().name()),
                        streamConfiguration.getScalingPolicy().getTargetRate(),
                        streamConfiguration.getScalingPolicy().getScaleFactor(),
                        streamConfiguration.getScalingPolicy().getMinNumSegments()
                ),
                new RetentionPolicyCommon(streamConfiguration.getRetentionPolicy().getRetentionTimeMillis())
        ));

    }
}
