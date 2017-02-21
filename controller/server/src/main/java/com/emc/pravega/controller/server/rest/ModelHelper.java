/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
import com.emc.pravega.stream.impl.StreamConfigurationImpl;

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
        return new StreamConfigurationImpl(
                scope,
                createStreamRequest.getStreamName(),
                new ScalingPolicy(
                        ScalingPolicy.Type.valueOf(createStreamRequest.getScalingPolicy().getType().name()),
                        createStreamRequest.getScalingPolicy().getTargetRate(),
                        createStreamRequest.getScalingPolicy().getScaleFactor(),
                        createStreamRequest.getScalingPolicy().getMinSegments()),
                new RetentionPolicy(createStreamRequest.getRetentionPolicy().getRetentionTimeMillis())
        );
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
        return new StreamConfigurationImpl(
                scope,
                stream,
                new ScalingPolicy(
                        ScalingPolicy.Type.valueOf(updateStreamRequest.getScalingPolicy().getType().name()),
                        updateStreamRequest.getScalingPolicy().getTargetRate(),
                        updateStreamRequest.getScalingPolicy().getScaleFactor(),
                        updateStreamRequest.getScalingPolicy().getMinSegments()),
                new RetentionPolicy(updateStreamRequest.getRetentionPolicy().getRetentionTimeMillis())
        );
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
        scalingPolicy.setTargetRate(streamConfiguration.getScalingPolicy().getTargetRate());
        scalingPolicy.setScaleFactor(streamConfiguration.getScalingPolicy().getScaleFactor());
        scalingPolicy.setMinSegments(streamConfiguration.getScalingPolicy().getMinNumSegments());

        RetentionConfig retentionPolicy = new RetentionConfig();
        retentionPolicy.setRetentionTimeMillis(streamConfiguration.getRetentionPolicy().getRetentionTimeMillis());

        StreamProperty streamProperty = new StreamProperty();
        streamProperty.setStreamName(streamConfiguration.getName());
        streamProperty.setScopeName(streamConfiguration.getScope());
        streamProperty.setScalingPolicy(scalingPolicy);
        streamProperty.setRetentionPolicy(retentionPolicy);

        return streamProperty;
    }
}
