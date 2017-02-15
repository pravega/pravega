/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.server.rest;

import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
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
                        createStreamRequest.getScalingPolicy().getMinNumSegments()),
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
                        updateStreamRequest.getScalingPolicy().getMinNumSegments()),
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

        com.emc.pravega.controller.server.rest.generated.model.ScalingPolicy scalingPolicy =
                new com.emc.pravega.controller.server.rest.generated.model.ScalingPolicy();
        scalingPolicy.setType(com.emc.pravega.controller.server.rest.generated.model.ScalingPolicy.TypeEnum.valueOf(
                streamConfiguration.getScalingPolicy().getType().name()));
        scalingPolicy.setTargetRate(streamConfiguration.getScalingPolicy().getTargetRate());
        scalingPolicy.setScaleFactor(streamConfiguration.getScalingPolicy().getScaleFactor());
        scalingPolicy.setMinNumSegments(streamConfiguration.getScalingPolicy().getMinNumSegments());

        com.emc.pravega.controller.server.rest.generated.model.RetentionPolicy retentionPolicy =
                new com.emc.pravega.controller.server.rest.generated.model.RetentionPolicy();
        retentionPolicy.setRetentionTimeMillis(streamConfiguration.getRetentionPolicy().getRetentionTimeMillis());

        StreamProperty streamProperty = new StreamProperty();
        streamProperty.setName(streamConfiguration.getName());
        streamProperty.setScope(streamConfiguration.getScope());
        streamProperty.setScalingPolicy(scalingPolicy);
        streamProperty.setRetentionPolicy(retentionPolicy);

        return streamProperty;
    }
}
