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

import com.emc.pravega.controller.server.rest.contract.common.RetentionPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.common.ScalingPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.response.StreamProperty;
import com.emc.pravega.controller.server.rest.contract.response.StreamResponse;
import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;

public class ModelHelper {

    public static final StreamConfiguration getCreateStreamConfig(CreateStreamRequest createStreamRequest,
                                                                  String scope) {
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

    public static final StreamConfiguration getUpdateStreamConfig(UpdateStreamRequest updateStreamRequest, String scope,
                                                                  String stream) {
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

    public static final StreamResponse encodeStreamResponse(StreamConfiguration streamConfiguration) {
        return new StreamResponse(new StreamProperty(
                streamConfiguration.getScope(),
                streamConfiguration.getName(),
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
