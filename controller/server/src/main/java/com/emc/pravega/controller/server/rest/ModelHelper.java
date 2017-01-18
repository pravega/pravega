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

import com.emc.pravega.controller.server.rest.contract.request.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.contract.request.UpdateStreamRequest;
import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;

public class ModelHelper {

    public static StreamConfigurationImpl getCreateStreamConfig(CreateStreamRequest createStreamRequest, String scope) {
        String streamName = createStreamRequest.getStreamName();
        ScalingPolicy.Type type = ScalingPolicy.Type.values()[createStreamRequest.getScalingPolicy().getType().ordinal()];
        long targetRate = createStreamRequest.getScalingPolicy().getTargetRate();
        int  scaleFactor = createStreamRequest.getScalingPolicy().getScaleFactor();
        int minNumSegments = createStreamRequest.getScalingPolicy().getMinNumSegments();
        long retentionTimeMillis = createStreamRequest.getRetentionPolicy().getRetentionTimeMillis();

        ScalingPolicy scalingPolicy = new ScalingPolicy(type,
                targetRate,
                scaleFactor,
                minNumSegments
                );

        RetentionPolicy retentionPolicy = new RetentionPolicy(retentionTimeMillis);
        return new StreamConfigurationImpl(scope, streamName, scalingPolicy, retentionPolicy);
    }

    public static StreamConfigurationImpl getUpdateStreamConfig(UpdateStreamRequest updateStreamRequest, String scope, String stream) {
        String streamName = stream;
        ScalingPolicy.Type type = ScalingPolicy.Type.values()[updateStreamRequest.getScalingPolicy().getType().ordinal()];
        long targetRate = updateStreamRequest.getScalingPolicy().getTargetRate();
        int  scaleFactor = updateStreamRequest.getScalingPolicy().getScaleFactor();
        int minNumSegments = updateStreamRequest.getScalingPolicy().getMinNumSegments();
        long retentionTimeMillis = updateStreamRequest.getRetentionPolicy().getRetentionTimeMillis();

        ScalingPolicy scalingPolicy = new ScalingPolicy(type,
                targetRate,
                scaleFactor,
                minNumSegments
        );

        RetentionPolicy retentionPolicy = new RetentionPolicy(retentionTimeMillis);
        return new StreamConfigurationImpl(scope, streamName, scalingPolicy, retentionPolicy);
    }

}
