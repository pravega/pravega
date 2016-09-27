/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.model;

import java.util.Map;
import java.util.stream.Collectors;

import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.ScalingPolicyType;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.PositionImpl;
import com.google.common.base.Preconditions;

/**
 * Provides translation (encode/decode) between the Model classes and its Thrift representation.
 */
public final class ModelHelper {

    public static final Segment encode(final com.emc.pravega.controller.stream.api.v1.SegmentId segment) {
        Preconditions.checkNotNull(segment, "Segment");
        return new Segment(segment.getScope(), segment.getStreamName(), segment.getNumber(), segment.getPrevious());
    }

    public static final ScalingPolicy encode(final com.emc.pravega.controller.stream.api.v1.ScalingPolicy policy) {
        Preconditions.checkNotNull(policy, "ScalingPolicy");
        return new ScalingPolicy(ScalingPolicy.Type.valueOf(policy.getType().name()), policy.getTargetRate(), policy.getScaleFactor(),
                policy.getMinNumSegments());
    }

    public static final StreamConfiguration encode(final com.emc.pravega.controller.stream.api.v1.StreamConfig config) {
        Preconditions.checkNotNull(config, "StreamConfig");
        return new StreamConfiguration() {
            @Override
            public String getName() {
                return config.getName();
            }

            @Override
            public ScalingPolicy getScalingingPolicy() {
                return encode(config.getPolicy());
            }
        };
    }

    public static final PositionImpl encode(final com.emc.pravega.controller.stream.api.v1.Position position) {
        Preconditions.checkNotNull(position, "Position");
        return new PositionImpl(encodeLogMap(position.getOwnedLogs()), encodeLogMap(position.getFutureOwnedLogs()));
    }

    public static com.emc.pravega.common.netty.PravegaNodeUri encode(NodeUri uri) {
        return new com.emc.pravega.common.netty.PravegaNodeUri(uri.getEndpoint(), uri.getPort());
    }

    public static final com.emc.pravega.controller.stream.api.v1.SegmentId decode(final Segment segmentId) {
        Preconditions.checkNotNull(segmentId, "Segment");
        return new com.emc.pravega.controller.stream.api.v1.SegmentId().setScope(segmentId.getScope()).setStreamName(segmentId.getStreamName())
                .setNumber(segmentId.getSegmentNumber()).setPrevious(segmentId.getPreviousNumber());

    }

    public static final com.emc.pravega.controller.stream.api.v1.ScalingPolicy decode(final ScalingPolicy policyModel) {
        Preconditions.checkNotNull(policyModel, "Policy");
        return new com.emc.pravega.controller.stream.api.v1.ScalingPolicy()
                .setType(ScalingPolicyType.valueOf(policyModel.getType().name())).setTargetRate(policyModel.getTargetRate())
                .setScaleFactor(policyModel.getScaleFactor()).setMinNumSegments(policyModel.getMinNumSegments());
    }

    public static final com.emc.pravega.controller.stream.api.v1.StreamConfig decode(final StreamConfiguration configModel) {
        Preconditions.checkNotNull(configModel, "StreamConfiguration");
        return new StreamConfig(configModel.getName(), decode(configModel.getScalingingPolicy()));
    }

    public static final com.emc.pravega.controller.stream.api.v1.Position decode(final PositionInternal position) {
        Preconditions.checkNotNull(position, "Position");
        return new com.emc.pravega.controller.stream.api.v1.Position(decodeLogMap(position.asInternalImpl().getOwnedLogs()),
                decodeLogMap(position.asInternalImpl().getFutureOwnedLogs()));
    }

    public static NodeUri decode(com.emc.pravega.common.netty.PravegaNodeUri uri) {
        return new NodeUri(uri.getEndpoint(), uri.getPort());
    }

    private static Map<Segment, Long> encodeLogMap(final Map<com.emc.pravega.controller.stream.api.v1.SegmentId, Long> map) {
        Preconditions.checkNotNull(map);
        return map.entrySet().stream().collect(Collectors.toMap(e -> encode(e.getKey()), Map.Entry::getValue));
    }

    private static Map<com.emc.pravega.controller.stream.api.v1.SegmentId, Long> decodeLogMap(final Map<Segment, Long> map) {
        Preconditions.checkNotNull(map);
        return map.entrySet().stream().collect(Collectors.toMap(e -> decode(e.getKey()), Map.Entry::getValue));
    }
}
