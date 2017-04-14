/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.common.Exceptions;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.client.stream.RetentionPolicy;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides translation (encode/decode) between the Model classes and its gRPC representation.
 */
public final class ModelHelper {

    /**
     * Returns UUID of transaction with given TxnId.
     *
     * @param txnId The Transaction Id.
     * @return UUID of the transaction.
     */
    public static final UUID encode(final TxnId txnId) {
        Preconditions.checkNotNull(txnId, "txnId");
        return new UUID(txnId.getHighBits(), txnId.getLowBits());
    }

    /**
     * Helper to convert Segment Id into Segment object.
     *
     * @param segment The Segment Id.
     * @return New instance of Segment.
     */
    public static final Segment encode(final SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");
        return new Segment(segment.getStreamInfo().getScope(),
                           segment.getStreamInfo().getStream(),
                           segment.getSegmentNumber());
    }

    public static final ScalingPolicy encode(final Controller.ScalingPolicy policy) {
        Preconditions.checkNotNull(policy, "policy");
        return ScalingPolicy.builder()
                            .type(ScalingPolicy.Type.valueOf(policy.getType().name()))
                            .targetRate(policy.getTargetRate())
                            .scaleFactor(policy.getScaleFactor())
                            .minNumSegments(policy.getMinNumSegments())
                            .build();
    }

    /**
     * Helper to convert retention policy from RPC call to internal representation.
     *
     * @param policy The retention policy from RPC interface.
     * @return New instance of RetentionPolicy.
     */
    public static final RetentionPolicy encode(final Controller.RetentionPolicy policy) {
        Preconditions.checkNotNull(policy, "policy");
        switch (policy.getType()) {
            case INFINITE:
                return RetentionPolicy.INFINITE;
            case LIMITED_SIZE_MB:
                return RetentionPolicy.builder()
                        .type(RetentionPolicy.Type.SIZE)
                        .value(policy.getValue())
                        .build();
            case LIMITED_DAYS:
                return RetentionPolicy.builder()
                        .type(RetentionPolicy.Type.TIME)
                        .value(Duration.ofDays(policy.getValue()).toMillis())
                        .build();
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Helper to convert StreamConfig into Stream Configuration Impl.
     *
     * @param config The StreamConfig
     * @return New instance of StreamConfiguration Impl.
     */
    public static final StreamConfiguration encode(final StreamConfig config) {
        Preconditions.checkNotNull(config, "config");
        return StreamConfiguration.builder()
                .scope(config.getStreamInfo().getScope())
                .streamName(config.getStreamInfo().getStream())
                .scalingPolicy(encode(config.getScalingPolicy()))
                .retentionPolicy(encode(config.getRetentionPolicy()))
                .build();
    }

    /**
     * Helper to convert NodeURI into PravegaNodeURI.
     *
     * @param uri Node URI.
     * @return PravegaNodeURI.
     */
    public static final PravegaNodeUri encode(final NodeUri uri) {
        Preconditions.checkNotNull(uri, "uri");
        return new PravegaNodeUri(uri.getEndpoint(), uri.getPort());
    }

    /**
     * Return list of key ranges available.
     *
     * @param keyRanges List of Key Value pairs.
     * @return Collection of key ranges available.
     */
    public static final List<AbstractMap.SimpleEntry<Double, Double>> encode(final Map<Double, Double> keyRanges) {
        Preconditions.checkNotNull(keyRanges, "keyRanges");

        return keyRanges
                .entrySet()
                .stream()
                .map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Returns actual status of given transaction status instance.
     *
     * @param state     TxnState object instance.
     * @param logString Description text to be logged when transaction status is invalid.
     * @return Transaction.Status
     */
    public static final Transaction.Status encode(final TxnState.State state, final String logString) {
        Preconditions.checkNotNull(state, "state");
        Exceptions.checkNotNullOrEmpty(logString, "logString");

        Transaction.Status result;
        switch (state) {
            case COMMITTED:
                result = Transaction.Status.COMMITTED;
                break;
            case ABORTED:
                result = Transaction.Status.ABORTED;
                break;
            case OPEN:
                result = Transaction.Status.OPEN;
                break;
            case ABORTING:
                result = Transaction.Status.ABORTING;
                break;
            case COMMITTING:
                result = Transaction.Status.COMMITTING;
                break;
            case UNKNOWN:
                throw new RuntimeException("Unknown transaction: " + logString);
            case UNRECOGNIZED:
            default:
                throw new IllegalStateException("Unknown status: " + state);
        }
        return result;
    }

    /**
     * Helper to convert SegmentRange to SegmentWithRange.
     *
     * @param segmentRange segmentRange
     * @return SegmentWithRange
     */
    public static final SegmentWithRange encode(final SegmentRange segmentRange) {
        return new SegmentWithRange(encode(segmentRange.getSegmentId()), segmentRange.getMinKey(), segmentRange
                .getMaxKey());
    }

    /**
     * Returns TxnId object instance for a given transaction with UUID.
     *
     * @param txnId UUID
     * @return Instance of TxnId.
     */
    public static final TxnId decode(final UUID txnId) {
        Preconditions.checkNotNull(txnId, "txnId");
        return TxnId.newBuilder()
                .setHighBits(txnId.getMostSignificantBits())
                .setLowBits(txnId.getLeastSignificantBits())
                .build();
    }

    /**
     * Decodes segment and returns an instance of SegmentId.
     *
     * @param segment The segment.
     * @return Instance of SegmentId.
     */
    public static final SegmentId decode(final Segment segment) {
        Preconditions.checkNotNull(segment, "segment");
        return createSegmentId(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber());
    }

    /**
     * Decodes ScalingPolicy and returns an instance of Scaling Policy impl.
     *
     * @param policyModel The Scaling Policy.
     * @return Instance of Scaling Policy Impl.
     */
    public static final Controller.ScalingPolicy decode(final ScalingPolicy policyModel) {
        Preconditions.checkNotNull(policyModel, "policyModel");
        return Controller.ScalingPolicy.newBuilder()
                .setType(Controller.ScalingPolicy.ScalingPolicyType.valueOf(policyModel.getType().name()))
                .setTargetRate(policyModel.getTargetRate())
                .setScaleFactor(policyModel.getScaleFactor())
                .setMinNumSegments(policyModel.getMinNumSegments())
                .build();
    }

    /**
     * Decodes RetentionPolicy and returns an instance of Retention Policy impl.
     *
     * @param policyModel The Retention Policy.
     * @return Instance of Retention Policy Impl.
     */
    public static final Controller.RetentionPolicy decode(final RetentionPolicy policyModel) {
        Preconditions.checkNotNull(policyModel, "policyModel");
        if (policyModel.getType() == RetentionPolicy.Type.TIME && policyModel.getValue() == Long.MAX_VALUE) {
            return Controller.RetentionPolicy.newBuilder()
                    .setType(Controller.RetentionPolicy.RetentionPolicyType.INFINITE).build();
        }
        switch (policyModel.getType()) {
            case SIZE:
                return Controller.RetentionPolicy.newBuilder()
                        .setType(Controller.RetentionPolicy.RetentionPolicyType.LIMITED_SIZE_MB)
                        .setValue(policyModel.getValue())
                        .build();
            case TIME:
                return Controller.RetentionPolicy.newBuilder()
                        .setType(Controller.RetentionPolicy.RetentionPolicyType.LIMITED_DAYS)
                        .setValue(Duration.ofMillis(policyModel.getValue()).toDays())
                        .build();
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Converts StreamConfiguration into StreamConfig.
     *
     * @param configModel The stream configuration.
     * @return StreamConfig instance.
     */
    public static final StreamConfig decode(final StreamConfiguration configModel) {
        Preconditions.checkNotNull(configModel, "configModel");
        return StreamConfig.newBuilder()
                .setStreamInfo(createStreamInfo(configModel.getScope(), configModel.getStreamName()))
                .setScalingPolicy(decode(configModel.getScalingPolicy()))
                .setRetentionPolicy(decode(configModel.getRetentionPolicy()))
                .build();
    }

    /**
     * Converts PravegaNodeURI into NodeURI.
     *
     * @param uri The PravegaNodeURI string.
     * @return Node URI string.
     */
    public static final NodeUri decode(final PravegaNodeUri uri) {
        Preconditions.checkNotNull(uri, "uri");
        return NodeUri.newBuilder().setEndpoint(uri.getEndpoint()).setPort(uri.getPort()).build();
    }
    
    public static final Set<Integer> getSegmentsFromPositions(final List<PositionInternal> positions) {
        Preconditions.checkNotNull(positions, "positions");
        return positions.stream()
            .flatMap(position -> position.getCompletedSegments().stream().map(Segment::getSegmentNumber))
            .collect(Collectors.toSet());
    }
    
    public static final Map<Integer, Long> toSegmentOffsetMap(final PositionInternal position) {
        Preconditions.checkNotNull(position, "position");
        return position.getOwnedSegmentsWithOffsets()
            .entrySet()
            .stream()
            .map(e -> new SimpleEntry<>(e.getKey().getSegmentNumber(), e.getValue()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    public static final Controller.ScopeInfo createScopeInfo(final String scope) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        return Controller.ScopeInfo.newBuilder().setScope(scope).build();
    }

    public static final StreamInfo createStreamInfo(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return StreamInfo.newBuilder().setScope(scope).setStream(stream).build();
    }

    public static final SegmentId createSegmentId(final String scope, final String stream, final int segmentNumber) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return SegmentId.newBuilder()
                .setStreamInfo(createStreamInfo(scope, stream))
                .setSegmentNumber(segmentNumber)
                .build();
    }

    public static final SegmentRange createSegmentRange(final String scope, final String stream,
            final int segmentNumber, final double rangeMinKey, final double rangeMaxKey) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return SegmentRange.newBuilder()
                .setSegmentId(createSegmentId(scope, stream, segmentNumber))
                .setMinKey(rangeMinKey)
                .setMaxKey(rangeMaxKey)
                .build();
    }

    public static final SuccessorResponse createSuccessorResponse(Map<SegmentRange, List<Integer>> segments) {
        Preconditions.checkNotNull(segments);
        return SuccessorResponse.newBuilder()
                .addAllSegments(
                        segments.entrySet().stream().map(
                                segmentRangeListEntry -> SuccessorResponse.SegmentEntry.newBuilder()
                                        .setSegment(segmentRangeListEntry.getKey())
                                        .addAllValue(segmentRangeListEntry.getValue())
                                        .build())
                                .collect(Collectors.toList()))
                .build();
    }

}
