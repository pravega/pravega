/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.control.impl;

import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.WriterPosition;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamCut;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamSubscriberInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscriberStreamCut;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides translation (encode/decode) between the Model classes and its gRPC representation.
 * 
 * NOTE: For some unknown reason all methods that encode data to go over the wire are called
 * "decode" and all method that take the wire format an instantiate java objects are called "encode".
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
                segment.getSegmentId());
    }

    public static final ScalingPolicy encode(final Controller.ScalingPolicy policy) {
        Preconditions.checkNotNull(policy, "policy");
        return ScalingPolicy.builder()
                            .scaleType(ScalingPolicy.ScaleType.valueOf(policy.getScaleType().name()))
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
        // Using default enum type of UNKNOWN(0) to detect if retention policy has been set or not.
        // This is required since proto3 does not have any other way to detect if a field has been set or not.
        if (policy != null && policy.getRetentionType() != Controller.RetentionPolicy.RetentionPolicyType.UNKNOWN) {
            return RetentionPolicy.builder()
                    .retentionType(RetentionPolicy.RetentionType.valueOf(policy.getRetentionType().name()))
                    .retentionParam(policy.getRetentionParam())
                    .build();
        } else {
            return null;
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
                .scalingPolicy(encode(config.getScalingPolicy()))
                .retentionPolicy(encode(config.getRetentionPolicy()))
                .build();
    }

    /**
     * Helper to convert KeyValueTableConfig object into KeyValueTableConfiguration Impl.
     *
     * @param config The KeyValueTable Config
     * @return New instance of KeyValueTableConfiguration Impl.
     */
    public static final KeyValueTableConfiguration encode(final KeyValueTableConfig config) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(config.getScope(), "scope");
        Preconditions.checkNotNull(config.getKvtName(), "kvtName");
        Preconditions.checkArgument(config.getPartitionCount() > 0, "Number of partitions should be > 0.");
        return KeyValueTableConfiguration.builder().partitionCount(config.getPartitionCount()).build();
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
    public static final List<Map.Entry<Double, Double>> encode(final Map<Double, Double> keyRanges) {
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
     * Returns the status of Ping Transaction.
     *
     * @param status     PingTxnStatus object instance.
     * @param logString Description text to be logged when ping transaction status is invalid.
     * @return Transaction.PingStatus
     * @throws PingFailedException if status of Ping transaction operations is not successful.
     */
    public static final Transaction.PingStatus encode(final Controller.PingTxnStatus.Status status, final String logString)
            throws PingFailedException {
        Preconditions.checkNotNull(status, "status");
        Exceptions.checkNotNullOrEmpty(logString, "logString");
        Transaction.PingStatus result;
        switch (status) {
            case OK:
                result = Transaction.PingStatus.OPEN;
                break;
            case COMMITTED:
                result = Transaction.PingStatus.COMMITTED;
                break;
            case ABORTED:
                result = Transaction.PingStatus.ABORTED;
                break;
            case UNKNOWN:
                throw new StatusRuntimeException(Status.NOT_FOUND);
            default:
                throw new PingFailedException("Ping transaction for " + logString + " failed with status " + status);
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
     * Helper method to convery stream cut to map of segment to position.
     * @param streamCut Stream cut
     * @return map of segment to position
     */
    public static Map<Long, Long> encode(Controller.StreamCut streamCut) {
        return streamCut.getCutMap();
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
        return createSegmentId(segment.getScope(), segment.getStreamName(), segment.getSegmentId());
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
                .setScaleType(Controller.ScalingPolicy.ScalingPolicyType.valueOf(policyModel.getScaleType().name()))
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
        if (policyModel != null) {
            return Controller.RetentionPolicy.newBuilder()
                    .setRetentionType(Controller.RetentionPolicy.RetentionPolicyType.valueOf(policyModel.getRetentionType().name()))
                    .setRetentionParam(policyModel.getRetentionParam())
                    .build();
        } else {
            return null;
        }
    }

    /**
     * Converts StreamConfiguration into StreamConfig.
     * 
     * @param scope the stream's scope 
     * @param streamName The Stream Name
     * @param configModel The stream configuration.
     * @return StreamConfig instance.
     */
    public static final StreamConfig decode(String scope, String streamName, final StreamConfiguration configModel) {
        Preconditions.checkNotNull(configModel, "configModel");
        final StreamConfig.Builder builder = StreamConfig.newBuilder()
                .setStreamInfo(createStreamInfo(scope, streamName))
                .setScalingPolicy(decode(configModel.getScalingPolicy()));
        if (configModel.getRetentionPolicy() != null) {
            builder.setRetentionPolicy(decode(configModel.getRetentionPolicy()));
        }
        return builder.build();
    }

    /**
     * Converts StreamConfiguration into StreamConfig.
     *
     * @param scope the stream's scope
     * @param streamName The Stream Name
     * @param subscriberId Id of the subscriber for this stream.
     * @return StreamSubscriberInfo instance.
     */
    public static final StreamSubscriberInfo decode(String scope, String streamName, final String subscriberId) {
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamName, "streamName");
        Preconditions.checkNotNull(subscriberId, "subscriber");
        final StreamSubscriberInfo.Builder builder = StreamSubscriberInfo.newBuilder()
                .setScope(scope).setStream(streamName).setSubscriber(subscriberId);
        return builder.build();
    }

    /**
     * Converts StreamConfiguration into StreamConfig.
     *
     * @param scope the stream's scope
     * @param streamName The Stream Name
     * @param subscriber subscriber for this stream.
     * @param streamCut truncationStreamCut for this subscriber for this stream.
     * @return SubscriberStreamCut instance.
     */
    public static final SubscriberStreamCut decode(String scope, String streamName,
                                                   final String subscriber, Map<Long, Long> streamCut) {
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamName, "streamName");
        Preconditions.checkNotNull(subscriber, "subscriber");
        Preconditions.checkNotNull(streamCut, "streamCut");
        final SubscriberStreamCut.Builder builder = SubscriberStreamCut.newBuilder()
                .setSubscriber(subscriber).setStreamCut(decode(scope, streamName, streamCut));
        return builder.build();
    }

    /**
     * Helper to convert KeyValueTableConfiguration object into KeyValueTableConfig Impl.
     *
     * @param scopeName Name for scope for KVTable.
     * @param kvtName KeyValueTable Name.
     * @param config The KeyValueTable Configuration object.
     * @return New instance of KeyValueTableConfig.
     */
    public static final KeyValueTableConfig decode(String scopeName, String kvtName, final KeyValueTableConfiguration config) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(scopeName, "scopeName");
        Preconditions.checkNotNull(kvtName, "kvtName");
        Preconditions.checkArgument(config.getPartitionCount() > 0, "Number of partitions should be > 0.");
        return KeyValueTableConfig.newBuilder().setScope(scopeName)
                .setKvtName(kvtName).setPartitionCount(config.getPartitionCount()).build();
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

    /**
     * Creates a stream cut object.
     *
     * @param scope     scope
     * @param stream    stream
     * @param streamCut map of segment to position
     * @return stream cut
     */
    public static Controller.StreamCut decode(final String scope, final String stream, Map<Long, Long> streamCut) {
        return Controller.StreamCut.newBuilder().setStreamInfo(createStreamInfo(scope, stream)).putAllCut(streamCut).build();
    }

    public static Controller.StreamCutRange decode(final String scope, final String stream, Map<Long, Long> from, Map<Long, Long> to) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        return Controller.StreamCutRange.newBuilder().setStreamInfo(createStreamInfo(scope, stream)).putAllFrom(from)
                .putAllTo(to).build();
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

    public static final KeyValueTableInfo createKeyValueTableInfo(final String scope, final String kvtName) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTable");
        return KeyValueTableInfo.newBuilder().setScope(scope).setKvtName(kvtName).build();
    }

    public static final SegmentId createSegmentId(final String scope, final String stream, final long segmentId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return SegmentId.newBuilder()
                .setStreamInfo(createStreamInfo(scope, stream))
                .setSegmentId(segmentId)
                .build();
    }

    public static final SegmentRange createSegmentRange(final String scope, final String stream,
            final long segmentId, final double rangeMinKey, final double rangeMaxKey) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return SegmentRange.newBuilder()
                .setSegmentId(createSegmentId(scope, stream, segmentId))
                .setMinKey(rangeMinKey)
                .setMaxKey(rangeMaxKey)
                .build();
    }

    public static final Controller.StreamCutRangeResponse createStreamCutRangeResponse(final String scope, final String stream,
                                                                                       final List<SegmentId> segments, String delegationToken) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Exceptions.checkArgument(segments.stream().allMatch(x -> x.getStreamInfo().getScope().equals(scope) &&
                        x.getStreamInfo().getStream().equals(stream)),
                "streamInfo", "stream info does not match segment id", scope, stream, segments);
        return Controller.StreamCutRangeResponse.newBuilder()
                .addAllSegments(segments)
                .setDelegationToken(delegationToken)
                .build();
    }

    /**
     * Builds a stream cut, mapping the segments of a stream to their offsets from a writer position object.
     * 
     * @param stream The stream the cut is on.
     * @param position The position object to take the offsets from.
     * @return a StreamCut.
     */
    public static StreamCut createStreamCut(Stream stream, WriterPosition position) {
        StreamCut.Builder builder = StreamCut.newBuilder().setStreamInfo(createStreamInfo(stream.getScope(), stream.getStreamName()));
        for (Entry<Segment, Long> entry : position.getSegmentsWithOffsets().entrySet()) {
            builder.putCut(entry.getKey().getSegmentId(), entry.getValue());
        }
        return builder.build();
    }

    public static final SuccessorResponse.Builder createSuccessorResponse(Map<SegmentRange, List<Long>> segments) {
        Preconditions.checkNotNull(segments);
        return SuccessorResponse.newBuilder()
                .addAllSegments(
                        segments.entrySet().stream().map(
                                segmentRangeListEntry -> SuccessorResponse.SegmentEntry.newBuilder()
                                        .setSegment(segmentRangeListEntry.getKey())
                                        .addAllValue(segmentRangeListEntry.getValue())
                                        .build())
                                .collect(Collectors.toList()));
    }
}
