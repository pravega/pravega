/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.ScalingPolicyType;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import com.emc.pravega.controller.stream.api.v1.TxnState;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.google.common.base.Preconditions;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides translation (encode/decode) between the Model classes and its Thrift representation.
 */
public final class ModelHelper {

    /**
     * Returns UUID of transaction with given TxnId.
     *
     * @param txnId The Transaction Id.
     * @return UUID of the transaction.
     */
    public static final UUID encode(TxnId txnId) {
        Preconditions.checkNotNull(txnId, "txnId");
        return new UUID(txnId.getHighBits(), txnId.getLowBits());
    }

    /**
     * Helper to convert TxnState instance into actual status value.
     *
     * @param txnState The state object instance.
     * @return Transaction.Status
     */
    public static final TxnStatus encode(TxnState txnState) {
        Preconditions.checkNotNull(txnState, "txnState");
        return TxnStatus.valueOf(txnState.name());
    }

    /**
     * Helper to convert Segment Id into Segment object.
     *
     * @param segment The Segment Id.
     * @return New instance of Segment.
     */
    public static final Segment encode(final SegmentId segment) {
        Preconditions.checkNotNull(segment, "Segment");
        return new Segment(segment.getScope(), segment.getStreamName(), segment.getNumber());
    }

    public static final ScalingPolicy encode(final com.emc.pravega.controller.stream.api.v1.ScalingPolicy policy) {
        Preconditions.checkNotNull(policy, "ScalingPolicy");
        return new ScalingPolicy(ScalingPolicy.Type.valueOf(policy.getType().name()), policy.getTargetRate(), policy.getScaleFactor(),
                policy.getMinNumSegments());
    }

    /**
     * Helper to convert StreamConfig into Stream Configuration Impl.
     *
     * @param config The StreamConfig
     * @return New instance of StreamConfiguration Impl.
     */
    public static final StreamConfiguration encode(final StreamConfig config) {
        Preconditions.checkNotNull(config, "StreamConfig");
        return StreamConfiguration.builder()
                                  .scope(config.getScope())
                                  .streamName(config.getName())
                                  .scalingPolicy(encode(config.getPolicy()))
                                  .build();
    }

    /**
     * Helper to convert Position into PositionImpl.
     *
     * @param position Position object
     * @return An instance of PositionImpl.
     */
    public static final PositionImpl encode(final Position position) {
        Preconditions.checkNotNull(position, "Position");
        return new PositionImpl(encodeSegmentMap(position.getOwnedSegments()));
    }

    /**
     * Helper to convert NodeURI into PravegaNodeURI.
     *
     * @param uri Node URI.
     * @return PravegaNodeURI.
     */
    public static com.emc.pravega.common.netty.PravegaNodeUri encode(NodeUri uri) {
        return new com.emc.pravega.common.netty.PravegaNodeUri(uri.getEndpoint(), uri.getPort());
    }

    /**
     * Return list of key ranges available.
     *
     * @param keyRanges List of Key Value pairs.
     * @return Collection of key ranges available.
     */
    public static List<AbstractMap.SimpleEntry<Double, Double>> encode(Map<Double, Double> keyRanges) {
        return keyRanges
                .entrySet()
                .stream()
                .map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Returns actual status of given transaction status instance.
     *
     * @param status    TxnState object instance.
     * @param logString Description text to be logged when transaction status is invalid.
     * @return Transaction.Status
     */
    public static Transaction.Status encode(TxnState status, String logString) {
        Transaction.Status result;
        switch (status) {
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
            default:
                throw new IllegalStateException("Unknown status: " + status);
        }
        return result;
    }

    /**
     * Returns TxnId object instance for a given transaction with UUID.
     *
     * @param txnId UUID
     * @return Instance of TxnId.
     */
    public static final TxnId decode(UUID txnId) {
        Preconditions.checkNotNull(txnId, "txnId");
        return new TxnId(txnId.getMostSignificantBits(), txnId.getLeastSignificantBits());
    }

    /**
     * Returns transaction status for a given transaction instance.
     *
     * @param txnStatus Transaction Status instance.
     * @return The Status.
     */
    public static final TxnState decode(TxnStatus txnStatus) {
        Preconditions.checkNotNull(txnStatus, "txnStatus");
        return TxnState.valueOf(txnStatus.name());
    }

    /**
     * Decodes segment and returns an instance of SegmentId.
     *
     * @param segment The segment.
     * @return Instance of SegmentId.
     */
    public static final SegmentId decode(final Segment segment) {
        Preconditions.checkNotNull(segment, "Segment");
        return new SegmentId().setScope(segment.getScope()).setStreamName(segment.getStreamName())
                .setNumber(segment.getSegmentNumber());

    }

    /**
     * Decodes ScalingPolicy and returns an instance of Scaling Policy impl.
     *
     * @param policyModel The Scaling Policy.
     * @return Instance of Scaling Policy Impl.
     */
    public static final com.emc.pravega.controller.stream.api.v1.ScalingPolicy decode(final ScalingPolicy policyModel) {
        Preconditions.checkNotNull(policyModel, "Policy");
        return new com.emc.pravega.controller.stream.api.v1.ScalingPolicy()
                .setType(ScalingPolicyType.valueOf(policyModel.getType().name())).setTargetRate(policyModel.getTargetRate())
                .setScaleFactor(policyModel.getScaleFactor()).setMinNumSegments(policyModel.getMinNumSegments());
    }

    /**
     * Converts StreamConfiguration into StreamConfig.
     *
     * @param configModel The stream configuration.
     * @return StreamConfig instance.
     */
    public static final StreamConfig decode(final StreamConfiguration configModel) {
        Preconditions.checkNotNull(configModel, "StreamConfiguration");
        return new StreamConfig(configModel.getScope(),
                configModel.getStreamName(),
                decode(configModel.getScalingPolicy()));
    }

    /**
     * Converts internal position into position.
     *
     * @param position An internal position.
     * @return Position instance.
     */
    public static final Position decode(final PositionInternal position) {
        Preconditions.checkNotNull(position, "Position");
        return new Position(decodeSegmentMap(position.getOwnedSegmentsWithOffsets()));
    }

    /**
     * Converts PravegaNodeURI into NodeURI.
     *
     * @param uri The PravegaNodeURI string.
     * @return Node URI string.
     */
    public static NodeUri decode(PravegaNodeUri uri) {
        return new NodeUri(uri.getEndpoint(), uri.getPort());
    }

    public static final Map<Integer, Long> toSegmentOffsetMap(PositionInternal position) {
        return position.getOwnedSegmentsWithOffsets()
            .entrySet()
            .stream()
            .map(e -> new SimpleEntry<>(e.getKey().getSegmentNumber(), e.getValue()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private static Map<Segment, Long> encodeSegmentMap(final Map<SegmentId, Long> map) {
        Preconditions.checkNotNull(map);
        HashMap<Segment, Long> result = new HashMap<>();
        for (Entry<SegmentId, Long> entry : map.entrySet()) {
            result.put(encode(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private static Map<SegmentId, Long> decodeSegmentMap(final Map<Segment, Long> map) {
        Preconditions.checkNotNull(map);
        return map.entrySet().stream().collect(Collectors.toMap(e -> decode(e.getKey()), Map.Entry::getValue));
    }
}
