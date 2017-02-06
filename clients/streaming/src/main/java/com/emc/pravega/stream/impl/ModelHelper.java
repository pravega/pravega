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
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Position;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.google.common.base.Preconditions;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
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

    public static final UUID encode(final TxnId txnId) {
        Preconditions.checkNotNull(txnId, "txnId");
        return new UUID(txnId.getHighBits(), txnId.getLowBits());
    }

    public static final TxnStatus encode(final TxnState txnState) {
        Preconditions.checkNotNull(txnState, "txnState");
        return TxnStatus.valueOf(txnState.getState().name());
    }

    public static final Segment encode(final SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");
        return new Segment(segment.getStreamInfo().getScope(),
                           segment.getStreamInfo().getStream(),
                           segment.getSegmentNumber());
    }

    public static final com.emc.pravega.stream.impl.FutureSegment encode(final SegmentId segment, int previous) {
        Preconditions.checkNotNull(segment, "segment");
        return new com.emc.pravega.stream.impl.FutureSegment(segment.getStreamInfo().getScope(),
                                                             segment.getStreamInfo().getStream(),
                                                             segment.getSegmentNumber(),
                                                             previous);
    }

    public static final ScalingPolicy encode(final Controller.ScalingPolicy policy) {
        Preconditions.checkNotNull(policy, "policy");
        return new ScalingPolicy(ScalingPolicy.Type.valueOf(policy.getType().name()),
                                 policy.getTargetRate(),
                                 policy.getScaleFactor(),
                                 policy.getMinNumSegments());
    }

    public static final StreamConfiguration encode(final StreamConfig config) {
        Preconditions.checkNotNull(config, "config");
        return new StreamConfigurationImpl(config.getStreamInfo().getScope(),
                config.getStreamInfo().getStream(),
                encode(config.getPolicy()));
    }

    public static final PositionImpl encode(final Position position) {
        Preconditions.checkNotNull(position, "position");
        return new PositionImpl(encodeSegmentMap(position.getOwnedSegmentsList()),
                                encodeFutureSegmentMap(position.getFutureOwnedSegmentsList()));
    }

    public static final com.emc.pravega.common.netty.PravegaNodeUri encode(final NodeUri uri) {
        Preconditions.checkNotNull(uri, "uri");
        return new com.emc.pravega.common.netty.PravegaNodeUri(uri.getEndpoint(), uri.getPort());
    }

    public static final List<AbstractMap.SimpleEntry<Double, Double>> encode(final Map<Double, Double> keyRanges) {
        Preconditions.checkNotNull(keyRanges, "keyRanges");
        return keyRanges
                .entrySet()
                .stream()
                .map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue()))
                .collect(Collectors.toList());
    }

    public static final Transaction.Status encode(final TxnState.State state, final String logString) {
        Preconditions.checkNotNull(state, "state");
        Exceptions.checkNotNullOrEmpty(logString, "logString");

        switch (state) {
            case COMMITTED:
                return Transaction.Status.COMMITTED;
            case ABORTED:
                return Transaction.Status.ABORTED;
            case OPEN:
                return Transaction.Status.OPEN;
            case SEALED:
                return Transaction.Status.SEALED;
            case UNKNOWN:
                throw new RuntimeException("Unknown transaction: " + logString);
            default:
                throw new IllegalStateException("Unknown status: " + state);
        }
    }

    public static final TxnId decode(final UUID txnId) {
        Preconditions.checkNotNull(txnId, "txnId");
        return TxnId.newBuilder()
                .setHighBits(txnId.getMostSignificantBits())
                .setLowBits(txnId.getLeastSignificantBits())
                .build();
    }

    public static final TxnState.State decode(final TxnStatus txnStatus) {
        Preconditions.checkNotNull(txnStatus, "txnStatus");
        return TxnState.State.valueOf(txnStatus.name());
    }

    public static final SegmentId decode(final Segment segment) {
        Preconditions.checkNotNull(segment, "segment");
        return createSegmentId(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber());
    }

    public static final Controller.ScalingPolicy decode(final ScalingPolicy policyModel) {
        Preconditions.checkNotNull(policyModel, "policyModel");
        return Controller.ScalingPolicy.newBuilder()
                .setType(Controller.ScalingPolicy.ScalingPolicyType.valueOf(policyModel.getType().name()))
                .setTargetRate(policyModel.getTargetRate())
                .setScaleFactor(policyModel.getScaleFactor())
                .setMinNumSegments(policyModel.getMinNumSegments())
                .build();
    }

    public static final StreamConfig decode(final StreamConfiguration configModel) {
        Preconditions.checkNotNull(configModel, "configModel");
        return StreamConfig.newBuilder()
                .setStreamInfo(createStreamInfo(configModel.getScope(), configModel.getName()))
                .setPolicy(decode(configModel.getScalingPolicy())).build();
    }

    public static final Position decode(final PositionInternal position) {
        Preconditions.checkNotNull(position, "position");
        return Position.newBuilder()
                .addAllOwnedSegments(decodeSegmentMap(position.getOwnedSegmentsWithOffsets()))
                .addAllFutureOwnedSegments(decodeFutureSegmentMap(position.getFutureOwnedSegmentsWithOffsets()))
                .build();
    }

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
    
    public static final Map<Integer, Integer> getFutureSegmentMap(final PositionInternal position) {
        Preconditions.checkNotNull(position, "position");
        Map<Integer, Integer> futures = new HashMap<>();
        position.getFutureOwnedSegments().stream().forEach(x -> futures.put(x.getSegmentNumber(),
                                                                            x.getPrecedingNumber()));
        return futures;
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

    private static final Map<Segment, Long> encodeSegmentMap(final List<Position.OwnedSegmentEntry> segmentList) {
        Preconditions.checkNotNull(segmentList);
        HashMap<Segment, Long> result = new HashMap<>();
        for (Position.OwnedSegmentEntry entry : segmentList) {
            result.put(encode(entry.getSegmentId()), entry.getValue());
        }
        return result;
    }

    private static final Map<com.emc.pravega.stream.impl.FutureSegment, Long> encodeFutureSegmentMap(
            final List<Position.FutureOwnedSegmentsEntry> futureOwnedSegmentsEntryList) {
        Preconditions.checkNotNull(futureOwnedSegmentsEntryList);
        HashMap<com.emc.pravega.stream.impl.FutureSegment, Long> result = new HashMap<>();
        for (Position.FutureOwnedSegmentsEntry entry : futureOwnedSegmentsEntryList) {
            result.put(encode(entry.getFutureSegment().getFutureSegment(),
                              entry.getFutureSegment().getPrecedingSegment().getSegmentNumber()),
                       entry.getValue());
        }
        return result;
    }

    private static final List<Position.FutureOwnedSegmentsEntry> decodeFutureSegmentMap(
            final Map<com.emc.pravega.stream.impl.FutureSegment, Long> map) {
        Preconditions.checkNotNull(map);
        List<Position.FutureOwnedSegmentsEntry> result = new ArrayList<>();
        for (Entry<com.emc.pravega.stream.impl.FutureSegment, Long> entry : map.entrySet()) {
            String scope = entry.getKey().getScope();
            String streamName = entry.getKey().getStreamName();
            int newNumber = entry.getKey().getSegmentNumber();
            int oldNumber = entry.getKey().getPrecedingNumber();
            result.add(Position.FutureOwnedSegmentsEntry.newBuilder().
                    setFutureSegment(Controller.FutureSegment.newBuilder().
                            setFutureSegment(SegmentId.newBuilder().
                                    setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(streamName)).
                                    setSegmentNumber(newNumber)).
                            setPrecedingSegment(SegmentId.newBuilder().
                                    setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(streamName)).
                                    setSegmentNumber(oldNumber))).
                    setValue(entry.getValue()).
                    build());
        }
        return result;
    }

    private static final List<Position.OwnedSegmentEntry> decodeSegmentMap(final Map<Segment, Long> map) {
        Preconditions.checkNotNull(map);
        List<Position.OwnedSegmentEntry> result = new ArrayList<>();
        map.forEach((segment, val) -> result.add(Position.OwnedSegmentEntry.newBuilder().
                setSegmentId(decode(segment)).
                setValue(val).
                build()));
        return result;
    }
}
