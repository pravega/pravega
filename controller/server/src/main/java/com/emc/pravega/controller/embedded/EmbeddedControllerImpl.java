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
package com.emc.pravega.controller.embedded;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamSegments;
import lombok.Getter;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Getter
public class EmbeddedControllerImpl implements EmbeddedController {

    private final ControllerService controller;

    public EmbeddedControllerImpl(ControllerService controller) {
        this.controller = controller;
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(StreamConfiguration streamConfig) {
        return controller.createStream(streamConfig, System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(StreamConfiguration streamConfig) {
        return controller.alterStream(streamConfig);
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> sealStream(String scope, String streamName) {
        return controller.sealStream(scope, streamName);
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final Stream stream,
                                                        final List<Integer> sealedSegments,
                                                        final Map<Double, Double> newKeyRanges) {
        return controller.scale(stream.getScope(),
                stream.getStreamName(),
                sealedSegments,
                newKeyRanges,
                System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String scope, String stream) {
        return controller.getCurrentSegments(scope, stream)
                .thenApply((List<SegmentRange> ranges) -> {
                    NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
                    for (SegmentRange r : ranges) {
                        rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
                    }
                    return rangeMap;
                })
                .thenApply(StreamSegments::new);
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, UUID txId) {
        return FutureHelpers.toVoidExpecting(controller.commitTransaction(stream.getScope(),
                stream.getStreamName(),
                ModelHelper.decode(txId)),
                TxnStatus.SUCCESS,
                TxnFailedException::new);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        return FutureHelpers.toVoidExpecting(controller.abortTransaction(stream.getScope(),
                stream.getStreamName(),
                ModelHelper.decode(txId)),
                TxnStatus.SUCCESS,
                TxnFailedException::new);
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txnId) {
        return controller.checkTransactionStatus(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txnId))
                .thenApply(status -> ModelHelper.encode(status, stream + " " + txnId));
    }

    @Override
    public CompletableFuture<UUID> createTransaction(Stream stream, long timeout) {
        return controller.createTransaction(stream.getScope(), stream.getStreamName())
                .thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(Stream stream, long timestamp, int count) {
        return controller.getPositions(stream.getScope(), stream.getStreamName(), timestamp, count)
                .thenApply(result -> result.stream().map(ModelHelper::encode).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(final Segment segment) {
        return controller.getSegmentsImmediatlyFollowing(ModelHelper.decode(segment)).thenApply(successors -> {
            Map<Segment, List<Integer>> result = new HashMap<>();
            for (Map.Entry<SegmentId, List<Integer>> successor : successors.entrySet()) {
                result.put(ModelHelper.encode(successor.getKey()), successor.getValue());
            }
            return result;
        });
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        Segment segment = Segment.fromScopedName(qualifiedSegmentName);
        try {
            return controller.getURI(new SegmentId(segment.getScope(), segment.getStreamName(),
                    segment.getSegmentNumber())).thenApply(ModelHelper::encode);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
