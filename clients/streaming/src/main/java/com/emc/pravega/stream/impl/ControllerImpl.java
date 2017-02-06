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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.GetPositionRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Position;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Positions;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdatePositionRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.grpc.ManagedChannelBuilder;

import com.emc.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * RPC based implementation of Stream Controller V1 API.
 */
@Slf4j
public class ControllerImpl implements Controller {

    private final ControllerServiceGrpc.ControllerServiceStub client;

    public ControllerImpl(final String host, final int port) {
        // create client
        client = ControllerServiceGrpc.newStub(
                ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build());
    }

    private static final class RPCAsyncCallback<T> implements StreamObserver<T> {
        private T result = null;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);

        }

        @Override
        public void onCompleted() {
            future.complete(result);
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }
    }


    @Override
    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig) {
        log.debug("Invoke AdminService.Client.createStream() with streamConfiguration: {}", streamConfig);

        RPCAsyncCallback<CreateStreamStatus> callback = new RPCAsyncCallback<>();
        client.createStream(ModelHelper.decode(streamConfig), callback);
        return callback.getFuture();
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        log.debug("Invoke AdminService.Client.alterStream() with streamConfiguration: {}", streamConfig);

        RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>();
        client.alterStream(ModelHelper.decode(streamConfig), callback);
        return callback.getFuture();
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final Stream stream,
                                                        final List<Integer> sealedSegments,
                                                        final Map<Double, Double> newKeyRanges) {
        log.debug("Invoke AdminService.Client.scaleStream() for stream: {}", stream);

        RPCAsyncCallback<ScaleResponse> callback = new RPCAsyncCallback<>();
        client.scale(ScaleRequest.newBuilder()
                             .setStreamInfo(StreamInfo.newBuilder().setScope(
                                     stream.getScope()).setStream(stream.getStreamName()))
                             .addAllSealedSegments(sealedSegments)
                             .addAllNewKeyRanges(newKeyRanges.entrySet().stream()
                                                         .map(x -> ScaleRequest.KeyRangeEntry.newBuilder()
                                                                 .setStart(x.getKey()).setEnd(x.getValue()).build())
                                                         .collect(Collectors.toList()))
                             .setScaleTimestamp(System.currentTimeMillis())
                             .build(),
                     callback);
        return callback.getFuture();
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String streamName) {
        log.debug("Invoke AdminService.Client.sealStream() for stream: {}", streamName);

        RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>();
        client.sealStream(StreamInfo.newBuilder().setScope(scope).setStream(streamName).build(), callback);
        return callback.getFuture();
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(final Stream stream, final long timestamp, final int count) {
        log.debug("Invoke ConsumerService.Client.getPositions() for stream: {}, timestamp: {}, count: {}", stream, timestamp, count);

        RPCAsyncCallback<Positions> callback = new RPCAsyncCallback<>();

        client.getPositions(GetPositionRequest.newBuilder().setStreamInfo(
                StreamInfo.newBuilder().setScope(stream.getScope()).setStream(stream.getStreamName()))
                .setTimestamp(timestamp)
                .setCount(count)
                .build(), callback);

        return callback.getFuture()
                .thenApply(positions -> {
                    log.debug("Received the following data from the controller {}", positions);
                    return positions.getPositionsList().stream().map(ModelHelper::encode).collect(Collectors.toList());
                });
    }

    @Override
    public CompletableFuture<List<PositionInternal>> updatePositions(final Stream stream, List<PositionInternal> positions) {
        log.debug("Invoke ConsumerService.Client.updatePositions() for positions: {} ", positions);

        RPCAsyncCallback<Positions> callback = new RPCAsyncCallback<>();

        final List<Position> transformed =
                positions.stream().map(ModelHelper::decode).collect(Collectors.toList());

            client.updatePositions(UpdatePositionRequest.newBuilder()
                                           .setStreamInfo(StreamInfo.newBuilder()
                                                                  .setScope(stream.getScope())
                                                                  .setStream(stream.getStreamName()))
                                           .setPositions(Positions.newBuilder().addAllPositions(transformed))
                                           .build(),
                                   callback);
        return callback.getFuture()
                .thenApply(result -> {
                    log.debug("Received the following data from the controller {}", result);
                    return result.getPositionsList().stream().map(ModelHelper::encode).collect(Collectors.toList());
                });
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String stream) {
        //Use RPC client to invoke getPositions
        log.debug("Invoke ProducerService.Client.getCurrentSegments() for stream: {}", stream);

        RPCAsyncCallback<SegmentRanges> callback =
                new RPCAsyncCallback<>();
        client.getCurrentSegments(StreamInfo.newBuilder().setScope(scope).setStream(stream).build(), callback);

        return callback.getFuture()
            .thenApply(ranges -> {
                NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
                for (com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange r : ranges.getSegmentRangesList()) {
                    rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
                }
                return rangeMap;
            })
            .thenApply(StreamSegments::new);
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(final String qualifiedSegmentName) {

        RPCAsyncCallback<NodeUri> callback = new RPCAsyncCallback<>();

            Segment segment = Segment.fromScopedName(qualifiedSegmentName);
            client.getURI(SegmentId.newBuilder()
                                  .setStreamInfo(StreamInfo.newBuilder()
                                                         .setScope(segment.getScope())
                                                         .setStream(segment.getStreamName()))
                                  .setSegmentNumber(segment.getSegmentNumber())
                                  .build(),
                          callback);
        return callback.getFuture()
                .thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<Boolean> isSegmentValid(final String scope, final String stream, final int segmentNumber) {
        RPCAsyncCallback<SegmentValidityResponse> callback = new RPCAsyncCallback<>();
        client.isSegmentValid(SegmentId.newBuilder()
                                      .setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(stream))
                                      .setSegmentNumber(segmentNumber).build(),
                              callback);
        return callback.getFuture()
                .thenApply(bRes -> bRes.getResponse());
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final Stream stream, final long timeout) {
        log.debug("Invoke AdminService.Client.createTransaction() with stream: {}", stream);

        RPCAsyncCallback<TxnId> callback = new RPCAsyncCallback<>();
            client.createTransaction(StreamInfo.newBuilder()
                                             .setScope(stream.getScope()).setStream(stream.getStreamName()).build(),
                                     callback);
        return callback.getFuture()
                .thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> commitTransaction(final Stream stream, final UUID txId) {
        log.debug("Invoke AdminService.Client.commitTransaction() with stream: {}, txUd: {}", stream, txId);

        RPCAsyncCallback<com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> callback = new RPCAsyncCallback<>();
        client.commitTransaction(TxnRequest.newBuilder().setStreamInfo(StreamInfo.newBuilder()
                                                                               .setScope(stream.getScope())
                                                                               .setStream(stream.getStreamName()))
                                         .setTxnId(ModelHelper.decode(txId))
                                         .build(),
                                 callback);
        return callback.getFuture();
    }

    @Override
    public CompletableFuture<com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> dropTransaction(final Stream stream, final UUID txId) {
        log.debug("Invoke AdminService.Client.dropTransaction() with stream: {}, txUd: {}", stream, txId);

        RPCAsyncCallback<com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> callback = new RPCAsyncCallback<>();
        client.dropTransaction(TxnRequest.newBuilder().setStreamInfo(StreamInfo.newBuilder()
                                                                             .setScope(stream.getScope())
                                                                             .setStream(stream.getStreamName()))
                                       .setTxnId(ModelHelper.decode(txId))
                                       .build(),
                               callback);
        return callback.getFuture();
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(final Stream stream, final UUID txId) {
        log.debug("Invoke AdminService.Client.checkTransactionStatus() with stream: {}, txUd: {}", stream, txId);

        RPCAsyncCallback<TxnState> callback = new RPCAsyncCallback<>();
        client.checkTransactionState(TxnRequest.newBuilder().setStreamInfo(StreamInfo.newBuilder()
                                                                                    .setScope(stream.getScope())
                                                                                    .setStream(stream.getStreamName()))
                                              .setTxnId(ModelHelper.decode(txId))
                                              .build(),
                                      callback);
        return callback.getFuture()
            .thenApply(status -> ModelHelper.encode(status.getState(), stream + " " + txId));
    }
}
