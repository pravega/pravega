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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.util.ThriftAsyncCallback;
import com.emc.pravega.controller.util.ThriftHelper;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;

import lombok.extern.slf4j.Slf4j;

/**
 * RPC based implementation of Stream Controller V1 API.
 */
@Slf4j
public class ControllerImpl implements Controller {

    private final ControllerService.AsyncClient client;

    public ControllerImpl(final String host, final int port) {
        try {
            // initialize transport, protocol factory, and async client manager
            final TNonblockingTransport transport = new TNonblockingSocket(host, port);
            final TAsyncClientManager asyncClientManager = new TAsyncClientManager();
            final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

            // create client
            client = new ControllerService.AsyncClient(protocolFactory, asyncClientManager, transport);
        } catch (IOException ioe) {
            log.error("Exception" + ioe.getMessage());
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig) {
        log.debug("Invoke AdminService.Client.createStream() with streamConfiguration: {}", streamConfig);

        final ThriftAsyncCallback<ControllerService.AsyncClient.createStream_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.createStream(ModelHelper.decode(streamConfig), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        log.debug("Invoke AdminService.Client.alterStream() with streamConfiguration: {}", streamConfig);

        final ThriftAsyncCallback<ControllerService.AsyncClient.alterStream_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.alterStream(ModelHelper.decode(streamConfig), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final String scope, final String streamName,
            final List<Integer> sealedSegments, final Map<Double, Double> newKeyRanges) {
        log.debug("Invoke AdminService.Client.scaleStream() for stream: {}", streamName);

        final ThriftAsyncCallback<ControllerService.AsyncClient.scale_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.scale(scope,
                    streamName,
                    sealedSegments,
                    newKeyRanges,
                    System.currentTimeMillis(),
                    callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(final String scope, final String streamName,
            final long timestamp, final int count) {
        log.debug("Invoke ConsumerService.Client.getPositions() for stream: {}, timestamp: {}, count: {}", streamName, timestamp, count);

        final ThriftAsyncCallback<ControllerService.AsyncClient.getPositions_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getPositions(scope, streamName, timestamp, count, callback);
            return null;
        });
        return callback
                .getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                .thenApply(result -> {
                    log.debug("Received the following data from the controller {}", result);
                    return result.stream().map(ModelHelper::encode).collect(Collectors.toList());
                });
    }

    @Override
    public CompletableFuture<List<PositionInternal>> updatePositions(final String scope, final String streamName,
            List<PositionInternal> positions) {
        log.debug("Invoke ConsumerService.Client.updatePositions() for positions: {} ", positions);

        final List<com.emc.pravega.controller.stream.api.v1.Position> transformed =
                positions.stream().map(ModelHelper::decode).collect(Collectors.toList());

        final ThriftAsyncCallback<ControllerService.AsyncClient.updatePositions_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.updatePositions(scope, streamName, transformed, callback);
            return null;
        });
        return callback
                .getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                .thenApply(result -> {
                    log.debug("Received the following data from the controller {}", result);
                    return result.stream().map(ModelHelper::encode).collect(Collectors.toList());
                });
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName) {
        //Use RPC client to invoke getPositions
        log.debug("Invoke ProducerService.Client.getCurrentSegments() for stream: {}", streamName);

        final ThriftAsyncCallback<ControllerService.AsyncClient.getCurrentSegments_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getCurrentSegments(scope, streamName, callback);
            return callback.getResult();
        });
        return callback.getResult()
            .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
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
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(final String qualifiedSegmentName) {
        final ThriftAsyncCallback<ControllerService.AsyncClient.getURI_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            Segment segment = Segment.fromScopedName(qualifiedSegmentName);
            client.getURI(new SegmentId(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber()), callback);
            return callback.getResult();
        });
        return callback
                .getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                .thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<Boolean> isSegmentValid(final String scope, final String streamName,
            final int segmentNumber) {
        final ThriftAsyncCallback<ControllerService.AsyncClient.isSegmentValid_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.isSegmentValid(scope, streamName, segmentNumber, callback);
            return callback.getResult();
        });

        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final String scope, final String streamName,
            final long timeout) {
        log.debug("Invoke AdminService.Client.createTransaction() with stream: {}", streamName);

        final ThriftAsyncCallback<ControllerService.AsyncClient.createTransaction_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.createTransaction(scope, streamName, callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult)).thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<TransactionStatus> commitTransaction(final String scope, final String streamName,
            final UUID txId) {
        log.debug("Invoke AdminService.Client.commitTransaction() with stream: {}, txUd: {}", streamName, txId);

        final ThriftAsyncCallback<ControllerService.AsyncClient.commitTransaction_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.commitTransaction(scope, streamName, ModelHelper.decode(txId), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<TransactionStatus> dropTransaction(final String scope, final String streamName,
            final UUID txId) {
        log.debug("Invoke AdminService.Client.dropTransaction() with stream: {}, txUd: {}", streamName, txId);

        final ThriftAsyncCallback<ControllerService.AsyncClient.dropTransaction_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.dropTransaction(scope, streamName, ModelHelper.decode(txId), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(final String scope, final String streamName,
            final UUID txId) {
        log.debug("Invoke AdminService.Client.checkTransactionStatus() with stream: {}, txUd: {}", streamName, txId);

        final ThriftAsyncCallback<ControllerService.AsyncClient.checkTransactionStatus_call> callback =
                new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.checkTransactionStatus(scope, streamName, ModelHelper.decode(txId), callback);
            return null;
        });
        return callback.getResult()
            .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
            .thenApply(status -> ModelHelper.encode(status, streamName + " " + txId));
    }
}
