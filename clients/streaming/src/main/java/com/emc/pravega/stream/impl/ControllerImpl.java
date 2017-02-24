/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.util.ThriftAsyncCallback;
import com.emc.pravega.controller.util.ThriftHelper;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import lombok.extern.slf4j.Slf4j;

/**
 * RPC based implementation of Stream Controller V1 API.
 */
@Slf4j
public class ControllerImpl implements Controller {

    private final ControllerService.AsyncClient client;

    /**
     * Creates a new instance of Controller class.
     *
     * @param host The controller host name.
     * @param port The controller port number.
     */
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
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        log.trace("Invoke AdminService.Client.createScope() with name: {}", scopeName);

        final ThriftAsyncCallback<ControllerService.AsyncClient.createScope_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.createScope(scopeName, callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig) {
        log.trace("Invoke AdminService.Client.createStream() with streamConfiguration: {}", streamConfig);

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
        log.trace("Invoke AdminService.Client.alterStream() with streamConfiguration: {}", streamConfig);

        final ThriftAsyncCallback<ControllerService.AsyncClient.alterStream_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.alterStream(ModelHelper.decode(streamConfig), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final Stream stream,
                                                        final List<Integer> sealedSegments,
                                                        final Map<Double, Double> newKeyRanges) {
        log.trace("Invoke AdminService.Client.scaleStream() for stream: {}", stream);

        final ThriftAsyncCallback<ControllerService.AsyncClient.scale_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.scale(stream.getScope(),
                    stream.getStreamName(),
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
    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String streamName) {
        log.trace("Invoke AdminService.Client.sealStream() for stream: {}", streamName);

        final ThriftAsyncCallback<ControllerService.AsyncClient.alterStream_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.sealStream(scope, streamName, callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult));
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(final Stream stream, final long timestamp, final int count) {
        log.trace("Invoke ConsumerService.Client.getPositions() for stream: {}, timestamp: {}, count: {}", stream, timestamp, count);

        final ThriftAsyncCallback<ControllerService.AsyncClient.getPositions_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getPositions(stream.getScope(), stream.getStreamName(), timestamp, count, callback);
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
    public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
        log.trace("Invoke ConsumerService.Client.getSegmentsImmediatlyFollowing() for segment: {} ", segment);
        final SegmentId transformed = ModelHelper.decode(segment);
        final ThriftAsyncCallback<ControllerService.AsyncClient.getSegmentsImmediatlyFollowing_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getSegmentsImmediatlyFollowing(transformed, callback);
            return null;
        });
        return callback.getResult()
                       .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
                       .thenApply(successors -> {
                           log.debug("Received the following data from the controller {}", successors);
                           Map<Segment, List<Integer>> result = new HashMap<>();
                           for (Entry<SegmentId, List<Integer>> successor : successors.entrySet()) {
                               result.put(ModelHelper.encode(successor.getKey()), successor.getValue());
                           }
                           return result;
                       });
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String stream) {
        //Use RPC client to invoke getPositions
        log.trace("Invoke ProducerService.Client.getCurrentSegments() for stream: {}", stream);

        final ThriftAsyncCallback<ControllerService.AsyncClient.getCurrentSegments_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.getCurrentSegments(scope, stream, callback);
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
        log.trace("Invoke ProducerService.Client.getEndpointForSegment() for segment: {}", qualifiedSegmentName);
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
    public CompletableFuture<UUID> createTransaction(final Stream stream, final long timeout) {
        log.trace("Invoke AdminService.Client.createTransaction() with stream: {}", stream);

        final ThriftAsyncCallback<ControllerService.AsyncClient.createTransaction_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.createTransaction(stream.getScope(), stream.getStreamName(), callback);
            return null;
        });
        return callback.getResult()
                .thenApply(result -> ThriftHelper.thriftCall(result::getResult)).thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<Void> commitTransaction(final Stream stream, final UUID txId) {
        log.trace("Invoke AdminService.Client.commitTransaction() with stream: {}, txUd: {}", stream, txId);

        final ThriftAsyncCallback<ControllerService.AsyncClient.commitTransaction_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.commitTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId), callback);
            return null;
        });
        return FutureHelpers.toVoidExpecting(callback.getResult()
                                             .thenApply(result -> ThriftHelper.thriftCall(result::getResult)),
                                     TxnStatus.SUCCESS,
                                     TxnFailedException::new);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(final Stream stream, final UUID txId) {
        log.trace("Invoke AdminService.Client.abortTransaction() with stream: {}, txUd: {}", stream, txId);

        final ThriftAsyncCallback<ControllerService.AsyncClient.abortTransaction_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.abortTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId), callback);
            return null;
        });
        return FutureHelpers.toVoidExpecting(callback.getResult()
                                                     .thenApply(result -> ThriftHelper.thriftCall(result::getResult)),
                                             TxnStatus.SUCCESS,
                                             TxnFailedException::new);
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(final Stream stream, final UUID txId) {
        log.trace("Invoke AdminService.Client.checkTransactionStatus() with stream: {}, txUd: {}", stream, txId);

        final ThriftAsyncCallback<ControllerService.AsyncClient.checkTransactionStatus_call> callback = new ThriftAsyncCallback<>();
        ThriftHelper.thriftCall(() -> {
            client.checkTransactionStatus(stream.getScope(),
                                          stream.getStreamName(),
                                          ModelHelper.decode(txId),
                                          callback);
            return null;
        });
        return callback.getResult()
            .thenApply(result -> ThriftHelper.thriftCall(result::getResult))
            .thenApply(status -> ModelHelper.encode(status, stream + " " + txId));
    }

}
