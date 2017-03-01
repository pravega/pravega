/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.mock;

import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommand;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.netty.WireCommands.AbortTransaction;
import com.emc.pravega.common.netty.WireCommands.CommitTransaction;
import com.emc.pravega.common.netty.WireCommands.CreateTransaction;
import com.emc.pravega.common.netty.WireCommands.TransactionAborted;
import com.emc.pravega.common.netty.WireCommands.TransactionCommitted;
import com.emc.pravega.common.netty.WireCommands.TransactionCreated;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.ConnectionClosedException;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamSegments;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.NotImplementedException;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MockController implements Controller {

    private final String endpoint;
    private final int port;
    private final ConnectionFactory connectionFactory;

    @Override
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(StreamConfiguration streamConfig) {
        Segment segmentId = new Segment(streamConfig.getScope(), streamConfig.getStreamName(), 0);
        createSegment(segmentId.getScopedName(), new PravegaNodeUri(endpoint, port));
        return CompletableFuture.completedFuture(CreateStreamStatus.newBuilder()
                                                         .setStatus(CreateStreamStatus.Status.SUCCESS).build());
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(StreamConfiguration streamConfig) {
        return null;
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(Stream stream, List<Integer> sealedSegments, Map<Double, Double> newKeyRanges) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> sealStream(String scope, String streamName) {
        throw new NotImplementedException();
    }

    boolean createSegment(String name, PravegaNodeUri uri) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }
        };
        ClientConnection connection = getAndHandleExceptions(connectionFactory.establishConnection(uri, replyProcessor),
                                                             RuntimeException::new);
        try {
            connection.send(new WireCommands.CreateSegment(name, WireCommands.CreateSegment.NO_SCALE, 0));
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String scope, String stream) {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(1.0, new Segment(scope, stream, 0));
        return CompletableFuture.completedFuture(new StreamSegments(segments));
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, UUID txId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCommitted(TransactionCommitted transactionCommitted) {
                result.complete(null);
            }

            @Override
            public void transactionAborted(TransactionAborted transactionAborted) {
                result.completeExceptionally(new TxnFailedException("Transaction already aborted."));
            }
        };
        sendRequestOverNewConnection(new CommitTransaction(Segment.getScopedName(stream.getScope(), stream.getStreamName(), 0), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCommitted(TransactionCommitted transactionCommitted) {
                result.completeExceptionally(new RuntimeException("Transaction already committed."));
            }

            @Override
            public void transactionAborted(TransactionAborted transactionAborted) {
                result.complete(null);
            }
        };
        sendRequestOverNewConnection(new AbortTransaction(Segment.getScopedName(stream.getScope(), stream.getStreamName(), 0), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<UUID> createTransaction(Stream stream, long timeout) {
        UUID txId = UUID.randomUUID();
        CompletableFuture<UUID> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionCreated(TransactionCreated transactionCreated) {
                result.complete(txId);
            }
        };
        sendRequestOverNewConnection(new CreateTransaction(Segment.getScopedName(stream.getScope(), stream.getStreamName(), 0), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(Stream stream, long timestamp, int count) {
        return CompletableFuture.completedFuture(ImmutableList.<PositionInternal>of(getInitialPosition(stream.getScope(), stream.getStreamName())));
    }
    
    @Override
    public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        return CompletableFuture.completedFuture(new PravegaNodeUri(endpoint, port));
    }

    private PositionImpl getInitialPosition(String scope, String stream) {
        return new PositionImpl(Collections.singletonMap(new Segment(scope, stream, 0), 0L));
    }
    
    private void sendRequestOverNewConnection(WireCommand request, ReplyProcessor replyProcessor) {
        ClientConnection connection = getAndHandleExceptions(connectionFactory
            .establishConnection(new PravegaNodeUri(endpoint, port), replyProcessor), RuntimeException::new);
        try {
            connection.send(request);
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
    }

}

