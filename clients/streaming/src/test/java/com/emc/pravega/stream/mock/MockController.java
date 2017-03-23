/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.mock;

import com.emc.pravega.common.concurrent.FutureHelpers;
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
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.ConnectionClosedException;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

@AllArgsConstructor
public class MockController implements Controller {

    private final String endpoint;
    private final int port;
    private final ConnectionFactory connectionFactory;
    @GuardedBy("$lock")
    private final Map<String, Set<Stream>> createdScopes = new HashMap<>();
    @GuardedBy("$lock")
    private final Map<Stream, StreamConfiguration> createdStreams = new HashMap<>();
    
    @Override
    @Synchronized
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        if (createdScopes.get(scopeName) != null) {
            return CompletableFuture.completedFuture(false);
        }
        createdScopes.put(scopeName, new HashSet<>());
        return CompletableFuture.completedFuture(true);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        if (createdScopes.get(scopeName) == null) {
            return CompletableFuture.completedFuture(false);
        }

        if (!createdScopes.get(scopeName).isEmpty()) {
            return FutureHelpers.failedFuture(new IllegalStateException("Scope is not empty."));
        }

        createdScopes.remove(scopeName);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> createStream(StreamConfiguration streamConfig) {
        Stream stream = new StreamImpl(streamConfig.getScope(), streamConfig.getStreamName());
        if (createdStreams.get(stream) != null) {
            return CompletableFuture.completedFuture(false);
        }

        if (createdScopes.get(streamConfig.getScope()) == null) {
            return FutureHelpers.failedFuture(new IllegalArgumentException("Scope does not exit."));
        }

        createdStreams.put(stream, streamConfig);
        createdScopes.get(streamConfig.getScope()).add(stream);
        for (Segment segment : getSegmentsForStream(stream)) {
            createSegment(segment.getScopedName(), new PravegaNodeUri(endpoint, port));
        }
        return CompletableFuture.completedFuture(true);
    }
    
    @Synchronized
    List<Segment> getSegmentsForStream(Stream stream) {
        StreamConfiguration config = createdStreams.get(stream);
        Preconditions.checkArgument(config != null, "Stream must be created first");
        ScalingPolicy scalingPolicy = config.getScalingPolicy();
        if (scalingPolicy.getType() != ScalingPolicy.Type.FIXED_NUM_SEGMENTS) {
            throw new IllegalArgumentException("Dynamic scaling not supported with a mock controller");
        }
        List<Segment> result = new ArrayList<>(scalingPolicy.getMinNumSegments());
        for (int i = 0; i < scalingPolicy.getMinNumSegments(); i++) {
            result.add(new Segment(config.getScope(), config.getStreamName(), i));
        }
        return result;
    }

    @Override
    public CompletableFuture<Boolean> alterStream(StreamConfiguration streamConfig) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Boolean> scaleStream(Stream stream, List<Integer> sealedSegments, Map<Double, Double> newKeyRanges) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Boolean> sealStream(String scope, String streamName) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(String scope, String streamName) {
        throw new NotImplementedException();
    }

    private boolean createSegment(String name, PravegaNodeUri uri) {
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
        return CompletableFuture.completedFuture(getCurrentSegments(new StreamImpl(scope, stream)));
    }
    
    private StreamSegments getCurrentSegments(Stream stream) {
        List<Segment> segmentsInStream = getSegmentsForStream(stream);
        TreeMap<Double, Segment> segments = new TreeMap<>();
        double increment = 1.0 / segmentsInStream.size();
        for (int i = 0; i < segmentsInStream.size(); i++) {
            segments.put((i + 1) * increment, new Segment(stream.getScope(), stream.getStreamName(), i));
        }
        return new StreamSegments(segments);
    }
    
    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, UUID txId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Segment segment : getSegmentsForStream(stream)) {
            futures.add(commitTxSegment(txId, segment));            
        }
        return FutureHelpers.allOf(futures);
    }
    
    private CompletableFuture<Void> commitTxSegment(UUID txId, Segment segment) {
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
        sendRequestOverNewConnection(new CommitTransaction(segment.getScopedName(), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Segment segment : getSegmentsForStream(stream)) {
            futures.add(abortTxSegment(txId, segment));            
        }
        return FutureHelpers.allOf(futures);
    }
    
    private CompletableFuture<Void> abortTxSegment(UUID txId, Segment segment) {
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
        sendRequestOverNewConnection(new AbortTransaction(segment.getScopedName(), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Pair<StreamSegments, UUID>> createTransaction(final Stream stream, final long lease,
                                                     final long maxExecutionTime, final long scaleGracePeriod) {
        UUID txId = UUID.randomUUID();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        StreamSegments currentSegments = getCurrentSegments(stream);
        for (Segment segment : currentSegments.getSegments()) {
            futures.add(createSegmentTx(txId, segment));            
        }
        return FutureHelpers.allOf(futures).thenApply(v -> Pair.of(currentSegments, txId));
    }

    private CompletableFuture<Void> createSegmentTx(UUID txId, Segment segment) {
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
            public void transactionCreated(TransactionCreated transactionCreated) {
                result.complete(null);
            }
        };
        sendRequestOverNewConnection(new CreateTransaction(segment.getScopedName(), txId), replyProcessor);
        return result;
    }

    @Override
    public CompletableFuture<Void> pingTransaction(Stream stream, UUID txId, long lease) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(Stream stream, long timestamp) {
        return CompletableFuture.completedFuture(getSegmentsForStream(stream).stream().collect(Collectors.toMap(s -> s, s -> 0L)));
    }
    
    @Override
    public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        return CompletableFuture.completedFuture(new PravegaNodeUri(endpoint, port));
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

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(Segment segment) {
        return CompletableFuture.completedFuture(true);
    }

}

