/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.mock;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.netty.impl.Flow;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.CancellableRequest;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.AllArgsConstructor;
import lombok.Synchronized;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

@AllArgsConstructor
public class MockController implements Controller {

    private final String endpoint;
    private final int port;
    private final ConnectionFactory connectionFactory;
    @GuardedBy("$lock")
    private final Map<String, Set<Stream>> createdScopes = new HashMap<>();
    @GuardedBy("$lock")
    private final Map<Stream, StreamConfiguration> createdStreams = new HashMap<>();
    private final Supplier<Long> idGenerator = () -> Flow.create().asLong();
    private final boolean callServer;
    
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
    public AsyncIterator<Stream> listStreams(String scopeName) {
        Set<Stream> collect = createdScopes.get(scopeName);
        return new AsyncIterator<Stream>() {
            Object lock = new Object();
            @GuardedBy("lock")
            Iterator<Stream> iterator = collect.iterator();
            @Override
            public CompletableFuture<Stream> getNext() {
                Stream next;
                synchronized (lock) {
                    if (!iterator.hasNext()) {
                        next = null;
                    } else {
                        next = iterator.next();
                    }
                }

                return CompletableFuture.completedFuture(next);
            }
        };
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        if (createdScopes.get(scopeName) == null) {
            return CompletableFuture.completedFuture(false);
        }

        if (!createdScopes.get(scopeName).isEmpty()) {
            return Futures.failedFuture(new IllegalStateException("Scope is not empty."));
        }

        createdScopes.remove(scopeName);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> createStream(String scope, String streamName, StreamConfiguration streamConfig) {
        Stream stream = new StreamImpl(scope, streamName);
        if (createdStreams.get(stream) != null) {
            return CompletableFuture.completedFuture(false);
        }

        if (createdScopes.get(scope) == null) {
            return Futures.failedFuture(new IllegalArgumentException("Scope does not exit."));
        }

        createdStreams.put(stream, streamConfig);
        createdScopes.get(scope).add(stream);
        for (Segment segment : getSegmentsForStream(stream)) {
            createSegment(segment.getScopedName());
        }
        return CompletableFuture.completedFuture(true);
    }
    
    @Synchronized
    List<Segment> getSegmentsForStream(Stream stream) {
        StreamConfiguration config = createdStreams.get(stream);
        Preconditions.checkArgument(config != null, "Stream must be created first");
        ScalingPolicy scalingPolicy = config.getScalingPolicy();
        if (scalingPolicy.getScaleType() != ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS) {
            throw new IllegalArgumentException("Dynamic scaling not supported with a mock controller");
        }
        List<Segment> result = new ArrayList<>(scalingPolicy.getMinNumSegments());
        for (int i = 0; i < scalingPolicy.getMinNumSegments(); i++) {
            result.add(new Segment(stream.getScope(), stream.getStreamName(), i));
        }
        return result;
    }

    @Override
    public CompletableFuture<Boolean> updateStream(String scope, String streamName, StreamConfiguration streamConfig) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final StreamCut cut) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> startScale(Stream stream, List<Long> sealedSegments, Map<Double, Double> newKeyRanges) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CancellableRequest<Boolean> scaleStream(Stream stream, List<Long> sealedSegments, Map<Double, Double> newKeyRanges,
                                                   ScheduledExecutorService executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> checkScaleStatus(Stream stream, int epoch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> sealStream(String scope, String streamName) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> deleteStream(String scope, String streamName) {
        Stream stream = new StreamImpl(scope, streamName);
        if (createdStreams.get(stream) == null) {
            return CompletableFuture.completedFuture(false);
        }
        for (Segment segment : getSegmentsForStream(stream)) {
            deleteSegment(segment.getScopedName());
        }
        createdStreams.remove(stream);
        createdScopes.get(scope).remove(stream);
        return CompletableFuture.completedFuture(true);
    }

    private boolean createSegment(String name) {
        if (!callServer) {
            return true;
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        CreateSegment command = new WireCommands.CreateSegment(idGenerator.get(), name, WireCommands.CreateSegment.NO_SCALE, 0, "");
        sendRequestOverNewConnection(command, replyProcessor, result);
        return getAndHandleExceptions(result, RuntimeException::new);
    }
    
    private boolean deleteSegment(String name) {
        if (!callServer) {
            return true;
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                result.complete(true);
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                result.complete(false);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        DeleteSegment command = new WireCommands.DeleteSegment(idGenerator.get(), name, "");
        sendRequestOverNewConnection(command, replyProcessor, result);
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String scope, String stream) {
        return CompletableFuture.completedFuture(getCurrentSegments(new StreamImpl(scope, stream)));
    }
    
    private StreamSegments getCurrentSegments(Stream stream) {
        List<Segment> segmentsInStream = getSegmentsForStream(stream);
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        double increment = 1.0 / segmentsInStream.size();
        for (int i = 0; i < segmentsInStream.size(); i++) {
            segments.put((i + 1) * increment,
                         new SegmentWithRange(new Segment(stream.getScope(), stream.getStreamName(), i),
                                              i * increment,
                                              (i + 1) * increment));
        }
        return new StreamSegments(segments, "");
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, final String writerId, final Long timestamp, UUID txId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Segment segment : getSegmentsForStream(stream)) {
            futures.add(commitTxSegment(txId, segment));            
        }
        return Futures.allOf(futures);
    }
    
    private CompletableFuture<Void> commitTxSegment(UUID txId, Segment segment) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!callServer) {
            result.complete(null);
            return result;
        }
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
                result.complete(null);
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                result.completeExceptionally(new TxnFailedException("Transaction already aborted."));
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        sendRequestOverNewConnection(new WireCommands.MergeSegments(idGenerator.get(), segment.getScopedName(),
                StreamSegmentNameUtils.getTransactionNameFromId(segment.getScopedName(), txId), ""), replyProcessor, result);
        return result;
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Segment segment : getSegmentsForStream(stream)) {
            futures.add(abortTxSegment(txId, segment));            
        }
        return Futures.allOf(futures);
    }
    
    private CompletableFuture<Void> abortTxSegment(UUID txId, Segment segment) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!callServer) {
            result.complete(null);
            return result;
        }
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
                result.completeExceptionally(new TxnFailedException("Transaction already committed."));
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted transactionAborted) {
                result.complete(null);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(segment.getScopedName(), txId);
        sendRequestOverNewConnection(new DeleteSegment(idGenerator.get(), transactionName, ""), replyProcessor, result);
        return result;
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<TxnSegments> createTransaction(final Stream stream, final long lease) {
        UUID txId = UUID.randomUUID();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        StreamSegments currentSegments = getCurrentSegments(stream);
        for (Segment segment : currentSegments.getSegments()) {
            futures.add(createSegmentTx(txId, segment));            
        }
        return Futures.allOf(futures).thenApply(v -> new TxnSegments(currentSegments, txId));
    }

    private CompletableFuture<Void> createSegmentTx(UUID txId, Segment segment) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!callServer) {
            result.complete(null);
            return result;
        }
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated transactionCreated) {
                result.complete(null);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(segment.getScopedName(), txId);
        sendRequestOverNewConnection(new CreateSegment(idGenerator.get(), transactionName, WireCommands.CreateSegment.NO_SCALE,
                0, ""), replyProcessor, result);
        return result;
    }

    @Override
    public CompletableFuture<Transaction.PingStatus> pingTransaction(Stream stream, UUID txId, long lease) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(Stream stream, long timestamp) {
        return CompletableFuture.completedFuture(getSegmentsForStream(stream).stream().collect(Collectors.toMap(s -> s, s -> 0L)));
    }
    
    @Override
    public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
        final Stream segmentStream = Stream.of(segment.getScopedStreamName());
        final CompletableFuture<StreamSegmentsWithPredecessors> result = new CompletableFuture<>();
        if (!createdStreams.containsKey(segmentStream)) {
            result.completeExceptionally(new RuntimeException("Stream is deleted"));
        } else {
            result.complete(new StreamSegmentsWithPredecessors(Collections.emptyMap(), ""));
        }
        return result;
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSuccessors(StreamCut from) {
        StreamConfiguration configuration = createdStreams.get(from.asImpl().getStream());
        if (configuration.getScalingPolicy().getScaleType() != ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS) {
            throw new IllegalArgumentException("getSuccessors not supported with dynamic scaling on mock controller");
        }
        return CompletableFuture.completedFuture(new StreamSegmentSuccessors(Collections.emptySet(), ""));
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSegments(StreamCut fromStreamCut, StreamCut toStreamCut) {
        Set<Segment> segments = ImmutableSet.<Segment>builder().addAll(fromStreamCut.asImpl().getPositions().keySet())
                                                               .addAll(toStreamCut.asImpl().getPositions().keySet()).build();
        return CompletableFuture.completedFuture(new StreamSegmentSuccessors(segments, ""));
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        return CompletableFuture.completedFuture(new PravegaNodeUri(endpoint, port));
    }

    private <T> void sendRequestOverNewConnection(WireCommand request, ReplyProcessor replyProcessor, CompletableFuture<T> resultFuture) {
        ClientConnection connection = getAndHandleExceptions(connectionFactory
            .establishConnection(Flow.from(((Request) request).getRequestId()), new PravegaNodeUri(endpoint, port), replyProcessor),
                                                             RuntimeException::new);
        resultFuture.whenComplete((result, e) -> {
            connection.close();
        });

        connection.sendAsync(request, cfe -> {
            if (cfe != null) {
                resultFuture.completeExceptionally(cfe);
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(Segment segment) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public void close() {
    }

    @Override
    public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName) {
        return CompletableFuture.completedFuture("");
    }

    @Override
    public CompletableFuture<Void> noteTimestampFromWriter(String writer, Stream stream, long timestamp,
                                                           Position lastWrittenPosition) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeWriter(String writerId, Stream stream) {
        return CompletableFuture.completedFuture(null);
    }
}

