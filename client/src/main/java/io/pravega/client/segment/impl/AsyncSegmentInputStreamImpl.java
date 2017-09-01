/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 9, 30000);
    private final ConnectionFactory connectionFactory;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private CompletableFuture<ClientConnection> connection = null;
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<WireCommands.SegmentRead>> outstandingRequests = new HashMap<>();

    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Controller controller;

    private final class ResponseProcessor extends FailingReplyProcessor {

        @Override
        public void connectionDropped() {
            closeConnection(new ConnectionFailedException());
        }

        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            closeConnection(new ConnectionFailedException(wrongHost.toString()));
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            //TODO: It's not clear how we should be handling this case. (It should be impossible...)
            closeConnection(new IllegalArgumentException(noSuchSegment.toString()));
        }
        
        @Override
        public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
            log.info("Received segmentSealed {}", segmentIsSealed);
            checkSegment(segmentIsSealed.getSegment());
            CompletableFuture<SegmentRead> future;
            synchronized (lock) {
                future = outstandingRequests.remove(segmentIsSealed.getRequestId());
            }
            if (future != null) {
                future.complete(new WireCommands.SegmentRead(segmentIsSealed.getSegment(),
                        segmentIsSealed.getRequestId(),
                        true,
                        true,
                        ByteBuffer.allocate(0)));
            }
        }

        @Override
        public void segmentRead(WireCommands.SegmentRead segmentRead) {
            checkSegment(segmentRead.getSegment());
            log.trace("Received read result {}", segmentRead);
            CompletableFuture<SegmentRead> future;
            synchronized (lock) {
                future = outstandingRequests.remove(segmentRead.getOffset());
            }
            if (future != null) {
                future.complete(segmentRead);
            }
        }

        @Override
        public void processingFailure(Exception error) {
            log.warn("Processing failure: ", error);
            closeConnection(error);
        }
        
        private void checkSegment(String segment) {
            Preconditions.checkState(segmentId.getScopedName().equals(segment),
                    "Operating on segmentId {} but received sealed for segment {}",
                    segmentId,
                    segment);
        }
    }

    public AsyncSegmentInputStreamImpl(Controller controller, ConnectionFactory connectionFactory, Segment segment) {
        super(segment);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(connectionFactory);
        Preconditions.checkNotNull(segment);
        this.controller = controller;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void close() {
        log.info("Closing reader for {}", segmentId);
        if (closed.compareAndSet(false, true)) {
            closeConnection(new ConnectionClosedException());
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public CompletableFuture<SegmentRead> read(long offset, int length) {
        Exceptions.checkNotClosed(closed.get(), this);
        WireCommands.ReadSegment request = new WireCommands.ReadSegment(segmentId.getScopedName(), offset, length);

        return backoffSchedule.retryingOn(Exception.class)
                .throwingOn(ConnectionClosedException.class)
                .runAsync(() -> {
                    return getConnection()
                            .whenComplete((connection, ex) -> {
                                if (ex != null) {
                                    log.warn("Exception while establishing connection with Pravega " +
                                            "node", ex);
                                    closeConnection(new ConnectionFailedException(ex));
                                }
                            }).thenCompose(c -> sendRequestOverConnection(request, c));
                }, connectionFactory.getInternalExecutor());
    }
    
    @SneakyThrows(ConnectionFailedException.class)
    private CompletableFuture<SegmentRead> sendRequestOverConnection(WireCommands.ReadSegment request, ClientConnection c) {
        CompletableFuture<WireCommands.SegmentRead> result = new CompletableFuture<>();            
        synchronized (lock) {
            outstandingRequests.put(request.getOffset(), result);
        }
        if (closed.get()) {
            throw new ConnectionClosedException();
        }
        log.trace("Sending read request {}", request);
        c.sendAsync(request);
        return result;
    }

    private void closeConnection(Exception exceptionToInflightRequests) {
        if (closed.get()) {
            log.info("Closing connection to segment: {}", segmentId);
        } else {            
            log.info("Closing connection to segment {} with exception: {}", segmentId, exceptionToInflightRequests);
        }
        CompletableFuture<ClientConnection> c;
        synchronized (lock) {
            c = connection;
            connection = null;
        }
        if (c != null && FutureHelpers.isSuccessful(c)) {
            try {
                c.getNow(null).close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ", e);
            }
        }
        failAllInflight(exceptionToInflightRequests);
    }

    CompletableFuture<ClientConnection> getConnection() {
        synchronized (lock) {
            //Optimistic check
            if (connection != null) {
                return connection;
            }
        }
        return controller.getEndpointForSegment(segmentId.getScopedName()).thenCompose((PravegaNodeUri uri) -> {
            synchronized (lock) {
                if (connection == null) {
                    connection = connectionFactory.establishConnection(uri, responseProcessor);
                }
                return connection;
            }
        });
    }

    private void failAllInflight(Exception e) {
        log.info("Connection failed due to a {}. Read requests will be retransmitted.", e.toString());
        List<CompletableFuture<WireCommands.SegmentRead>> readsToFail;
        synchronized (lock) {
            readsToFail = new ArrayList<>(outstandingRequests.values());
            outstandingRequests.clear();
        }
        for (CompletableFuture<WireCommands.SegmentRead> read : readsToFail) {
            read.completeExceptionally(e);
        }
    }

}
