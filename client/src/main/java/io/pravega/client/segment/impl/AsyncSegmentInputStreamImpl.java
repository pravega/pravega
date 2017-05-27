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
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 5);
    private final ConnectionFactory connectionFactory;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private CompletableFuture<ClientConnection> connection = null;
    @GuardedBy("lock")
    private final Map<Long, ReadFutureImpl> outstandingRequests = new HashMap<>();

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
            checkSegment(segmentIsSealed.getSegment());
            ReadFutureImpl future;
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
            ReadFutureImpl future;
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

    @Data
    private static class ReadFutureImpl implements ReadFuture {
        private final WireCommands.ReadSegment request;
        private final AtomicReference<CompletableFuture<WireCommands.SegmentRead>> result;

        ReadFutureImpl(WireCommands.ReadSegment request) {
            Preconditions.checkNotNull(request);
            this.request = request;
            this.result = new AtomicReference<>(new CompletableFuture<>());
        }

        @Override
        public boolean await(long timeout) {
            FutureHelpers.await(result.get(), timeout);
            return result.get().isDone();
        }
        
        public boolean await() {
            return FutureHelpers.await(result.get());
        }

        private WireCommands.SegmentRead get() throws ExecutionException {
            return Exceptions.handleInterrupted(() -> result.get().get());
        }

        private void complete(WireCommands.SegmentRead r) {
            result.get().complete(r);
        }

        public void completeExceptionally(Exception e) {
            result.get().completeExceptionally(e);
        }

        private void reset() {
            CompletableFuture<WireCommands.SegmentRead> old = result.getAndSet(new CompletableFuture<>());
            if (!old.isDone()) {
                old.completeExceptionally(new RuntimeException("Retry already in progress"));
            }
        }

        @Override
        public boolean isSuccess() {
            return FutureHelpers.isSuccessful(result.get());
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
        if (closed.compareAndSet(false, true)) {
            closeConnection(new ConnectionClosedException());
        }
    }

    @Override
    public ReadFuture read(long offset, int length) {
        Exceptions.checkNotClosed(closed.get(), this);
        WireCommands.ReadSegment request = new WireCommands.ReadSegment(segmentId.getScopedName(), offset, length);
        
        ReadFutureImpl read = new ReadFutureImpl(request);
        synchronized (lock) {
            outstandingRequests.put(read.request.getOffset(), read);
        }
        getConnection().thenAccept((ClientConnection c) -> {
            log.debug("Sending read request {}", read);
            c.sendAsync(read.request);
        });
        return read;
    }

    private void closeConnection(Exception exceptionToInflightRequests) {
        log.trace("Closing connection with exception: {}", exceptionToInflightRequests.toString());
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
        List<ReadFutureImpl> readsToFail;
        synchronized (lock) {
            readsToFail = new ArrayList<>(outstandingRequests.values());
            //outstanding requests are not removed as they may be retried.
        }
        for (ReadFutureImpl read : readsToFail) {
            read.completeExceptionally(e);
        }
    }

    @Override
    public WireCommands.SegmentRead getResult(ReadFuture ongoingRead) {
        ReadFutureImpl read = (ReadFutureImpl) ongoingRead;
        return backoffSchedule.retryingOn(ExecutionException.class).throwingOn(RuntimeException.class).run(() -> {
            if (closed.get()) {
                throw new ObjectClosedException(this);
            }
            if (!read.await()) {
                log.debug("Retransmitting a read request {}", read.request);
                read.reset();
                ClientConnection c = Exceptions.handleInterrupted(() -> getConnection().get());
                try {
                    c.send(read.request);
                } catch (ConnectionFailedException e) {
                    closeConnection(e);
                }
            }
            return Exceptions.<ExecutionException, WireCommands.SegmentRead>handleInterrupted(() -> read.get());
        });
    }


}
