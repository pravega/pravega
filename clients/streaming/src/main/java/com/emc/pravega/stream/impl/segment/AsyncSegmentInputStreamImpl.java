/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.stream.impl.segment;

import static com.emc.pravega.common.Exceptions.handleInterrupted;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.common.util.Retry.RetryWithBackoff;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.impl.Controller;
import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 5);
    private final ConnectionFactory connectionFactory;
    private final String segment;
    @GuardedBy("lock")
    private CompletableFuture<ClientConnection> connection = null;
    private final Object lock = new Object();
    private final ConcurrentHashMap<Long, ReadFutureImpl> outstandingRequests = new ConcurrentHashMap<>();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Controller controller;

    private final class ResponseProcessor extends FailingReplyProcessor {

        @Override
        public void connectionDropped() {
            closeConnection(new ConnectionFailedException());
        }

        @Override
        public void wrongHost(WrongHost wrongHost) {
            closeConnection(new ConnectionFailedException(wrongHost.toString()));
        }

        @Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
            //TODO: It's not clear how we should be handling this case. (It should be impossible...)
            closeConnection(new IllegalArgumentException(noSuchSegment.toString()));
        }

        @Override
        public void segmentRead(SegmentRead segmentRead) {
            ReadFutureImpl future = outstandingRequests.remove(segmentRead.getOffset());
            if (future != null) {
                future.complete(segmentRead);
            }
        }
    }

    @Data
    private static class ReadFutureImpl implements ReadFuture {
        private final ReadSegment request;
        private final AtomicReference<CompletableFuture<SegmentRead>> result;

        ReadFutureImpl(ReadSegment request) {
            Preconditions.checkNotNull(request);
            this.request = request;
            this.result = new AtomicReference<>(new CompletableFuture<>());
        }

        private boolean await() {
            return FutureHelpers.await(result.get());
        }

        private SegmentRead get() throws ExecutionException {
            return handleInterrupted(() -> result.get().get());
        }

        private void complete(SegmentRead r) {
            result.get().complete(r);
        }

        public void completeExceptionally(Exception e) {
            result.get().completeExceptionally(e);
        }

        private void reset() {
            CompletableFuture<SegmentRead> old = result.getAndSet(new CompletableFuture<>());
            if (!old.isDone()) {
                old.completeExceptionally(new RuntimeException("Retry already in progress"));
            }
        }

        @Override
        public boolean isSuccess() {
            return FutureHelpers.isSuccessful(result.get());
        }
    }

    public AsyncSegmentInputStreamImpl(Controller controller, ConnectionFactory connectionFactory, String segment) {
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(connectionFactory);
        Preconditions.checkNotNull(segment);
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.segment = segment;
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
        ReadSegment request = new ReadSegment(segment, offset, length);
        ReadFutureImpl read = new ReadFutureImpl(request);
        outstandingRequests.put(read.request.getOffset(), read);
        getConnection().thenApply((ClientConnection c) -> {
            c.sendAsync(read.request);
            return null;
        });
        return read;
    }

    private void closeConnection(Exception exceptionToInflightRequests) {
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
        return controller.getEndpointForSegment(segment).thenCompose((PravegaNodeUri uri) -> {
            synchronized (lock) {
                if (connection == null) {
                    connection = connectionFactory.establishConnection(uri, responseProcessor);
                }
                return connection;
            }
        });
    }

    private void failAllInflight(Exception e) {
        log.info("Connection failed due to a {}. Read requests will be retransmitted.", e.getMessage());
        for (ReadFutureImpl read : outstandingRequests.values()) {
            read.completeExceptionally(e);
        }
    }

    @Override
    public SegmentRead getResult(ReadFuture ongoingRead) {
        ReadFutureImpl read = (ReadFutureImpl) ongoingRead;
        return backoffSchedule.retryingOn(ExecutionException.class).throwingOn(RuntimeException.class).
                run(() -> {
            if (closed.get()) {
                throw new ObjectClosedException(this);
            }
            if (!read.await()) {
                log.debug("Retransmitting a read request {}", read.request);
                read.reset();
                ClientConnection c = handleInterrupted(() -> getConnection().get());
                try {
                    c.send(read.request);
                } catch (ConnectionFailedException e) {
                    closeConnection(e);
                }
            }
            return Exceptions.<ExecutionException, SegmentRead>handleInterrupted(() -> read.get());
        });
    }

}
