/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.common.util.Retry.RetryWithBackoff;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.ConnectionClosedException;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static com.emc.pravega.common.Exceptions.handleInterrupted;

@Slf4j
class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 5);
    private final ConnectionFactory connectionFactory;

    @GuardedBy("lock")
    private CompletableFuture<ClientConnection> connection = null;
    private final Object lock = new Object();
    private final ConcurrentHashMap<Long, ReadFutureImpl> outstandingRequests = new ConcurrentHashMap<>();
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final ConcurrentLinkedQueue<CompletableFuture<StreamSegmentInfo>> infoRequests = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Controller controller;

    private final class ResponseProcessor extends FailingReplyProcessor {
        
        @Override
        public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
            CompletableFuture<StreamSegmentInfo> request = infoRequests.poll();
            while (request != null) {
                request.complete(streamInfo);
                request = infoRequests.poll();
            }
        }        

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

        @Override
        public boolean await(long timeout) {
            return FutureHelpers.await(result.get(), timeout);
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
        ReadSegment request = new ReadSegment(segmentId.getScopedName(), offset, length);
        
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
        log.info("Connection failed due to a {}. Read requests will be retransmitted.", e.getMessage());
        for (ReadFutureImpl read : outstandingRequests.values()) {
            read.completeExceptionally(e);
        }
        CompletableFuture<StreamSegmentInfo> request = infoRequests.poll();
        while (request != null) {
            request.completeExceptionally(e);
            request = infoRequests.poll();
        }
    }

    @Override
    public SegmentRead getResult(ReadFuture ongoingRead) {
        ReadFutureImpl read = (ReadFutureImpl) ongoingRead;
        return backoffSchedule.retryingOn(ExecutionException.class).throwingOn(RuntimeException.class).run(() -> {
            if (closed.get()) {
                throw new ObjectClosedException(this);
            }
            if (!read.await(Long.MAX_VALUE)) {
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

    @Override
    public CompletableFuture<StreamSegmentInfo> getSegmentInfo() {
        CompletableFuture<StreamSegmentInfo> result = new CompletableFuture<>();
        infoRequests.add(result);
        getConnection().thenApply(c -> {
            try {
                c.send(new GetStreamSegmentInfo(segmentId.getScopedName()));
            } catch (ConnectionFailedException e) {
                closeConnection(e);
            }
            return null; 
        });
        return result;
    }

}
