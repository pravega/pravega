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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static com.emc.pravega.common.Exceptions.handleInterrupted;

@Slf4j
class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 5);
    private final ConnectionFactory connectionFactory;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private CompletableFuture<ClientConnection> connection = null;
    @GuardedBy("lock")
    private final Map<Long, ReadFutureImpl> outstandingRequests = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<StreamSegmentInfo>> infoRequests = new HashMap<>();
    private final Supplier<Long> infoRequestIdGenerator = new AtomicLong()::incrementAndGet;
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Controller controller;

    private final class ResponseProcessor extends FailingReplyProcessor {
        
        @Override
        public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
            log.trace("Received stream segment info {}", streamInfo);
            CompletableFuture<StreamSegmentInfo> future;
            synchronized (lock) {
                future = infoRequests.remove(streamInfo.getRequestId());
            }
            if (future != null) {
                future.complete(streamInfo);
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
            log.trace("Received read result {}", segmentRead);
            ReadFutureImpl future;
            synchronized (lock) {
                future = outstandingRequests.remove(segmentRead.getOffset());
            }
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
            FutureHelpers.await(result.get(), timeout);
            return result.get().isDone();
        }
        
        public boolean await() {
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
        synchronized (lock) {
            outstandingRequests.put(read.request.getOffset(), read);
        }
        getConnection().thenApply((ClientConnection c) -> {
            log.info("Sending read request {}", read);
            c.sendAsync(read.request);
            return null;
        });
        return read;
    }

    private void closeConnection(Exception exceptionToInflightRequests) {
        log.info("Closing connection with exception: {}", exceptionToInflightRequests.toString());
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
        List<CompletableFuture<StreamSegmentInfo>> infoRequestsToFail;
        synchronized (lock) {
            readsToFail = new ArrayList<>(outstandingRequests.values());
            infoRequestsToFail = new ArrayList<>(infoRequests.values());
            infoRequests.clear();
            //outstanding requests are not removed as they may be retried.
        }
        for (ReadFutureImpl read : readsToFail) {
            read.completeExceptionally(e);
        }
        for (CompletableFuture<StreamSegmentInfo> infoRequest : infoRequestsToFail) {
            infoRequest.completeExceptionally(e);
        }
    }

    @Override
    public SegmentRead getResult(ReadFuture ongoingRead) {
        ReadFutureImpl read = (ReadFutureImpl) ongoingRead;
        return backoffSchedule.retryingOn(ExecutionException.class).throwingOn(RuntimeException.class).run(() -> {
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

    @Override
    public CompletableFuture<StreamSegmentInfo> getSegmentInfo() {
        CompletableFuture<StreamSegmentInfo> result = new CompletableFuture<>();
        long requestId = infoRequestIdGenerator.get();
        synchronized (lock) {
            infoRequests.put(requestId, result);
        }
        getConnection().thenApply(c -> {
            try {
                log.trace("Getting segment info");
                c.send(new GetStreamSegmentInfo(requestId, segmentId.getScopedName()));
            } catch (ConnectionFailedException e) {
                closeConnection(e);
            }
            return null; 
        });
        return result;
    }

}
