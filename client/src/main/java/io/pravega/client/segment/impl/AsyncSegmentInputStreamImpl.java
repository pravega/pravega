/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.segment.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.control.impl.Controller;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsTruncated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AsyncSegmentInputStreamImpl extends AsyncSegmentInputStream {

    private final RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 9, 30000);
    private final ConnectionPool connectionPool;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private CompletableFuture<ClientConnection> connection = null;
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<WireCommands.SegmentRead>> outstandingRequests = new HashMap<>();

    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Controller controller;
    private final DelegationTokenProvider tokenProvider;
    @VisibleForTesting
    @Getter
    private final long requestId = Flow.create().asLong();
    private final Semaphore replyAvailable;

    private final class ResponseProcessor extends FailingReplyProcessor {

        @Override
        public void process(Reply reply) {
            super.process(reply);
            if (replyAvailable != null) {
                replyAvailable.release();
            }
        }
        
        @Override
        public void connectionDropped() {
            closeConnection(new ConnectionFailedException());
        }


        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            log.info("Received wrongHost {}", wrongHost);
            ClientConnection conn;
                if (Futures.isSuccessful(connection)) {
                    conn = connection.join();
                    controller.updateStaleValueInCache(wrongHost.getSegment(), conn.getLocation());
                }
                closeConnection(new ConnectionFailedException(wrongHost.toString()));
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            log.info("Received noSuchSegment {}", noSuchSegment);
            CompletableFuture<SegmentRead> future = grabFuture(noSuchSegment.getSegment(), noSuchSegment.getOffset());
            if (future != null) {
                future.completeExceptionally(new SegmentTruncatedException(String.format("Segment %s no longer exists.", noSuchSegment.getSegment())));
            }
        }
        
        @Override
        public void segmentIsTruncated(SegmentIsTruncated segmentIsTruncated) {
            log.info("Received segmentIsTruncated {}", segmentIsTruncated);
            CompletableFuture<SegmentRead> future = grabFuture(segmentIsTruncated.getSegment(), segmentIsTruncated.getOffset());
            if (future != null) {
                future.completeExceptionally(new SegmentTruncatedException(segmentIsTruncated.toString()));
            }
        }
        
        @Override
        public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
            log.info("Received segmentSealed {}", segmentIsSealed);
            CompletableFuture<SegmentRead> future = grabFuture(segmentIsSealed.getSegment(), segmentIsSealed.getOffset());
            if (future != null) {
                future.complete(new WireCommands.SegmentRead(
                        segmentIsSealed.getSegment(),
                        segmentIsSealed.getOffset(),
                        true,
                        true,
                        Unpooled.EMPTY_BUFFER,
                        segmentIsSealed.getRequestId()));
            }
        }

        @Override
        public void segmentRead(WireCommands.SegmentRead segmentRead) {
            log.trace("Received read result {}", segmentRead);
            CompletableFuture<SegmentRead> future = grabFuture(segmentRead.getSegment(), segmentRead.getOffset());
            if (future != null) {
                future.complete(segmentRead);
            }
        }

        private CompletableFuture<SegmentRead> grabFuture(String segment, long offset) {
            checkSegment(segment);
            synchronized (lock) {
                return outstandingRequests.remove(offset);
            }
        }

        @Override
        public void processingFailure(Exception error) {
            log.warn("Processing failure on segment {}", segmentId, error);
            closeConnection(error);
        }

        @Override
        public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
            log.warn("Auth check failed for reads on segment {} with {}",  segmentId, authTokenCheckFailed);
            if (authTokenCheckFailed.isTokenExpired()) {
                tokenProvider.signalTokenExpired();
                closeConnection(new TokenExpiredException(authTokenCheckFailed.getServerStackTrace()));
            } else {
                closeConnection(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        }

        private void checkSegment(String segment) {
            Preconditions.checkState(segmentId.getScopedName().equals(segment),
                    "Operating on segmentId {} but received sealed for segment {}",
                    segmentId,
                    segment);
        }

        @Override
        public void errorMessage(WireCommands.ErrorMessage errorMessage) {
            log.info("Received an errorMessage containing an unhandled {} on segment {}",
                    errorMessage.getErrorCode().getExceptionType().getSimpleName(),
                    errorMessage.getSegment());
            closeConnection(errorMessage.getThrowableException());
        }

    }

    public AsyncSegmentInputStreamImpl(Controller controller, ConnectionPool connectionPool, Segment segment,
                                       DelegationTokenProvider tokenProvider, Semaphore dataAvailable) {
        super(segment);
        this.tokenProvider = tokenProvider;
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(connectionPool);
        Preconditions.checkNotNull(segment);
        this.controller = controller;
        this.connectionPool = connectionPool;
        this.replyAvailable = dataAvailable;
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
        return backoffSchedule.retryWhen(t -> {
            Throwable ex = Exceptions.unwrap(t);
            if (closed.get()) {
                log.debug("Exception: {} while reading from Segment : {}", ex.toString(), segmentId);
            } else {
                log.warn("Exception while reading from Segment {} at offset {} :", segmentId, offset, ex);
            }
            return ex instanceof Exception && !(ex instanceof ConnectionClosedException) && !(ex instanceof SegmentTruncatedException)
                    && !(ex instanceof AuthenticationException);
        }).runAsync(() -> this.tokenProvider.retrieveToken().thenComposeAsync(token -> {
            final WireCommands.ReadSegment request = new WireCommands.ReadSegment(segmentId.getScopedName(), offset, length,
                    token, requestId);
            return getConnection()
                    .whenComplete((connection1, ex) -> {
                        if (ex != null) {
                            log.warn("Exception while establishing connection with Pravega node {}: ", connection1,  ex);
                            closeConnection(new ConnectionFailedException(ex));
                        }
                    }).thenCompose(c -> sendRequestOverConnection(request, c)
                            .whenComplete((reply, ex) -> {
                                if (ex instanceof ConnectionFailedException) {
                                    log.debug("ConnectionFailedException observed when sending request {}", request, ex);
                                    closeConnection((ConnectionFailedException) ex);
                                }
                            })
                    );
        }, connectionPool.getInternalExecutor()), connectionPool.getInternalExecutor());
    }
        
    private CompletableFuture<SegmentRead> sendRequestOverConnection(WireCommands.ReadSegment request, ClientConnection c) {
        CompletableFuture<WireCommands.SegmentRead> result = new CompletableFuture<>();            
        if (closed.get()) {
            result.completeExceptionally(new ConnectionClosedException());
            return result;
        }
        synchronized (lock) {
            outstandingRequests.put(request.getOffset(), result);
        }
        log.trace("Sending read request {}", request);
        try {
            c.send(request);
        } catch (ConnectionFailedException cfe) {
            log.error("Error while sending request {} to Pravega node {} :", request, c, cfe);
            synchronized (lock) {
                outstandingRequests.remove(request.getOffset());
            }
            result.completeExceptionally(cfe);                
        }
        return result;
    }

    private void closeConnection(Exception exceptionToInflightRequests) {
        if (closed.get()) {
            log.info("Closing connection to segment: {}", segmentId);
        } else {            
            log.warn("Closing connection to segment {} with exception: {}", segmentId, exceptionToInflightRequests.toString());
        }
        CompletableFuture<ClientConnection> c;
        synchronized (lock) {
            c = connection;
            connection = null;
        }
        if (c != null && Futures.isSuccessful(c)) {
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
                    connection = connectionPool.getClientConnection(Flow.from(requestId), uri, responseProcessor);
                }
                return connection;
            }
        });
    }

    private void failAllInflight(Exception e) {
        log.info("Connection failed due to a {}. Read requests for segment {} will be retransmitted.", e.toString(), segmentId);
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
