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
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.SealSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentTruncated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.TruncateSegment;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import io.pravega.shared.security.auth.AccessOperation;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
class SegmentMetadataClientImpl implements SegmentMetadataClient {
    private static final RetryWithBackoff RETRY_SCHEDULE = Retry.withExpBackoff(1, 10, 10, 30000);

    private final Segment segmentId;
    private final Controller controller;
    private final ConnectionPool connectionPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private RawClient client = null;
    private final DelegationTokenProvider tokenProvider;

    @VisibleForTesting
    public SegmentMetadataClientImpl(Segment segment, Controller controller, ConnectionPool connectionPool,
                                     String delegationToken) {
        // The current constructor is used only for testing. Therefore, hard-coding the access operation.
        this(segment, controller, connectionPool,
                DelegationTokenProviderFactory.create(delegationToken, controller, segment, AccessOperation.READ));
    }
    
    private final ScheduledExecutorService executor() {
        return connectionPool.getInternalExecutor();
    }

    private void closeConnection(Reply badReply) {
        log.info("Closing connection as a result of receiving: {}", badReply);
        RawClient c;
        synchronized (lock) {
            c = client;
            client = null;
        }
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ", e);
            }
        }
    }

    private void closeConnection(Throwable exceptionToInflightRequests) {
        log.debug("Closing connection with exception: {}", exceptionToInflightRequests.getMessage());
        RawClient c;
        synchronized (lock) {
            c = client;
            client = null;
        }
        if (c != null) {
            try {
                c.close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ", e);
            }
        }
    }
    
    RawClient getConnection() {
        synchronized (lock) {
            if (client == null || client.isClosed()) {
                client = new RawClient(controller, connectionPool, segmentId);
            }
            return client;
        }
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows({ConnectionFailedException.class, TokenException.class})
    private <T extends Reply> T transformReply(Reply reply, Class<T> klass) {
        if (klass.isAssignableFrom(reply.getClass())) {
            return (T) reply;
        }
        closeConnection(reply);
        if (reply instanceof WireCommands.NoSuchSegment) {
            throw new NoSuchSegmentException(reply.toString());
        } else if (reply instanceof WrongHost) {
            throw new ConnectionFailedException(reply.toString());
        } else if (reply instanceof WireCommands.SegmentIsTruncated) {
            throw new ConnectionFailedException(new SegmentTruncatedException(reply.toString()));
        } else if (reply instanceof WireCommands.AuthTokenCheckFailed) {
            WireCommands.AuthTokenCheckFailed authTokenCheckReply = (WireCommands.AuthTokenCheckFailed) reply;
            if (authTokenCheckReply.isTokenExpired()) {
                log.info("Delegation token expired");
                // We want to have the request retried by the client in this case, with a renewed token.
                this.tokenProvider.signalTokenExpired();
                throw new ConnectionFailedException(new TokenExpiredException(authTokenCheckReply.toString()));
            } else {
                log.info("Delegation token invalid");
                throw new InvalidTokenException(authTokenCheckReply.toString());
            }
        } else {
            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting a "
                    + klass.getName());
        }
    }

    @VisibleForTesting
    CompletableFuture<StreamSegmentInfo> getStreamSegmentInfo() {
        log.debug("Getting segment info for segment: {}", segmentId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();

        return tokenProvider.retrieveToken()
                .thenCompose(token -> connection.sendRequest(requestId, new GetStreamSegmentInfo(
                        requestId, segmentId.getScopedName(), token)))
                .thenApply(r -> transformReply(r, StreamSegmentInfo.class));
    }
    
    private CompletableFuture<WireCommands.SegmentAttribute> getPropertyAsync(UUID attributeId) {
        log.debug("Getting segment attribute: {}", attributeId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();

        return tokenProvider.retrieveToken()
                .thenCompose(token -> connection.sendRequest(requestId, new GetSegmentAttribute(requestId,
                        segmentId.getScopedName(), attributeId, token)))
                .thenApply(r -> transformReply(r, WireCommands.SegmentAttribute.class));
    }

    private CompletableFuture<SegmentAttributeUpdated> updatePropertyAsync(UUID attributeId, long expected, long value) {
        log.trace("Updating segment attribute: {}", attributeId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();

        return tokenProvider.retrieveToken()
                .thenCompose(token -> connection.sendRequest(requestId, new UpdateSegmentAttribute(requestId,
                        segmentId.getScopedName(), attributeId, value, expected, token)))
                .thenApply(r -> transformReply(r, SegmentAttributeUpdated.class));
    }

    private CompletableFuture<SegmentTruncated> truncateSegmentAsync(Segment segment, long offset,
                                                                     DelegationTokenProvider tokenProvider) {
        log.debug("Truncating segment: {} at offset {}", segment, offset);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();

        return tokenProvider.retrieveToken()
                .thenCompose(token -> connection.sendRequest(requestId,
                        new TruncateSegment(requestId, segment.getScopedName(), offset, token)))
                .thenApply(r -> transformReply(r, SegmentTruncated.class));
    }
    
    private CompletableFuture<SegmentSealed> sealSegmentAsync(Segment segment, DelegationTokenProvider tokenProvider) {
        log.trace("Sealing segment: {}", segment);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();

        return tokenProvider.retrieveToken()
                .thenCompose(token -> connection.sendRequest(requestId, new SealSegment(requestId,
                        segment.getScopedName(), token)))
                .thenApply(r -> transformReply(r, SegmentSealed.class));
    }

    @Override
    public CompletableFuture<Long> fetchCurrentSegmentHeadOffset() {
        Exceptions.checkNotClosed(closed.get(), this);
        val result = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                .throwingOn(NoSuchSegmentException.class)
                .runAsync(this::getStreamSegmentInfo, executor());
        return result.thenApply(info -> info.getStartOffset());
    }

    @Override
    public CompletableFuture<Long> fetchCurrentSegmentLength() {
        Exceptions.checkNotClosed(closed.get(), this);
        val result = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(this::getStreamSegmentInfo, executor());
        return result.thenApply(info -> info.getWriteOffset());
    }

    @Override
    public CompletableFuture<Long> fetchProperty(SegmentAttribute attribute) {
        Exceptions.checkNotClosed(closed.get(), this);
        val result = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> getPropertyAsync(attribute.getValue()), executor());
        return result.thenApply(p -> p.getValue());
    }

    @Override
    public CompletableFuture<Boolean> compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue) {
        Exceptions.checkNotClosed(closed.get(), this);
        val result = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> updatePropertyAsync(attribute.getValue(), expectedValue, newValue), executor());
        return result.thenApply(r -> r.isSuccess());
    }

    @Override
    public void close() {
        log.info("Closing segment metadata connection for {}", segmentId);
        if (closed.compareAndSet(false, true)) {
            closeConnection(new ConnectionClosedException());
        }
    }

    @Override
    public CompletableFuture<SegmentInfo> getSegmentInfo() {
        return RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
            .throwingOn(NoSuchSegmentException.class)
            .runAsync(() -> getStreamSegmentInfo(), executor())
            .thenApply(info -> {
                return new SegmentInfo(segmentId,
                        info.getStartOffset(),
                        info.getWriteOffset(),
                        info.isSealed(),
                        info.getLastModified());
            });
    }

    @Override
    public CompletableFuture<Void> truncateSegment(long offset) {
        return Futures.toVoid(RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
            .throwingOn(NoSuchSegmentException.class)
            .runAsync(() -> truncateSegmentAsync(segmentId, offset, tokenProvider).exceptionally(t -> {
                final Throwable ex = Exceptions.unwrap(t);
                if (ex.getCause() instanceof SegmentTruncatedException) {
                    log.debug("Segment {} already truncated at offset {}. Details: {}", segmentId,
                              offset,
                              ex.getCause().getMessage());
                    return null;
                }
                throw new CompletionException(ex);
            }), executor()));
    }

    @Override
    public CompletableFuture<Void> sealSegment() {
        return Futures.toVoid(RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
            .throwingOn(NoSuchSegmentException.class)
            .runAsync(() -> sealSegmentAsync(segmentId, tokenProvider), executor()));
    }

}
