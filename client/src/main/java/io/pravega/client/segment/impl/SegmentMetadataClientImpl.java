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

import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.RawClient;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.impl.Controller;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
class SegmentMetadataClientImpl implements SegmentMetadataClient {
    private static final RetryWithBackoff RETRY_SCHEDULE = Retry.withExpBackoff(1, 10, 9, 30000);

    private final Segment segmentId;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private RawClient client = null;
    private final DelegationTokenProvider tokenProvider;

    public SegmentMetadataClientImpl(Segment segment, Controller controller, ConnectionFactory connectionFactory,
                                     String delegationToken) {
        this(segment, controller, connectionFactory,
                DelegationTokenProviderFactory.create(delegationToken, controller, segment));
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
                client = new RawClient(controller, connectionFactory, segmentId);
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
        } else if (reply instanceof WireCommands.AuthTokenCheckFailed) {
            WireCommands.AuthTokenCheckFailed authTokenCheckReply = (WireCommands.AuthTokenCheckFailed) reply;
            if (authTokenCheckReply.isTokenExpired()) {
                log.info("Delegation token expired");
                throw new TokenExpiredException(authTokenCheckReply.toString());
            } else {
                log.info("Delegation token invalid");
                throw new TokenException(authTokenCheckReply.toString());
            }
        } else {
            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting a "
                    + klass.getName());
        }
    }

    private CompletableFuture<StreamSegmentInfo> getStreamSegmentInfo() {
        log.debug("Getting segment info for segment: {}", segmentId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId, new GetStreamSegmentInfo(
                requestId, segmentId.getScopedName(), tokenProvider.retrieveToken()))
                         .thenApply(r -> transformReply(r, StreamSegmentInfo.class));
    }
    
    private CompletableFuture<WireCommands.SegmentAttribute> getPropertyAsync(UUID attributeId) {
        log.debug("Getting segment attribute: {}", attributeId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId, new GetSegmentAttribute(requestId, segmentId.getScopedName(),
                                                    attributeId, tokenProvider.retrieveToken()))
                         .thenApply(r -> transformReply(r, WireCommands.SegmentAttribute.class));
    }

    private CompletableFuture<SegmentAttributeUpdated> updatePropertyAsync(UUID attributeId, long expected, long value) {
        log.trace("Updating segment attribute: {}", attributeId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId,
                                      new UpdateSegmentAttribute(requestId, segmentId.getScopedName(), attributeId,
                                                                 value, expected, tokenProvider.retrieveToken()))
                         .thenApply(r -> transformReply(r, SegmentAttributeUpdated.class));
    }

    private CompletableFuture<SegmentTruncated> truncateSegmentAsync(Segment segment, long offset,
                                                                     DelegationTokenProvider tokenProvider) {
        log.trace("Truncating segment: {}", segment);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId, new TruncateSegment(requestId, segment.getScopedName(), offset, tokenProvider.retrieveToken()))
                         .thenApply(r -> transformReply(r, SegmentTruncated.class));
    }
    
    private CompletableFuture<SegmentSealed> sealSegmentAsync(Segment segment, DelegationTokenProvider tokenProvider) {
        log.trace("Sealing segment: {}", segment);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId, new SealSegment(requestId, segment.getScopedName(), tokenProvider.retrieveToken()))
                         .thenApply(r -> transformReply(r, SegmentSealed.class));
    }

    @Override
    public long fetchCurrentSegmentLength() {
        Exceptions.checkNotClosed(closed.get(), this);
        val result = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .run(() -> Futures.getThrowingException(getStreamSegmentInfo()));
        return result.getWriteOffset();
    }

    @Override
    public long fetchProperty(SegmentAttribute attribute) {
        Exceptions.checkNotClosed(closed.get(), this);
        val result = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .run(() -> Futures.getThrowingException(getPropertyAsync(attribute.getValue())));
        return result.getValue();
    }

    @Override
    public boolean compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue) {
        Exceptions.checkNotClosed(closed.get(), this);
        val result = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .run(() -> Futures.getThrowingException(updatePropertyAsync(attribute.getValue(), expectedValue, newValue)));
        return result.isSuccess();
    }

    @Override
    public void close() {
        log.info("Closing segment metadata connection for {}", segmentId);
        if (closed.compareAndSet(false, true)) {
            closeConnection(new ConnectionClosedException());
        }
    }

    @Override
    public SegmentInfo getSegmentInfo() {
        StreamSegmentInfo info = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                .throwingOn(NoSuchSegmentException.class)
                .run(() -> Futures.getThrowingException(getStreamSegmentInfo()));
        return new SegmentInfo(segmentId, info.getStartOffset(), info.getWriteOffset(), info.isSealed(),
                               info.getLastModified());
    }

    @Override
    public void truncateSegment(long offset) {
        RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class).throwingOn(NoSuchSegmentException.class).run(() -> {
            truncateSegmentAsync(segmentId, offset, tokenProvider).join();
            return null;
        });
    }

    @Override
    public void sealSegment() {
        RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class).throwingOn(NoSuchSegmentException.class).run(() -> {
            sealSegmentAsync(segmentId, tokenProvider).join();
            return null;
        });
    }

}
