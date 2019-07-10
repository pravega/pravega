/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.RawClient;
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
    private final String delegationToken;

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
    @SneakyThrows(ConnectionFailedException.class)
    private <T extends Reply> T transformReply(Reply reply, Class<T> klass) {
        if (klass.isAssignableFrom(reply.getClass())) {
            log.debug("Returning reply {}", reply);
            return (T) reply;
        }
        closeConnection(reply);
        if (reply instanceof WireCommands.NoSuchSegment) {
            throw new NoSuchSegmentException(reply.toString());
        } else if (reply instanceof WrongHost) {
            throw new ConnectionFailedException(reply.toString());
        } else {
            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting a "
                    + klass.getName());
        }
    }

    private CompletableFuture<StreamSegmentInfo> getStreamSegmentInfo(String delegationToken) {
        log.debug("Getting segment info for segment: {}", segmentId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId, new GetStreamSegmentInfo(requestId, segmentId.getScopedName(), delegationToken))
                         .thenApply(r -> transformReply(r, StreamSegmentInfo.class));
    }
    
    private CompletableFuture<WireCommands.SegmentAttribute> getPropertyAsync(UUID attributeId, String delegationToken) {
        log.debug("Getting segment attribute: {}", attributeId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId,
                                      new GetSegmentAttribute(requestId, segmentId.getScopedName(), attributeId, delegationToken))
                         .thenApply(r -> transformReply(r, WireCommands.SegmentAttribute.class));
    }

    private CompletableFuture<SegmentAttributeUpdated> updatePropertyAsync(UUID attributeId, long expected,
                                                                                        long value, String delegationToken) {
        log.debug("Updating segment attribute: {}", attributeId);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId,
                                      new UpdateSegmentAttribute(requestId, segmentId.getScopedName(), attributeId,
                                                                 value, expected, delegationToken))
                         .thenApply(r -> transformReply(r, SegmentAttributeUpdated.class));
    }

    private CompletableFuture<SegmentTruncated> truncateSegmentAsync(Segment segment, long offset, String delegationToken) {
        log.trace("Truncating segment: {}", segment);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId, new TruncateSegment(requestId, segment.getScopedName(), offset, delegationToken))
                         .thenApply(r -> transformReply(r, SegmentTruncated.class));
    }
    
    private CompletableFuture<SegmentSealed> sealSegmentAsync(Segment segment, String delegationToken) {
        log.trace("Sealing segment: {}", segment);
        RawClient connection = getConnection();
        long requestId = connection.getFlow().getNextSequenceNumber();
        return connection.sendRequest(requestId, new SealSegment(requestId, segment.getScopedName(), delegationToken))
                         .thenApply(r -> transformReply(r, SegmentSealed.class));
    }

    @Override
    public long fetchCurrentSegmentLength() {
        Exceptions.checkNotClosed(closed.get(), this);
        val future = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> getStreamSegmentInfo(delegationToken), connectionFactory.getInternalExecutor());
        return Futures.getThrowingException(future).getWriteOffset();
    }

    @Override
    public long fetchProperty(SegmentAttribute attribute) {
        Exceptions.checkNotClosed(closed.get(), this);
        val future = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> getPropertyAsync(attribute.getValue(), delegationToken),
                                             connectionFactory.getInternalExecutor());
        return Futures.getThrowingException(future).getValue();
    }

    @Override
    public boolean compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue) {
        Exceptions.checkNotClosed(closed.get(), this);
        val future = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> updatePropertyAsync(attribute.getValue(), expectedValue, newValue, delegationToken),
                                             connectionFactory.getInternalExecutor());
        return Futures.getThrowingException(future).isSuccess();
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
        val future = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> getStreamSegmentInfo(delegationToken), connectionFactory.getInternalExecutor());
        StreamSegmentInfo info = Futures.getThrowingException(future);
        log.debug("Received SegmentInfo {}", info);
        return new SegmentInfo(segmentId, info.getStartOffset(), info.getWriteOffset(), info.isSealed(),
                               info.getLastModified());
    }

    @Override
    public void truncateSegment(long offset) {
        val future = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> truncateSegmentAsync(segmentId, offset, delegationToken),
                                             connectionFactory.getInternalExecutor());
        future.join();
    }

    @Override
    public void sealSegment() {
        val future = RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                                   .throwingOn(NoSuchSegmentException.class)
                                   .runAsync(() -> sealSegmentAsync(segmentId, delegationToken),
                                             connectionFactory.getInternalExecutor());
        future.join();
    }

}
