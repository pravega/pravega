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

import io.netty.buffer.Unpooled;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.RawClient;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalAppend;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
class ConditionalOutputStreamImpl implements ConditionalOutputStream {

    private final UUID writerId;
    private final Segment segmentId;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private RawClient client = null;
    
    private final String delegationToken;
    private final Supplier<Long> requestIdGenerator = new AtomicLong()::incrementAndGet;
    private final RetryWithBackoff retrySchedule;
    
    @Override
    public String getScopedSegmentName() {
        return segmentId.getScopedName();
    }

    @Override
    public boolean write(ByteBuffer data, long expectedOffset) throws SegmentSealedException {
        synchronized (lock) { //Used to preserver order.
            long appendSequence = requestIdGenerator.get();
            return retrySchedule.retryingOn(ConnectionFailedException.class)
                    .throwingOn(SegmentSealedException.class)
                    .run(() -> {
                        if (client == null || client.isClosed()) {
                            client = new RawClient(controller, connectionFactory, segmentId);
                            long requestId = client.getFlow().getNextSequenceNumber();
                            log.debug("Setting up append on segment: {}", segmentId);
                            SetupAppend setup = new SetupAppend(requestId, writerId,
                                                                segmentId.getScopedName(),
                                                                delegationToken);
                            val reply = client.sendRequest(requestId, setup);
                            AppendSetup appendSetup = transformAppendSetup(reply.join());
                            if (appendSetup.getLastEventNumber() >= appendSequence) {
                                return true;
                            }
                        }
                        long requestId = client.getFlow().getNextSequenceNumber();
                        val request = new ConditionalAppend(writerId, appendSequence, expectedOffset,
                                                            new Event(Unpooled.wrappedBuffer(data)), requestId);
                        val reply = client.sendRequest(requestId, request);
                        return transformDataAppended(reply.join());
                    });
        } 
    }

    @Override
    public void close() {
        log.info("Closing segment metadata connection for {}", segmentId);
        if (closed.compareAndSet(false, true)) {
            closeConnection("Closed call");
        }
    }
    
    private AppendSetup transformAppendSetup(Reply reply) {
        if (reply instanceof AppendSetup) {
            return (AppendSetup) reply;
        } else {
            throw handelUnexpectedReply(reply);
        }
    }

    private boolean transformDataAppended(Reply reply) {
        if (reply instanceof DataAppended) {
            return true;
        } else if (reply instanceof ConditionalCheckFailed) {
            return false;
        } else {
            throw handelUnexpectedReply(reply);
        }
    }
    
    private RuntimeException handelUnexpectedReply(Reply reply) {
        closeConnection(reply.toString());
        if (reply instanceof WireCommands.NoSuchSegment) {
            throw new NoSuchSegmentException(reply.toString());
        } else if (reply instanceof SegmentIsSealed) {
            throw Exceptions.sneakyThrow(new SegmentSealedException(reply.toString()));
        } else if (reply instanceof WrongHost) {
            throw Exceptions.sneakyThrow(new ConnectionFailedException(reply.toString()));
        } else {
            throw Exceptions.sneakyThrow(new ConnectionFailedException("Unexpected reply of " + reply + " when expecting an AppendSetup"));
        }
    }
    
    private void closeConnection(String message) {
        if (closed.get()) {
            log.debug("Closing connection as a result of receiving: {} for segment: {}", message, segmentId);
        } else {
            log.warn("Closing connection as a result of receiving: {} for segment: {}", message, segmentId);
        }
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
}
