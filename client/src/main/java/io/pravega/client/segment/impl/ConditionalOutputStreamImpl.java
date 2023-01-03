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
import io.netty.buffer.Unpooled;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalAppend;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
class ConditionalOutputStreamImpl implements ConditionalOutputStream {

    private final UUID writerId;
    private final Segment segmentId;
    private final Controller controller;
    private final ConnectionPool connectionPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private RawClient client = null;
    
    private final DelegationTokenProvider tokenProvider;
    private final Supplier<Long> requestIdGenerator = new AtomicLong()::incrementAndGet;
    private final RetryWithBackoff retrySchedule;
    
    @Override
    public String getScopedSegmentName() {
        return segmentId.getScopedName();
    }

    @Override
    public boolean write(ByteBuffer data, long expectedOffset) throws SegmentSealedException {
        Exceptions.checkNotClosed(closed.get(), this);
        synchronized (lock) { //Used to preserver order.
            long appendSequence = requestIdGenerator.get();
            return retrySchedule.retryWhen(e -> {
                        Throwable cause = Exceptions.unwrap(e);
                        boolean hasTokenExpired = cause instanceof TokenExpiredException;
                        if (hasTokenExpired) {
                            this.tokenProvider.signalTokenExpired();
                        }
                        return cause instanceof Exception &&
                                (hasTokenExpired || cause instanceof ConnectionFailedException);
                    })
                    .run(() -> {
                        if (client == null || client.isClosed()) {
                            client = new RawClient(controller, connectionPool, segmentId);
                            long requestId = client.getFlow().getNextSequenceNumber();
                            log.debug("Setting up appends on segment {} for ConditionalOutputStream with writer id {}", segmentId, writerId);

                            CompletableFuture<Reply> reply = tokenProvider.retrieveToken().thenCompose(token -> {
                                SetupAppend setup = new SetupAppend(requestId, writerId,
                                        segmentId.getScopedName(), token);
                                return client.sendRequest(requestId, setup);
                            });

                            AppendSetup appendSetup = transformAppendSetup(reply.join());
                            if (appendSetup.getLastEventNumber() >= appendSequence) {
                                return true;
                            }
                        }
                        long requestId = client.getFlow().getNextSequenceNumber();
                        final ConditionalAppend request = new ConditionalAppend(writerId, appendSequence, expectedOffset,
                                                            new Event(Unpooled.wrappedBuffer(data)), requestId);
                        final CompletableFuture<Reply> reply = client.sendRequest(requestId, request);
                        return transformDataAppended(reply.join());
                    });
        } 
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeConnection("Closed call");
        }
    }
    
    private AppendSetup transformAppendSetup(Reply reply) {
        if (reply instanceof AppendSetup) {
            return (AppendSetup) reply;
        } else {
            throw handleUnexpectedReply(reply, "AppendSetup");
        }
    }

    private boolean transformDataAppended(Reply reply) {
        if (reply instanceof DataAppended) {
            return true;
        } else if (reply instanceof ConditionalCheckFailed) {
            return false;
        } else {
            throw handleUnexpectedReply(reply, "DataAppended");
        }
    }

    @VisibleForTesting
    RuntimeException handleUnexpectedReply(Reply reply, String expectation) {
        log.warn("Unexpected reply {} observed instead of {} for conditional writer {}", reply, expectation, writerId);
        closeConnection(reply.toString());
        if (reply instanceof WireCommands.NoSuchSegment) {
            throw new NoSuchSegmentException(reply.toString());
        } else if (reply instanceof SegmentIsSealed) {
            throw Exceptions.sneakyThrow(new SegmentSealedException(reply.toString()));
        } else if (reply instanceof WrongHost) {
            throw Exceptions.sneakyThrow(new ConnectionFailedException(reply.toString()));
        } else if (reply instanceof InvalidEventNumber) {
            InvalidEventNumber ien = (InvalidEventNumber) reply;
            throw Exceptions.sneakyThrow(new ConnectionFailedException(ien.getWriterId() + 
                    " Got stale data from setupAppend on segment " + segmentId + " for ConditionalOutputStream. Flow number was " + ien.getRequestId() + "Event number was " + ien.getEventNumber()));
        } else if (reply instanceof AuthTokenCheckFailed) {
            AuthTokenCheckFailed authTokenCheckFailed = (WireCommands.AuthTokenCheckFailed) reply;
            if (authTokenCheckFailed.isTokenExpired()) {
                this.tokenProvider.signalTokenExpired();
                throw Exceptions.sneakyThrow(new TokenExpiredException(authTokenCheckFailed.getServerStackTrace()));
            } else {
                throw Exceptions.sneakyThrow(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        } else {
            throw Exceptions.sneakyThrow(new ConnectionFailedException("Unexpected reply of " + reply + " when expecting an " + expectation));
        }
    }
    
    private void closeConnection(String message) {
        if (closed.get()) {
            log.debug("Closing connection as a result of receiving: {} for segment: {} and conditional writerId: {}", message, segmentId, writerId);
        } else {
            log.warn("Closing connection as a result of receiving: {} for segment: {} and conditional writerId: {}", message, segmentId, writerId);
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
                log.warn("Exception tearing down connection for writerId: {} ", writerId, e);
            }
        }
    }
}
