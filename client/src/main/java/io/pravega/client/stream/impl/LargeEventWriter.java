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
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalBlockEnd;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransientSegment;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.MergeSegments;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsTruncated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentsMerged;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class LargeEventWriter {

    private static final int WRITE_SIZE = Serializer.MAX_EVENT_SIZE;
    private final UUID writerId;
    private Controller controller;
    private ConnectionPool connectionPool;

    public void writeLargeEvent(Segment segment, List<ByteBuffer> events, DelegationTokenProvider tokenProvider,
            EventWriterConfig config) throws NoSuchSegmentException, AuthenticationException, SegmentSealedException {
        List<ByteBuf> payloads = createBufs(events);
        int attempts = 1 + Math.max(0, config.getRetryAttempts());
        boolean sent = false;
        Exception cause = null;
        while (!sent && attempts > 0) {
            try {
                @Cleanup
                RawClient client = new RawClient(controller, connectionPool, segment);
                write(segment, payloads, client, tokenProvider);
                sent = true;
            } catch (ConnectionFailedException e) {
                log.info("Connection failure while sending large event: {}. Retrying", e.getMessage());
                cause = e;
            } catch (TokenExpiredException e) {
                tokenProvider.signalTokenExpired();
                log.info("Authentication token expired while writing large event to segment {}. Retrying", segment);
            }
            attempts--;
        }
        if (!sent) {
            throw new RetriesExhaustedException("Failed to write large event to segment " + segment, cause);
        }
    }

    private List<ByteBuf> createBufs(List<ByteBuffer> events) {
        ByteBuffer[] toWrite = new ByteBuffer[2 * events.size()];
        for (int i = 0; i < events.size(); i++) {
            ByteBuffer event = events.get(i);
            byte[] header = new byte[WireCommands.TYPE_PLUS_LENGTH_SIZE];
            ByteBuffer wrapped = ByteBuffer.wrap(header);
            wrapped.putInt(WireCommandType.EVENT.getCode());
            wrapped.putInt(event.remaining());
            wrapped.reset();
            toWrite[2 * i] = wrapped;
            toWrite[2 * i + 1] = event;
        }
        ByteBuf master = Unpooled.wrappedBuffer(toWrite);
        ArrayList<ByteBuf> result = new ArrayList<>();
        while (master.isReadable()) {
            int toRead = Math.min(master.readableBytes(), WRITE_SIZE);
            result.add(master.readSlice(toRead));
        }
        return result;
    }

    private void write(Segment parentSegment, List<ByteBuf> payloads, RawClient client,
            DelegationTokenProvider tokenProvider) throws TokenExpiredException, NoSuchSegmentException,
            AuthenticationException, SegmentSealedException, ConnectionFailedException {

        long requestId = client.getFlow().getNextSequenceNumber();
        log.debug("Writing large event to segment {} with writer id {}", parentSegment, writerId);

        String token = tokenProvider.retrieveToken().join();

        CreateTransientSegment createSegment = new CreateTransientSegment(requestId,
                writerId,
                parentSegment.getScopedName(),
                token);

        SegmentCreated created = transformSegmentCreated(client.sendRequest(0, createSegment).join(),
                                                         parentSegment.getScopedName());

        SetupAppend setup = new SetupAppend(requestId, writerId, created.getSegment(), token);

        AppendSetup appendSetup = transformAppendSetup(client.sendRequest(requestId, setup).join(),
                                                       created.getSegment());

        if (appendSetup.getLastEventNumber() != 0) {
            throw new IllegalStateException(
                    "Server indicates that transient segment was already written to: " + created.getSegment());
        }

        long expectedOffset = 0;
        val futures = new ArrayList<CompletableFuture<Reply>>();
        for (int i = 0; i < payloads.size(); i++) {
            requestId = client.getFlow().getNextSequenceNumber();
            ByteBuf payload = payloads.get(i);
            val request = new ConditionalBlockEnd(writerId,
                    i,
                    expectedOffset,
                    Unpooled.wrappedBuffer(payload),
                    requestId);
            expectedOffset += payload.readableBytes();
            val reply = client.sendRequest(requestId, request);
            failFast(futures, created.getSegment());
            futures.add(reply);
        }
        for (int i = 0; i < futures.size(); i++) {
            transformDataAppended(futures.get(i).join(), created.getSegment());
        }
        requestId = client.getFlow().getNextSequenceNumber();

        MergeSegments merge = new MergeSegments(requestId, created.getSegment(), parentSegment.getScopedName(), token);

        transformSegmentMerged(client.sendRequest(requestId, merge).join(), created.getSegment());
    }

    // Trick to fail fast if any of the futures have completed.
    private void failFast(ArrayList<CompletableFuture<Reply>> futures, String segmentId) throws TokenExpiredException,
            NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<Reply> future = futures.get(i);
            if (!future.isDone()) {
                break;
            } else {
                transformDataAppended(future.join(), segmentId);
            }
        }
    }

    private SegmentCreated transformSegmentCreated(Reply reply, String segmentId) throws TokenExpiredException,
            NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        if (reply instanceof SegmentCreated) {
            return (SegmentCreated) reply;
        } else {
            throw handleUnexpectedReply(reply, "SegmentCreated", segmentId);
        }
    }

    private AppendSetup transformAppendSetup(Reply reply, String segmentId) throws TokenExpiredException,
            NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        if (reply instanceof AppendSetup) {
            return (AppendSetup) reply;
        } else {
            throw handleUnexpectedReply(reply, "AppendSetup", segmentId);
        }
    }

    private Void transformDataAppended(Reply reply, String segmentId) throws TokenExpiredException,
            NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        if (reply instanceof DataAppended) {
            return null;
        } else {
            throw handleUnexpectedReply(reply, "DataAppended", segmentId);
        }
    }

    private SegmentsMerged transformSegmentMerged(Reply reply, String segmentId) throws TokenExpiredException,
            NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        if (reply instanceof SegmentsMerged) {
            return (SegmentsMerged) reply;
        } else {
            throw handleUnexpectedReply(reply, "MergeSegments", segmentId);
        }
    }

    @VisibleForTesting
    RuntimeException handleUnexpectedReply(Reply reply, String expectation, String segmentId)
            throws NoSuchSegmentException, SegmentSealedException, TokenExpiredException, AuthenticationException,
            ConnectionFailedException {
        log.warn("Unexpected reply {} observed instead of {} for conditional writer {} for {}", reply, expectation, writerId, segmentId);
        if (reply instanceof WireCommands.NoSuchSegment) {
            throw new NoSuchSegmentException(reply.toString());
        } else if (reply instanceof SegmentIsSealed) {
            throw new SegmentSealedException(reply.toString());
        } else if (reply instanceof WrongHost) {
            throw new ConnectionFailedException(reply.toString());
        } else if (reply instanceof InvalidEventNumber) {
            InvalidEventNumber ien = (InvalidEventNumber) reply;
            throw new ConnectionFailedException(ien.getWriterId() + " Got stale data from setupAppend on segment "
                    + segmentId + " for ConditionalOutputStream. Event number was " + ien.getEventNumber());
        } else if (reply instanceof AuthTokenCheckFailed) {
            AuthTokenCheckFailed authTokenCheckFailed = (WireCommands.AuthTokenCheckFailed) reply;
            if (authTokenCheckFailed.isTokenExpired()) {
                throw new TokenExpiredException(authTokenCheckFailed.getServerStackTrace());
            } else {
                throw new AuthenticationException(authTokenCheckFailed.toString());
            }
        } else if (reply instanceof OperationUnsupported) {
            throw new UnsupportedOperationException("Attempted to write a large append on segment " + segmentId
                    + " to a server version which only supports appends < 8mb.");
        } else if (reply instanceof ConditionalCheckFailed | reply instanceof InvalidEventNumber
                | reply instanceof SegmentIsTruncated | reply instanceof SegmentAlreadyExists) {
            log.error("Failure: " + reply + " while appending to transient segment: " + segmentId
                    + ". This indicates a bug, but the request will be retried.");
            throw new ConnectionFailedException("Server state error");
        } else {
            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting an " + expectation);
        }
    }

}
