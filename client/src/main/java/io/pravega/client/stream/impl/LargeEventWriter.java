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
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
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
import javax.annotation.Nonnull;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.Futures.getThrowingException;

/**
 * Used by {@link EventStreamWriterImpl} to write events larger than {@link Serializer#MAX_EVENT_SIZE}.
 * It works by creating a Transient segment and then writing the data to the transient segment in multiple commands,
 * then merging the transient segment into the parent segment.
 * 
 * Ordinary connection failures and other transient errors are handled internally. However if the parent segment is sealed
 * this this class will throw {@link SegmentSealedException} and the caller will have to handle it.
 */
@RequiredArgsConstructor
@Slf4j
public class LargeEventWriter {

    private static final int WRITE_SIZE = Serializer.MAX_EVENT_SIZE;
    @Nonnull
    private final UUID writerId;
    @Nonnull
    private final Controller controller;
    @Nonnull
    private final ConnectionPool connectionPool;

    /**
     * Write the provided list of events (atomically) to the provided segment.
     * 
     * @param segment The segment to write to
     * @param events The events to append
     * @param tokenProvider A token provider
     * @param config Used for retry configuration parameters
     * @throws NoSuchSegmentException If the provided segment does not exit.
     * @throws SegmentSealedException If the segment is sealed.
     * @throws AuthenticationException If the token can't be used for this segment.
     * @throws UnsupportedOperationException If the server does not support large events.
     */
    public void writeLargeEvent(Segment segment, List<ByteBuffer> events, DelegationTokenProvider tokenProvider,
            EventWriterConfig config) throws NoSuchSegmentException, AuthenticationException, SegmentSealedException {
        List<ByteBuf> payloads = createBufs(events);
        int attempts = 1 + Math.max(0, config.getRetryAttempts());
        Retry.withExpBackoff(config.getInitialBackoffMillis(), config.getBackoffMultiple(), attempts, config.getMaxBackoffMillis()).retryWhen(t -> {
            Throwable ex = Exceptions.unwrap(t);
            if (ex instanceof ConnectionFailedException) {
                log.info("Connection failure while sending large event: {}. Retrying", ex.getMessage());
                return true;
            } else if (ex instanceof TokenExpiredException) {
                tokenProvider.signalTokenExpired();
                log.info("Authentication token expired while writing large event to segment {}. Retrying", segment);
                return true;
            } else {
                return false;
            }
        }).run(() -> {
            @Cleanup
            RawClient client = new RawClient(controller, connectionPool, segment);
            write(segment, payloads, client, tokenProvider);
            return null;
        });
    }

    private List<ByteBuf> createBufs(List<ByteBuffer> events) {
        ByteBuffer[] toWrite = new ByteBuffer[2 * events.size()];
        for (int i = 0; i < events.size(); i++) {
            ByteBuffer event = events.get(i);
            byte[] header = new byte[WireCommands.TYPE_PLUS_LENGTH_SIZE];
            ByteBuffer wrapped = ByteBuffer.wrap(header);
            wrapped.putInt(WireCommandType.EVENT.getCode());
            wrapped.putInt(event.remaining());
            wrapped.flip();
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

        String token = getThrowingException(tokenProvider.retrieveToken());

        CreateTransientSegment createSegment = new CreateTransientSegment(requestId,
                writerId,
                parentSegment.getScopedName(),
                token);

        SegmentCreated created = transformSegmentCreated(getThrowingException(client.sendRequest(requestId, createSegment)),
                                                         parentSegment.getScopedName());
        requestId = client.getFlow().getNextSequenceNumber();
        SetupAppend setup = new SetupAppend(requestId, writerId, created.getSegment(), token);

        AppendSetup appendSetup = transformAppendSetup(getThrowingException(client.sendRequest(requestId, setup)),
                                                       created.getSegment());

        if (appendSetup.getLastEventNumber() != WireCommands.NULL_ATTRIBUTE_VALUE) {
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
            transformDataAppended(getThrowingException(futures.get(i)), created.getSegment());
        }
        requestId = client.getFlow().getNextSequenceNumber();

        MergeSegments merge = new MergeSegments(requestId, parentSegment.getScopedName(), created.getSegment(), token);

        transformSegmentMerged(getThrowingException(client.sendRequest(requestId, merge)), created.getSegment());
    }

    // Trick to fail fast if any of the futures have completed.
    private void failFast(ArrayList<CompletableFuture<Reply>> futures, String segmentId) throws TokenExpiredException,
            NoSuchSegmentException, AuthenticationException, SegmentSealedException, ConnectionFailedException {
        for (CompletableFuture<Reply> future : futures) {
            if (!future.isDone()) {
                break;
            } else {
                transformDataAppended(getThrowingException(future), segmentId);
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
        } else if (reply instanceof AuthTokenCheckFailed) {
            AuthTokenCheckFailed authTokenCheckFailed = (WireCommands.AuthTokenCheckFailed) reply;
            if (authTokenCheckFailed.isTokenExpired()) {
                throw new TokenExpiredException(authTokenCheckFailed.getServerStackTrace());
            } else {
                throw new AuthenticationException(authTokenCheckFailed.toString());
            }
        } else if (reply instanceof OperationUnsupported) {
            throw new UnsupportedOperationException("Attempted to write a large append on segment " + segmentId
                    + " to a server version which only supports appends < 8 MB.");
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
