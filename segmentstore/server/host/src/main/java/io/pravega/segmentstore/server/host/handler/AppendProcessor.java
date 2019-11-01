/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.DelegatingRequestProcessor;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;

/**
 * Process incoming Append requests and write them to the SegmentStore.
 */
@Slf4j
public class AppendProcessor extends DelegatingRequestProcessor {
    //region Members

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final String EMPTY_STACK_TRACE = "";
    private final StreamSegmentStore store;
    private final ServerConnection connection;

    @Getter
    private final RequestProcessor nextRequestProcessor;
    private final SegmentStatsRecorder statsRecorder;
    private final DelegationTokenVerifier tokenVerifier;
    private final boolean replyWithStackTraceOnError;
    private final ConcurrentHashMap<Pair<String, UUID>, WriterState> writerStates = new ConcurrentHashMap<>();

    //endregion

    //region Constructor


    /**
     * Creates a new instance of the AppendProcessor class with no Metrics StatsRecorder.
     *
     * @param store      The SegmentStore to send append requests to.
     * @param connection The ServerConnection to send responses to.
     * @param next       The RequestProcessor to invoke next.
     * @param verifier    The token verifier.
     */
    @VisibleForTesting
    public AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next, DelegationTokenVerifier verifier) {
        this(store, connection, next, SegmentStatsRecorder.noOp(), verifier, false);
    }

    /**
     * Creates a new instance of the AppendProcessor class.
     * @param store         The SegmentStore to send append requests to.
     * @param connection    The ServerConnection to send responses to.
     * @param next          The RequestProcessor to invoke next.
     * @param statsRecorder A StatsRecorder to record Metrics.
     * @param tokenVerifier Delegation token verifier.
     * @param replyWithStackTraceOnError Whether client replies upon failed requests contain server-side stack traces or not.
     */
    AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next, SegmentStatsRecorder statsRecorder,
                    DelegationTokenVerifier tokenVerifier, boolean replyWithStackTraceOnError) {
        this.store = Preconditions.checkNotNull(store, "store");
        this.connection = Preconditions.checkNotNull(connection, "connection");
        this.nextRequestProcessor = Preconditions.checkNotNull(next, "next");
        this.statsRecorder = Preconditions.checkNotNull(statsRecorder, statsRecorder);
        this.tokenVerifier = tokenVerifier;
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
    }

    //endregion

    //region RequestProcessor Implementation

    @Override
    public void hello(Hello hello) {
        log.info("Received hello from connection: {}", connection);
        connection.send(new Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
        if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
            log.warn("Incompatible wire protocol versions {} from connection {}", hello, connection);
            connection.close();
        }
    }

    /**
     * Setup an append so that subsequent append calls can occur.
     * This requires validating that the segment exists.
     * The reply: AppendSetup indicates that appends may proceed and contains the eventNumber which they should proceed
     * from (in the event that this is a reconnect from a producer we have seen before)
     */
    @Override
    public void setupAppend(SetupAppend setupAppend) {
        String newSegment = setupAppend.getSegment();
        UUID writer = setupAppend.getWriterId();
        log.info("Setting up appends for writer: {} on segment: {}", writer, newSegment);

        if (this.tokenVerifier != null) {
            try {
                tokenVerifier.verifyToken(newSegment,
                        setupAppend.getDelegationToken(),
                        AuthHandler.Permissions.READ_UPDATE);
            } catch (TokenException e) {
                handleException(setupAppend.getWriterId(), setupAppend.getRequestId(), newSegment,
                        "Update Segment Attribute", e);
                return;
            }
        }

        // Get the last Event Number for this writer from the Store. This operation (cache=true) will automatically put
        // the value in the Store's cache so it's faster to access later.
        store.getAttributes(newSegment, Collections.singleton(writer), true, TIMEOUT)
                .whenComplete((attributes, u) -> {
                    try {
                        if (u != null) {
                            handleException(writer, setupAppend.getRequestId(), newSegment, "setting up append", u);
                        } else {
                            long eventNumber = attributes.getOrDefault(writer, Attributes.NULL_ATTRIBUTE_VALUE);
                            this.writerStates.putIfAbsent(Pair.of(newSegment, writer), new WriterState(eventNumber));
                            connection.send(new AppendSetup(setupAppend.getRequestId(), newSegment, writer, eventNumber));
                        }
                    } catch (Throwable e) {
                        handleException(writer, setupAppend.getRequestId(), newSegment, "handling setupAppend result", e);
                    }
                });
    }

    /**
     * Append data to the store.
     */
    @Override
    public void append(Append append) {
        long traceId = LoggerHelpers.traceEnter(log, "append", append);
        UUID id = append.getWriterId();
        WriterState state = this.writerStates.get(Pair.of(append.getSegment(), id));
        Preconditions.checkState(state != null, "Data from unexpected connection: %s.", id);
        long previousEventNumber = state.beginAppend(append.getEventNumber());
        int appendLength = append.getData().readableBytes();
        this.connection.adjustOutstandingBytes(appendLength);
        Timer timer = new Timer();
        storeAppend(append, previousEventNumber)
                .whenComplete((newLength, ex) -> {
                    handleAppendResult(append, newLength, previousEventNumber, ex, timer);
                    LoggerHelpers.traceLeave(log, "storeAppend", traceId, append, ex);
                })
                .whenComplete((v, e) -> {
                    this.connection.adjustOutstandingBytes(-appendLength);
                    append.getData().release();
                });
    }

    private CompletableFuture<Long> storeAppend(Append append, long lastEventNumber) {
        List<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(append.getWriterId(), AttributeUpdateType.ReplaceIfEquals, append.getEventNumber(), lastEventNumber),
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, append.getEventCount()));
        ByteBufWrapper buf = new ByteBufWrapper(append.getData());
        if (append.isConditional()) {
            return store.append(append.getSegment(), append.getExpectedLength(), buf, attributes, TIMEOUT);
        } else {
            return store.append(append.getSegment(), buf, attributes, TIMEOUT);
        }
    }

    private void handleAppendResult(final Append append, Long newWriteOffset, Long previousEventNumber, Throwable exception, Timer elapsedTimer) {
        boolean success = exception == null;
        try {
            boolean conditionalFailed = !success && (Exceptions.unwrap(exception) instanceof BadOffsetException);

            WriterState state = this.writerStates.getOrDefault(Pair.of(append.getSegment(), append.getWriterId()), null);
            if (success) {
                Preconditions.checkState(state != null, "Synchronization error while processing append: %s. Unable to send ack.", append);
                synchronized (state.getAckLock()) {
                    // Acks must be sent in order. The only way to do this is by using a lock.
                    long previousLastAcked = state.appendSuccessful(append.getEventNumber());
                    if (previousLastAcked < append.getEventNumber()) {
                        final DataAppended dataAppendedAck = new DataAppended(append.getRequestId(), append.getWriterId(), append.getEventNumber(),
                                previousLastAcked, newWriteOffset);
                        log.trace("Sending DataAppended : {}", dataAppendedAck);
                        connection.send(dataAppendedAck);
                    }
                }
            } else {
                if (conditionalFailed) {
                    log.debug("Conditional append failed due to incorrect offset: {}, {}", append, exception.getMessage());
                    if (state != null) {
                        // Revert the state to the last known good one. This is needed because we do not close the connection
                        // for offset-conditional append failures, hence we must revert the effects of the failed append.
                        state.conditionalAppendFailed();
                    }
                    connection.send(new ConditionalCheckFailed(append.getWriterId(), append.getEventNumber(), append.getRequestId()));
                } else {
                    // Clear the state in case of error.
                    this.writerStates.remove(Pair.of(append.getSegment(), append.getWriterId()));
                    handleException(append.getWriterId(), append.getRequestId(), append.getSegment(), append.getEventNumber(),
                            "appending data", exception);
                }
            }
        } catch (Throwable e) {
            success = false;
            handleException(append.getWriterId(), append.getEventNumber(), append.getSegment(), "handling append result", e);
        }

        if (success) {
            // Record any necessary metrics or statistics, but after we have sent the ack back and initiated the next append.
            this.statsRecorder.recordAppend(append.getSegment(), append.getDataLength(), append.getEventCount(), elapsedTimer.getElapsed());
        }
    }

    private void handleException(UUID writerId, long requestId, String segment, String doingWhat, Throwable u) {
        handleException(writerId, requestId, segment, -1L, doingWhat, u);
    }

    private void handleException(UUID writerId, long requestId, String segment, long eventNumber, String doingWhat, Throwable u) {
        if (u == null) {
            IllegalStateException exception = new IllegalStateException("No exception to handle.");
            log.error("Append processor: Error {} on segment = '{}'", doingWhat, segment, exception);
            throw exception;
        }

        u = Exceptions.unwrap(u);
        String clientReplyStackTrace = replyWithStackTraceOnError ? Throwables.getStackTraceAsString(u) : EMPTY_STACK_TRACE;

        if (u instanceof StreamSegmentExistsException) {
            log.warn("Segment '{}' already exists and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new SegmentAlreadyExists(requestId, segment, clientReplyStackTrace));
        } else if (u instanceof StreamSegmentNotExistsException) {
            log.warn("Segment '{}' does not exist and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new NoSuchSegment(requestId, segment, clientReplyStackTrace, -1L));
        } else if (u instanceof StreamSegmentSealedException) {
            log.info("Segment '{}' is sealed and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new SegmentIsSealed(requestId, segment, clientReplyStackTrace, eventNumber));
        } else if (u instanceof ContainerNotFoundException) {
            int containerId = ((ContainerNotFoundException) u).getContainerId();
            log.warn("Wrong host. Segment '{}' (Container {}) is not owned and {} cannot perform operation '{}'.",
                    segment, containerId, writerId, doingWhat);
            connection.send(new WrongHost(requestId, segment, "", clientReplyStackTrace));
        } else if (u instanceof BadAttributeUpdateException) {
            log.warn("Bad attribute update by {} on segment {}.", writerId, segment, u);
            connection.send(new InvalidEventNumber(writerId, requestId, clientReplyStackTrace));
            connection.close();
        } else if (u instanceof TokenExpiredException) {
            log.warn("Token expired for writer {} on segment {}.", writerId, segment, u);
            connection.send(new WireCommands.AuthTokenCheckFailed(requestId, clientReplyStackTrace,
                    WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
        } else if (u instanceof TokenException) {
            log.warn("Token check failed or writer {} on segment {}.", writerId, segment, u);
            connection.send(new WireCommands.AuthTokenCheckFailed(requestId, clientReplyStackTrace,
                    WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
        } else if (u instanceof UnsupportedOperationException) {
            log.warn("Unsupported Operation '{}'.", doingWhat, u);
            connection.send(new OperationUnsupported(requestId, doingWhat, clientReplyStackTrace));
        } else if (u instanceof CancellationException) {
            // Cancellation exception is thrown when the Operation processor is shutting down.
            log.info("Closing connection '{}' while performing append on Segment '{}' due to {}.", connection, segment, u.toString());
            connection.close();
        } else {
            logError(segment, u);
            connection.close(); // Closing connection should reinitialize things, and hopefully fix the problem
        }
    }

    private void logError(String segment, Throwable u) {
        if (u instanceof IllegalContainerStateException) {
            log.warn("Error (Segment = '{}', Operation = 'append'): {}.", segment, u.toString());
        } else {
            log.error("Error (Segment = '{}', Operation = 'append')", segment, u);
        }
    }

    //endregion

    //region WriterState

    @ThreadSafe
    private static class WriterState {
        @Getter
        private final Object ackLock = new Object();
        @GuardedBy("this")
        private long lastStoredEventNumber;
        @GuardedBy("this")
        private long lastAckedEventNumber;
        @GuardedBy("this")
        private long inFlightCount;

        WriterState(long initialEventNumber) {
            this.inFlightCount = 0;
            this.lastStoredEventNumber = initialEventNumber;
            this.lastAckedEventNumber = initialEventNumber;
        }

        /**
         * Invoked when a new append is initiated.
         *
         * @param eventNumber The Append's Event Number.
         * @return The previously attempted Event Number.
         */
        synchronized long beginAppend(long eventNumber) {
            long previousEventNumber = this.lastStoredEventNumber;
            Preconditions.checkState(eventNumber >= previousEventNumber, "Event was already appended.");
            this.lastStoredEventNumber = eventNumber;
            this.inFlightCount++;
            return previousEventNumber;
        }

        /**
         * Invoked when a conditional append has failed due to {@link BadOffsetException}. If no more appends are in the
         * pipeline, then the Last Stored Event Number is reverted to the Last (Successfully) Acked Event Number.
         */
        synchronized void conditionalAppendFailed() {
            this.inFlightCount--;
            if (this.inFlightCount == 0) {
                this.lastStoredEventNumber = this.lastAckedEventNumber;
            }
        }

        /**
         * Invoked when an append has been successfully stored and is about to be ack-ed to the Client.
         *
         * This method is designed to be invoked immediately before sending a {@link DataAppended} ack to the client,
         * however both its invocation and the ack must be sent atomically as the Client expects acks to arrive in order.
         *
         * When composing a {@link DataAppended} ack, the value passed to eventNumber should be passed as
         * {@link DataAppended#getEventNumber()} and the return value from this method should be passed as
         * {@link DataAppended#getPreviousEventNumber()}.
         *
         * @param eventNumber The Append's Event Number. This should correspond to the last successful append in the Store
         *                    and will be sent in the {@link DataAppended} ack back to the Client. This value will be
         *                    remembered and returned upon the next invocation of this method. If this value is less
         *                    than that of a previous invocation of this method (due to out-of-order acks from the Store),
         *                    it will have no effect as it has already been ack-ed as part of a previous call.
         * @return The last successful Append's Event Number (prior to this one). This is the value of eventNumber for
         * the previous invocation of this method.
         */
        synchronized long appendSuccessful(long eventNumber) {
            this.inFlightCount--;
            long previousLastAcked = this.lastAckedEventNumber;
            this.lastAckedEventNumber = Math.max(previousLastAcked, eventNumber);
            return previousLastAcked;
        }

        @Override
        public synchronized String toString() {
            return String.format("Stored=%s, Acked=%s", this.lastStoredEventNumber, this.lastAckedEventNumber);
        }
    }

    //endregion
}
