/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
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
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import io.pravega.shared.security.token.JsonWebToken;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;


import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;

/**
 * Process incoming Append requests and write them to the SegmentStore.
 */
@Builder
public class AppendProcessor extends DelegatingRequestProcessor {
    //region Members

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final String EMPTY_STACK_TRACE = "";
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(AppendProcessor.class));
    @NonNull
    private final StreamSegmentStore store;
    @NonNull
    private final ServerConnection connection;
    @NonNull
    private final ConnectionTracker connectionTracker;
    @Getter
    @NonNull
    private final RequestProcessor nextRequestProcessor;
    @NonNull
    private final SegmentStatsRecorder statsRecorder;
    private final DelegationTokenVerifier tokenVerifier;
    private final boolean replyWithStackTraceOnError;
    private final ConcurrentHashMap<Pair<String, UUID>, WriterState> writerStates = new ConcurrentHashMap<>();
    private final AtomicLong outstandingBytes = new AtomicLong();
    private final ScheduledExecutorService tokenExpiryHandlerExecutor;

    //endregion

    //region Builder

    /**
     * Creates a new {@link AppendProcessorBuilder} instance with all optional arguments set to default values.
     * These default values may not be appropriate for production use and should be used for testing purposes only.
     * @return A {@link AppendProcessorBuilder} instance.
     */
    @VisibleForTesting
    public static AppendProcessorBuilder defaultBuilder() {
        return builder()
                .nextRequestProcessor(new FailingRequestProcessor())
                .statsRecorder(SegmentStatsRecorder.noOp())
                .connectionTracker(new ConnectionTracker())
                .replyWithStackTraceOnError(false);
    }

    //endregion

    //region RequestProcessor Implementation

    @Override
    public void hello(Hello hello) {
        log.info("Received hello from connection: {}", connection);
        connection.send(new Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
        if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
            log.warn(hello.getRequestId(), "Incompatible wire protocol versions {} from connection {}", hello, connection);
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
                JsonWebToken token = tokenVerifier.verifyToken(newSegment,
                        setupAppend.getDelegationToken(),
                        AuthHandler.Permissions.READ_UPDATE);
                setupTokenExpiryTask(setupAppend, token);
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

    @VisibleForTesting
    CompletableFuture<Void> setupTokenExpiryTask(@NonNull SetupAppend setupAppend, @NonNull JsonWebToken token) {
        String segment = setupAppend.getSegment();
        UUID writerId = setupAppend.getWriterId();
        long requestId = setupAppend.getRequestId();

        if (token.getExpirationTime() == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return Futures.delayedTask(() -> {
                if (isSetupAppendCompleted(segment, writerId)) {
                    // Closing the connection will result in client authenticating with Controller again
                    // and retrying the request with a new token.
                    log.debug("Closing client connection for writer {} due to token expiry, when processing " +
                                "request {} for segment {}", writerId, requestId, segment);
                    connection.close();
                }
                return null;
            }, token.durationToExpiry(), this.tokenExpiryHandlerExecutor);
        }
    }

    @VisibleForTesting
    boolean isSetupAppendCompleted(String newSegment, UUID writer) {
        return writerStates.containsKey(Pair.of(newSegment, writer));
    }

    /**
     * Append data to the store.
     */
    @Override
    public void append(Append append) {
        long traceId = LoggerHelpers.traceEnter(log, "append", append);
        UUID id = append.getWriterId();
        WriterState state = this.writerStates.get(Pair.of(append.getSegment(), id));
        Preconditions.checkState(state != null, "Data from unexpected connection: Segment=%s, WriterId=%s.", append.getSegment(), id);
        long previousEventNumber = state.beginAppend(append.getEventNumber());
        int appendLength = append.getData().readableBytes();
        adjustOutstandingBytes(appendLength);
        Timer timer = new Timer();
        storeAppend(append, previousEventNumber)
                .whenComplete((newLength, ex) -> {
                    handleAppendResult(append, newLength, ex, state, timer);
                    LoggerHelpers.traceLeave(log, "storeAppend", traceId, append, ex);
                })
                .whenComplete((v, e) -> {
                    adjustOutstandingBytes(-appendLength);
                    append.getData().release();
                });
    }

    private void adjustOutstandingBytes(int delta) {
        long currentOutstanding = this.outstandingBytes.updateAndGet(p -> Math.max(0, p + delta));
        this.connectionTracker.updateOutstandingBytes(this.connection, delta, currentOutstanding);
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

    private void handleAppendResult(final Append append, Long newWriteOffset, Throwable exception, WriterState state, Timer elapsedTimer) {
        Preconditions.checkNotNull(state, "state");
        boolean success = exception == null;
        try {
            boolean conditionalFailed = !success && (Exceptions.unwrap(exception) instanceof BadOffsetException);

            if (success) {
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

                if (append.getEventNumber() > state.getLowestFailedEventNumber()) {
                    // The Store should not be successfully completing an Append that followed a failed one. If somehow
                    // this happened, record it in the log.
                    log.warn(append.getRequestId(), "Acknowledged a successful append after a failed one. Segment={}, WriterId={}, FailedEventNumber={}, AppendEventNumber={}",
                            append.getSegment(), append.getWriterId(), state.getLowestFailedEventNumber(), append.getEventNumber());
                }
            } else {
                if (conditionalFailed) {
                    log.debug("Conditional append failed due to incorrect offset: {}, {}", append, exception.getMessage());
                    synchronized (state.getAckLock()) {
                        // Revert the state to the last known good one. This is needed because we do not close the connection
                        // for offset-conditional append failures, hence we must revert the effects of the failed append.
                        state.conditionalAppendFailed(append.getEventNumber());
                        connection.send(new ConditionalCheckFailed(append.getWriterId(), append.getEventNumber(), append.getRequestId()));
                    }
                } else {
                    // Record the exception handling into the Writer State. It will be executed  once all the current
                    // in-flight appends are completed.
                    state.appendFailed(append.getEventNumber(), () ->
                            handleException(append.getWriterId(), append.getRequestId(), append.getSegment(), append.getEventNumber(),
                                    "appending data", exception));
                }
            }

            // After every append completes, check the WriterState and trigger any error handlers that are now eligible for execution.
            executeDelayedErrorHandler(state, append.getSegment(), append.getWriterId());
        } catch (Throwable e) {
            success = false;
            handleException(append.getWriterId(), append.getEventNumber(), append.getSegment(), "handling append result", e);
        }

        if (success) {
            // Record any necessary metrics or statistics, but after we have sent the ack back and initiated the next append.
            this.statsRecorder.recordAppend(append.getSegment(), append.getDataLength(), append.getEventCount(), elapsedTimer.getElapsed());
        }
    }

    /**
     * Inquires the {@link WriterState} for any eligible {@link WriterState.DelayedErrorHandler} that can be executed
     * right now. If so, invokes all eligible handlers synchronously. If there are no more handlers remaining after this,
     * the {@link WriterState} is unregistered, which would essentially force the client to reinvoke {@link #setupAppend}.
     *
     * @param state       The {@link WriterState} to query.
     * @param segmentName The name of the Segment for which the append failed.
     * @param writerId    The Writer Id of the Append.
     */
    private void executeDelayedErrorHandler(WriterState state, String segmentName, UUID writerId) {
        WriterState.DelayedErrorHandler h = state.fetchEligibleDelayedErrorHandler();
        if (h == null) {
            // This WriterState is healthy - nothing to do.
            return;
        }

        synchronized (state.getAckLock()) {
            try {
                // Execute all eligible delayed handlers. Note that this may be an empty list, which is OK - it means
                // the WriterState is not healthy but there's nothing yet to execute.
                h.getHandlersToExecute().forEach(Runnable::run);
            } finally {
                if (h.getHandlersRemaining() == 0) {
                    // We've executed all handlers and have none remaining. Time to clean up this WriterState and force
                    // the Client to reinitialize it via setupAppend().
                    this.writerStates.remove(Pair.of(segmentName, writerId));
                }
            }
        }
    }

    private void handleException(UUID writerId, long requestId, String segment, String doingWhat, Throwable u) {
        handleException(writerId, requestId, segment, -1L, doingWhat, u);
    }

    private void handleException(UUID writerId, long requestId, String segment, long eventNumber, String doingWhat, Throwable u) {
        if (u == null) {
            IllegalStateException exception = new IllegalStateException("No exception to handle.");
            log.error(requestId, "Append processor: Error {} on segment = '{}'", doingWhat, segment, exception);
            throw exception;
        }

        u = Exceptions.unwrap(u);
        String clientReplyStackTrace = replyWithStackTraceOnError ? Throwables.getStackTraceAsString(u) : EMPTY_STACK_TRACE;

        if (u instanceof StreamSegmentExistsException) {
            log.warn(requestId, "Segment '{}' already exists and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new SegmentAlreadyExists(requestId, segment, clientReplyStackTrace));
        } else if (u instanceof StreamSegmentNotExistsException) {
            log.warn(requestId, "Segment '{}' does not exist and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new NoSuchSegment(requestId, segment, clientReplyStackTrace, -1L));
        } else if (u instanceof StreamSegmentSealedException) {
            log.info("Segment '{}' is sealed and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new SegmentIsSealed(requestId, segment, clientReplyStackTrace, eventNumber));
        } else if (u instanceof ContainerNotFoundException) {
            int containerId = ((ContainerNotFoundException) u).getContainerId();
            log.warn(requestId, "Wrong host. Segment '{}' (Container {}) is not owned and {} cannot perform operation '{}'.",
                    segment, containerId, writerId, doingWhat);
            connection.send(new WrongHost(requestId, segment, "", clientReplyStackTrace));
        } else if (u instanceof BadAttributeUpdateException) {
            log.warn(requestId, "Bad attribute update by {} on segment {}.", writerId, segment, u);
            connection.send(new InvalidEventNumber(writerId, requestId, clientReplyStackTrace));
            connection.close();
        } else if (u instanceof TokenExpiredException) {
            log.warn(requestId, "Token expired for writer {} on segment {}.", writerId, segment, u);
            connection.close();
        } else if (u instanceof TokenException) {
            log.warn(requestId, "Token check failed or writer {} on segment {}.", writerId, segment, u);
            connection.send(new WireCommands.AuthTokenCheckFailed(requestId, clientReplyStackTrace,
                    WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
        } else if (u instanceof UnsupportedOperationException) {
            log.warn(requestId, "Unsupported Operation '{}'.", doingWhat, u);
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
}
