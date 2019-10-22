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
import com.google.common.collect.LinkedListMultimap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
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
    private static final int HIGH_WATER_MARK = 1024 * 1024; // 1MB
    private static final int LOW_WATER_MARK = 640 * 1024;  // 640KB
    private static final String EMPTY_STACK_TRACE = "";
    private final StreamSegmentStore store;
    private final ServerConnection connection;
    private final AtomicLong pendingBytes = new AtomicLong(0);

    @Getter
    private final RequestProcessor nextRequestProcessor;
    private final Object lock = new Object();
    private final SegmentStatsRecorder statsRecorder;
    private final DelegationTokenVerifier tokenVerifier;
    private final boolean replyWithStackTraceOnError;

    @GuardedBy("lock")
    private final LinkedListMultimap<UUID, Append> waitingAppends = LinkedListMultimap.create(2);
    @GuardedBy("lock")
    private final HashMap<Pair<String, UUID>, Long> latestEventNumbers = new HashMap<>();
    @GuardedBy("lock")
    private Append outstandingAppend = null;

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
                            synchronized (lock) {
                                latestEventNumbers.putIfAbsent(Pair.of(newSegment, writer), eventNumber);
                            }
                            connection.send(new AppendSetup(setupAppend.getRequestId(), newSegment, writer, eventNumber));
                        }
                    } catch (Throwable e) {
                        handleException(writer, setupAppend.getRequestId(), newSegment, "handling setupAppend result", e);
                    }
                });
    }

    /**
     * If there isn't already an append outstanding against the store, write a new one.
     * Appends are opportunistically batched here. i.e. If many are waiting they are combined into a single append and
     * that is written.
     */
    private void performNextWrite() {
        Append append = getNextAppend();
        if (append == null) {
            return;
        }
        long traceId = LoggerHelpers.traceEnter(log, "storeAppend", append);
        Timer timer = new Timer();
        storeAppend(append)
                .whenComplete((length, e) -> {
                    handleAppendResult(append, length, e, timer);
                    LoggerHelpers.traceLeave(log, "storeAppend", traceId, append, e);
                })
                .whenComplete((v, e) -> append.getData().release());
    }

    private Append getNextAppend() {
        synchronized (lock) {
            if (outstandingAppend != null || waitingAppends.isEmpty()) {
                return null;
            }
            UUID writer = waitingAppends.keys().iterator().next();
            List<Append> appends = waitingAppends.get(writer);
            if (appends.get(0).isConditional()) {
                outstandingAppend = appends.remove(0);
            } else {
                ByteBuf[] toAppend = new ByteBuf[appends.size()];
                Append last = appends.get(0);
                int eventCount = 0;
                int i = -1;
                for (Iterator<Append> iterator = appends.iterator(); iterator.hasNext(); ) {
                    Append a = iterator.next();
                    if (a.isConditional()) {
                        break;
                    }
                    i++;
                    toAppend[i] = a.getData();
                    last = a;
                    eventCount += a.getEventCount();
                    iterator.remove();
                }
                ByteBuf data = Unpooled.wrappedBuffer(toAppend);
                String segment = last.getSegment();
                long eventNumber = last.getEventNumber();
                outstandingAppend = new Append(segment, writer, eventNumber, eventCount, data, null, last.getRequestId());
            }
            setPendingBytes(getPendingBytes() - outstandingAppend.getData().readableBytes());
            return outstandingAppend;
        }
    }

    private CompletableFuture<Long> storeAppend(Append append) {
        long lastEventNumber;
        synchronized (lock) {
            lastEventNumber = latestEventNumbers.get(Pair.of(append.getSegment(), append.getWriterId()));
        }

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

    private void handleAppendResult(final Append append, Long newWriteOffset, Throwable exception, Timer elapsedTimer) {
        boolean success = exception == null;
        try {
            boolean conditionalFailed = !success && (Exceptions.unwrap(exception) instanceof BadOffsetException);
            long previousEventNumber;
            synchronized (lock) {
                previousEventNumber = latestEventNumbers.get(Pair.of(append.getSegment(), append.getWriterId()));
                Preconditions.checkState(outstandingAppend == append,
                        "Synchronization error in: %s while processing append: %s.",
                        AppendProcessor.this.getClass().getName(), append);
            }

            if (success) {
                final DataAppended dataAppendedAck = new DataAppended(append.getRequestId(), append.getWriterId(), append.getEventNumber(),
                        previousEventNumber, newWriteOffset);
                log.trace("Sending DataAppended : {}", dataAppendedAck);
                connection.send(dataAppendedAck);
            } else {
                if (conditionalFailed) {
                    log.debug("Conditional append failed due to incorrect offset: {}, {}", append, exception.getMessage());
                    connection.send(new ConditionalCheckFailed(append.getWriterId(), append.getEventNumber(), append.getRequestId()));
                } else {
                    handleException(append.getWriterId(), append.getRequestId(), append.getSegment(), append.getEventNumber(),
                                    "appending data", exception);
                }
            }

            /* Reply (DataAppended in case of success, else an error Reply based on exception) has been sent. Next,
             *   - clear outstandingAppend to handle the next Append message.
             *   - ensure latestEventNumbers and waitingAppends are updated.
             */
            synchronized (lock) {
                Preconditions.checkState(outstandingAppend == append,
                        "Synchronization error in: %s while processing append: %s.",
                        AppendProcessor.this.getClass().getName(), append);
                outstandingAppend = null;
                if (exception == null) {
                    latestEventNumbers.put(Pair.of(append.getSegment(), append.getWriterId()), append.getEventNumber());
                } else {
                    if (!conditionalFailed) {
                        long bytes = waitingAppends.removeAll(append.getWriterId())
                                .stream().mapToInt(a -> a.getData().readableBytes()).sum();
                        setPendingBytes(getPendingBytes() - bytes);
                        latestEventNumbers.remove(Pair.of(append.getSegment(), append.getWriterId()));
                    }
                }
            }
      
            pauseOrResumeReading();
            performNextWrite();
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

    /**
     * If there is too much data waiting throttle the producer by stopping consumption from the socket.
     * If there is room for more data, we resume consuming from the socket.
     */
    private void pauseOrResumeReading() {
        long bytesWaiting = getPendingBytes();

        if (bytesWaiting > HIGH_WATER_MARK) {
            log.debug("Pausing writing from connection {}", connection);
            connection.pauseReading();
        }
        if (bytesWaiting < LOW_WATER_MARK) {
            log.trace("Resuming writing from connection {}", connection);
            connection.resumeReading();
        }
    }

    /**
     * return the remaining bytes to be consumed.
     */
    protected long getPendingBytes() {
        return pendingBytes.get();
    }

    /**
     * set the remaining bytes to be consumed.
     * Make sure that negative value is not set.
     *
     * @param val  New value to set.
     */
    protected void setPendingBytes(long val) {
        pendingBytes.set(Math.max(val, 0));
    }

    /**
     * Append data to the store.
     * Because ordering dictates that there only be one outstanding append from a given connection, this is implemented
     * by adding the append to a queue.
     */
    @Override
    public void append(Append append) {
        log.trace("Processing append received from client {}", append);
        UUID id = append.getWriterId();
        synchronized (lock) {
            Long lastEventNumber = latestEventNumbers.get(Pair.of(append.getSegment(), id));
            Preconditions.checkState(lastEventNumber != null, "Data from unexpected connection: %s.", id);
            Preconditions.checkState(append.getEventNumber() >= lastEventNumber, "Event was already appended.");
            waitingAppends.put(id, append);
        }
        setPendingBytes(getPendingBytes() + append.getData().readableBytes());
        pauseOrResumeReading();
        performNextWrite();
    }
    //endregion
}
