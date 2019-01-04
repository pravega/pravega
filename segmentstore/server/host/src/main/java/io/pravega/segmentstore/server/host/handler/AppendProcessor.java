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
import io.pravega.auth.AuthenticationException;
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
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import io.pravega.shared.protocol.netty.Append;
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
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_EVENTS;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_LATENCY;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsNames.nameFromSegment;

/**
 * Process incoming Append requests and write them to the SegmentStore.
 */
@Slf4j
public class AppendProcessor extends DelegatingRequestProcessor {
    //region Members

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final int HIGH_WATER_MARK = 128 * 1024;
    private static final int LOW_WATER_MARK = 64 * 1024;
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final OpStatsLogger WRITE_STREAM_SEGMENT = STATS_LOGGER.createStats(SEGMENT_WRITE_LATENCY);
    private static final String EMPTY_STACK_TRACE = "";
    private final StreamSegmentStore store;
    private final ServerConnection connection;
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
        this(store, connection, next, null, verifier, false);
    }

    /**
     * Creates a new instance of the AppendProcessor class.
     *  @param store         The SegmentStore to send append requests to.
     * @param connection    The ServerConnection to send responses to.
     * @param next          The RequestProcessor to invoke next.
     * @param statsRecorder (Optional) A StatsRecorder to record Metrics.
     * @param tokenVerifier Delegation token verifier.
     * @param replyWithStackTraceOnError Whether client replies upon failed requests contain server-side stack traces or not.
     */
    AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next, SegmentStatsRecorder statsRecorder,
                    DelegationTokenVerifier tokenVerifier, boolean replyWithStackTraceOnError) {
        this.store = Preconditions.checkNotNull(store, "store");
        this.connection = Preconditions.checkNotNull(connection, "connection");
        this.nextRequestProcessor = Preconditions.checkNotNull(next, "next");
        this.statsRecorder = statsRecorder;
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
        if (this.tokenVerifier != null && !tokenVerifier.verifyToken(newSegment,
                setupAppend.getDelegationToken(), AuthHandler.Permissions.READ_UPDATE)) {
            log.warn("Delegation token verification failed");
            handleException(setupAppend.getWriterId(), setupAppend.getRequestId(), newSegment,
                    "Update Segment Attribute", new AuthenticationException("Token verification failed"));
            return;
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
                .whenComplete((v, e) -> {
                    handleAppendResult(append, e);
                    LoggerHelpers.traceLeave(log, "storeAppend", traceId, v, e);
                    if (e == null) {
                        WRITE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
                    } else {
                        WRITE_STREAM_SEGMENT.reportFailEvent(timer.getElapsed());
                    }
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
                outstandingAppend = new Append(segment, writer, eventNumber, eventCount, data, null);
            }
            return outstandingAppend;
        }
    }

    private CompletableFuture<Void> storeAppend(Append append) {
        long lastEventNumber;
        synchronized (lock) {
            lastEventNumber = latestEventNumbers.get(Pair.of(append.getSegment(), append.getWriterId()));
        }

        List<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(append.getWriterId(), AttributeUpdateType.ReplaceIfEquals, append.getEventNumber(), lastEventNumber),
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, append.getEventCount()));
        ByteBuf buf = append.getData().asReadOnly();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        if (append.isConditional()) {
            return store.append(append.getSegment(), append.getExpectedLength(), bytes, attributes, TIMEOUT);
        } else {
            return store.append(append.getSegment(), bytes, attributes, TIMEOUT);
        }
    }

    private void handleAppendResult(final Append append, Throwable exception) {
        try {
            boolean conditionalFailed = exception != null && (Exceptions.unwrap(exception) instanceof BadOffsetException);
            long previousEventNumber;
            synchronized (lock) {
                previousEventNumber = latestEventNumbers.get(Pair.of(append.getSegment(), append.getWriterId()));
                Preconditions.checkState(outstandingAppend == append,
                        "Synchronization error in: %s while processing append: %s.",
                        AppendProcessor.this.getClass().getName(), append);
            }
      
            if (exception != null) {
                if (conditionalFailed) {
                    log.debug("Conditional append failed due to incorrect offset: {}, {}", append, exception.getMessage());
                    connection.send(new ConditionalCheckFailed(append.getWriterId(), append.getEventNumber()));
                } else {
                    handleException(append.getWriterId(), append.getEventNumber(), append.getSegment(), "appending data", exception);
                }
            } else {
                if (statsRecorder != null && !StreamSegmentNameUtils.isTransactionSegment(append.getSegment())) {
                    statsRecorder.record(append.getSegment(), append.getDataLength(), append.getEventCount());
                }
                final DataAppended dataAppendedAck = new DataAppended(append.getWriterId(), append.getEventNumber(),
                        previousEventNumber);
                log.trace("Sending DataAppended : {}", dataAppendedAck);
                connection.send(dataAppendedAck);
                //Don't report metrics if segment is a transaction
                //Update the parent segment metrics, once the transaction is merged
                //TODO: https://github.com/pravega/pravega/issues/2570
                if (!StreamSegmentNameUtils.isTransactionSegment(append.getSegment())) {
                    DYNAMIC_LOGGER.incCounterValue(globalMetricName(SEGMENT_WRITE_BYTES), append.getDataLength());
                    DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_WRITE_BYTES, append.getSegment()), append.getDataLength());
                    DYNAMIC_LOGGER.incCounterValue(globalMetricName(SEGMENT_WRITE_EVENTS), append.getEventCount());
                    DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_WRITE_EVENTS, append.getSegment()), append.getEventCount());
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
                        waitingAppends.removeAll(append.getWriterId());
                        latestEventNumbers.remove(Pair.of(append.getSegment(), append.getWriterId()));
                    }
                }
            }
      
            pauseOrResumeReading();
            performNextWrite();
        } catch (Throwable e) {
            handleException(append.getWriterId(), append.getEventNumber(), append.getSegment(), "handling append result", e);
        }
    }

    private void handleException(UUID writerId, long requestId, String segment, String doingWhat, Throwable u) {
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
            connection.send(new NoSuchSegment(requestId, segment, clientReplyStackTrace));
        } else if (u instanceof StreamSegmentSealedException) {
            log.info("Segment '{}' is sealed and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new SegmentIsSealed(requestId, segment, clientReplyStackTrace));
        } else if (u instanceof ContainerNotFoundException) {
            int containerId = ((ContainerNotFoundException) u).getContainerId();
            log.warn("Wrong host. Segment '{}' (Container {}) is not owned and {} cannot perform operation '{}'.",
                    segment, containerId, writerId, doingWhat);
            connection.send(new WrongHost(requestId, segment, "", clientReplyStackTrace));
        } else if (u instanceof BadAttributeUpdateException) {
            log.warn("Bad attribute update by {} on segment {}.", writerId, segment, u);
            connection.send(new InvalidEventNumber(writerId, requestId, clientReplyStackTrace));
            connection.close();
        } else if (u instanceof AuthenticationException) {
            log.warn("Token check failed while being written by {} on segment {}.", writerId, segment, u);
            connection.send(new WireCommands.AuthTokenCheckFailed(requestId, clientReplyStackTrace));
            connection.close();
        } else if (u instanceof UnsupportedOperationException) {
            log.warn("Unsupported Operation '{}'.", doingWhat, u);
            connection.send(new OperationUnsupported(requestId, doingWhat, clientReplyStackTrace));
        } else {
            log.error("Error (Segment = '{}', Operation = 'append')", segment, u);
            connection.close(); // Closing connection should reinitialize things, and hopefully fix the problem
        }
    }

    /**
     * If there is too much data waiting throttle the producer by stopping consumption from the socket.
     * If there is room for more data, we resume consuming from the socket.
     */
    private void pauseOrResumeReading() {
        int bytesWaiting;
        synchronized (lock) {
            bytesWaiting = waitingAppends.values()
                    .stream()
                    .mapToInt(a -> a.getData().readableBytes())
                    .sum();
        }

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
        pauseOrResumeReading();
        performNextWrite();
    }

    @Override
    public void mergeTableSegments(WireCommands.MergeTableSegments mergeSegments) {
        // TODO:
    }

    @Override
    public void sealTableSegment(WireCommands.SealTableSegment sealTableSegment) {
        // TODO:
    }

    @Override
    public void createTableSegment(WireCommands.CreateTableSegment createTableSegment) {
        // TODO:
    }

    @Override
    public void deleteTableSegment(WireCommands.DeleteTableSegment deleteSegment) {
        // TODO:
    }

    @Override
    public void putTableEntries(WireCommands.PutTableEntry tableEntries) {
        // TODO:
    }

    //endregion
}
