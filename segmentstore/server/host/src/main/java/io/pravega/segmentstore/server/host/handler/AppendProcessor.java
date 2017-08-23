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

import com.google.common.collect.LinkedListMultimap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.WrongHostException;
import io.pravega.segmentstore.server.SegmentMetadata;
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
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_EVENTS;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_LATENCY;
import static io.pravega.shared.MetricsNames.nameFromSegment;

/**
 * Process incoming Append requests and write them to the appropriate store.
 */
@Slf4j
public class AppendProcessor extends DelegatingRequestProcessor {

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final int HIGH_WATER_MARK = 128 * 1024;
    private static final int LOW_WATER_MARK = 64 * 1024;

    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    private static final OpStatsLogger WRITE_STREAM_SEGMENT = STATS_LOGGER.createStats(SEGMENT_WRITE_LATENCY);

    private final StreamSegmentStore store;
    private final ServerConnection connection;
    private final RequestProcessor next;
    private final Object lock = new Object();
    private final SegmentStatsRecorder statsRecorder;

    @GuardedBy("lock")
    private final LinkedListMultimap<UUID, Append> waitingAppends = LinkedListMultimap.create(2);
    @GuardedBy("lock")
    private final HashMap<Pair<String, UUID>, Long> latestEventNumbers = new HashMap<>();
    @GuardedBy("lock")
    private Append outstandingAppend = null;

    public AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next) {
        this(store, connection, next, null);
    }

    public AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next, SegmentStatsRecorder statsRecorder) {
        this.store = store;
        this.connection = connection;
        this.next = next;
        this.statsRecorder = statsRecorder;
    }

    @Override
    public void hello(Hello hello) {
        log.info("Received hello from connection: {}", connection);
        connection.send(new Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATABLE_VERSION));
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
        store.getStreamSegmentInfo(newSegment, true, TIMEOUT)
                .whenComplete((info, u) -> {
                    try {
                        if (u != null) {
                            handleException(writer, setupAppend.getRequestId(), newSegment, "setting up append", u);
                        } else {
                            long eventNumber = info.getAttributes().getOrDefault(writer, SegmentMetadata.NULL_ATTRIBUTE_VALUE);
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
    public void performNextWrite() {
        Append append = getNextAppend();
        if (append == null) {
            return;
        }
        long traceId = LoggerHelpers.traceEnter(log, "storeAppend", append);
        Timer timer = new Timer();
        storeAppend(append).whenComplete((v, e) -> {
            handleAppendResult(append, e);
            LoggerHelpers.traceLeave(log, "storeAppend", traceId, v, e);
            if (e == null) {
                WRITE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
            } else {
                WRITE_STREAM_SEGMENT.reportFailEvent(timer.getElapsed());
            }
        }).whenComplete((v, e) -> {
            append.getData().release();  
        });
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
        ArrayList<AttributeUpdate> attributes = new ArrayList<>(2);
        synchronized (lock) {
            long lastEventNumber = latestEventNumbers.get(Pair.of(append.getSegment(), append.getWriterId()));
            attributes.add(new AttributeUpdate(append.getWriterId(), AttributeUpdateType.ReplaceIfEquals,
                                               append.getEventNumber(), lastEventNumber));
        }
        attributes.add(new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, append.getEventCount()));
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
            boolean conditionalFailed = exception != null && (ExceptionHelpers.getRealException(exception) instanceof BadOffsetException);
            synchronized (lock) {
                if (outstandingAppend != append) {
                    throw new IllegalStateException(
                            "Synchronization error in: " + AppendProcessor.this.getClass().getName());
                }
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
      
            if (exception != null) {
                if (conditionalFailed) {
                    log.debug("Conditional apend failed due to incorrect offset: {}, {}", append, exception.getMessage());
                    connection.send(new ConditionalCheckFailed(append.getWriterId(), append.getEventNumber()));
                } else {
                    handleException(append.getWriterId(), append.getEventNumber(), append.getSegment(), "appending data", exception);
                }
            } else {
                if (statsRecorder != null) {
                    statsRecorder.record(append.getSegment(), append.getDataLength(), append.getEventCount());
                }
                connection.send(new DataAppended(append.getWriterId(), append.getEventNumber()));
                DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_WRITE_BYTES, append.getSegment()), append.getDataLength());
                DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_WRITE_EVENTS, append.getSegment()), append.getEventCount());
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
            log.error("Append processor: Error {} onsegment = '{}'", doingWhat, segment, exception);
            throw exception;
        }

        if (u instanceof CompletionException) {
            u = u.getCause();
        }

        log.error("Error (Segment = '{}', Operation = 'append')", segment, u);
        if (u instanceof StreamSegmentExistsException) {
            connection.send(new SegmentAlreadyExists(requestId, segment));
        } else if (u instanceof StreamSegmentNotExistsException) {
            connection.send(new NoSuchSegment(requestId, segment));
        } else if (u instanceof StreamSegmentSealedException) {
            connection.send(new SegmentIsSealed(requestId, segment));
        } else if (u instanceof WrongHostException) {
            WrongHostException wrongHost = (WrongHostException) u;
            connection.send(new WrongHost(requestId, wrongHost.getStreamSegmentName(), wrongHost.getCorrectHost()));
        } else if (u instanceof ContainerNotFoundException) {
            connection.send(new WrongHost(requestId, segment, ""));
        } else if (u instanceof BadAttributeUpdateException) {
            connection.send(new InvalidEventNumber(writerId, requestId));
            connection.close();
        } else {
            // TODO: don't know what to do here...
            connection.close();
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
        synchronized (lock) {
            UUID id = append.getWriterId();
            Long lastEventNumber = latestEventNumbers.get(Pair.of(append.getSegment(), id));
            if (lastEventNumber == null) {
                throw new IllegalStateException("Data from unexpected connection: " + id);
            }
            if (append.getEventNumber() <= lastEventNumber) {
                throw new IllegalStateException("Event was already appended.");
            }
            waitingAppends.put(id, append);
        }
        pauseOrResumeReading();
        performNextWrite();
    }

    @Override
    public RequestProcessor getNextRequestProcessor() {
        return next;
    }
}
