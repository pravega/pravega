/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.host.handler;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Timer;
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
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.AttributeUpdateType;
import io.pravega.service.contracts.BadOffsetException;
import io.pravega.service.contracts.StreamSegmentExistsException;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.service.contracts.StreamSegmentSealedException;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.contracts.WrongHostException;
import io.pravega.service.server.SegmentMetadata;
import io.pravega.service.server.host.stat.SegmentStatsRecorder;
import com.google.common.collect.LinkedListMultimap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.concurrent.GuardedBy;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_LATENCY;
import static io.pravega.shared.MetricsNames.nameFromSegment;
import static io.pravega.service.contracts.Attributes.EVENT_COUNT;

/**
 * Process incoming Append requests and write them to the appropriate store.
 */
@Slf4j
public class AppendProcessor extends DelegatingRequestProcessor {

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final int HIGH_WATER_MARK = 128 * 1024;
    private static final int LOW_WATER_MARK = 64 * 1024;

    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("host");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    static final OpStatsLogger WRITE_STREAM_SEGMENT = STATS_LOGGER.createStats(SEGMENT_WRITE_LATENCY);

    private final StreamSegmentStore store;
    private final ServerConnection connection;
    private final RequestProcessor next;
    private final Object lock = new Object();
    private final SegmentStatsRecorder statsRecorder;

    @GuardedBy("lock")
    private final LinkedListMultimap<UUID, Append> waitingAppends = LinkedListMultimap.create(2);
    @GuardedBy("lock")
    private final HashMap<UUID, Long> latestEventNumbers = new HashMap<>();
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
        store.getStreamSegmentInfo(newSegment, true, TIMEOUT)
                .whenComplete((info, u) -> {
                    try {
                        if (u != null) {
                            handleException(setupAppend.getRequestId(), newSegment, "setting up append", u);
                        } else {
                            long eventNumber = info.getAttributes().getOrDefault(writer, SegmentMetadata.NULL_ATTRIBUTE_VALUE);
                            if (eventNumber == SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                                // First append to this segment.
                                eventNumber = 0;
                            }

                            synchronized (lock) {
                                latestEventNumbers.putIfAbsent(writer, eventNumber);
                            }
                            connection.send(new AppendSetup(setupAppend.getRequestId(), newSegment, writer, eventNumber));
                        }
                    } catch (Throwable e) {
                        handleException(setupAppend.getRequestId(), newSegment, "handling setupAppend result", e);
                    }
                });
    }

    /**
     * If there isn't already an append outstanding against the store, write a new one.
     * Appends are opportunistically batched here. i.e. If many are waiting they are combined into a single append and
     * that is written.
     */
    public void performNextWrite() {
        Append append;
        long numOfEvents = 1;

        synchronized (lock) {
            if (outstandingAppend != null || waitingAppends.isEmpty()) {
                return;
            }

            UUID writer = waitingAppends.keys().iterator().next();
            List<Append> appends = waitingAppends.get(writer);
            if (appends.get(0).isConditional()) {
                append = appends.remove(0);
            } else {
                ByteBuf[] toAppend = new ByteBuf[appends.size()];
                Append first = appends.get(0);
                Append last = first;
                int i = -1;
                for (Iterator<Append> iterator = appends.iterator(); iterator.hasNext(); ) {
                    Append a = iterator.next();
                    if (a.isConditional()) {
                        break;
                    }
                    i++;
                    toAppend[i] = a.getData();
                    last = a;
                    iterator.remove();
                }
                ByteBuf data = Unpooled.wrappedBuffer(toAppend);
                if (last != null) {
                    numOfEvents = last.getEventNumber() - first.getEventNumber() + 1;
                }

                String segment = last != null ? last.getSegment() : first.getSegment();
                long eventNumber = last != null ? last.getEventNumber() : first.getEventNumber();
                append = new Append(segment, writer, eventNumber, data, null);
            }
            outstandingAppend = append;
        }
        write(append, numOfEvents);
    }

    /**
     * Write the provided append to the store, and upon completion ack it back to the producer.
     */
    private void write(final Append toWrite, long numOfEvents) {
        Timer timer = new Timer();
        ByteBuf buf = toWrite.getData().asReadOnly();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        val attributes = Arrays.asList(new AttributeUpdate(
                        toWrite.getWriterId(),
                        AttributeUpdateType.ReplaceIfGreater,
                        toWrite.getEventNumber()),
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, numOfEvents));

        CompletableFuture<Void> future;
        String segment = toWrite.getSegment();
        if (toWrite.isConditional()) {
            future = store.append(segment, toWrite.getExpectedLength(), bytes, attributes, TIMEOUT);
        } else {
            future = store.append(segment, bytes, attributes, TIMEOUT);
        }
        future.whenComplete((t, u) -> {
            try {
                boolean conditionalFailed = u != null && (ExceptionHelpers.getRealException(u) instanceof BadOffsetException);
                synchronized (lock) {
                    if (outstandingAppend != toWrite) {
                        throw new IllegalStateException(
                                "Synchronization error in: " + AppendProcessor.this.getClass().getName());
                    }

                    toWrite.getData().release();
                    outstandingAppend = null;
                    if (u != null && !conditionalFailed) {
                        waitingAppends.removeAll(toWrite.getWriterId());
                        latestEventNumbers.remove(toWrite.getWriterId());
                    }
                }

                if (u != null) {
                    if (conditionalFailed) {
                        connection.send(new ConditionalCheckFailed(toWrite.getWriterId(), toWrite.getEventNumber()));
                    } else {
                        handleException(toWrite.getEventNumber(), segment, "appending data", u);
                    }
                } else {
                    DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_WRITE_BYTES, toWrite.getSegment()), bytes.length);
                    WRITE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
                    connection.send(new DataAppended(toWrite.getWriterId(), toWrite.getEventNumber()));

                    if (statsRecorder != null) {
                        statsRecorder.record(segment, bytes.length, (int) numOfEvents);
                    }
                }

                pauseOrResumeReading();
                performNextWrite();
            } catch (Throwable e) {
                handleException(toWrite.getEventNumber(), segment, "handling append result", e);
            }
        });
    }

    private void handleException(long requestId, String segment, String doingWhat, Throwable u) {
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
            connection.pauseReading();
        }
        if (bytesWaiting < LOW_WATER_MARK) {
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
        synchronized (lock) {
            UUID id = append.getWriterId();
            Long lastEventNumber = latestEventNumbers.get(id);
            if (lastEventNumber == null) {
                throw new IllegalStateException("Data from unexpected connection: " + id);
            }
            if (append.getEventNumber() <= lastEventNumber) {
                throw new IllegalStateException("Event was already appended.");
            }
            latestEventNumbers.put(id, append.getEventNumber());
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
