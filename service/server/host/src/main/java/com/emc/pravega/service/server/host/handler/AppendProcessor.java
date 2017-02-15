/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.emc.pravega.service.server.host.handler;

import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.common.metrics.StatsLogger;
import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.DelegatingRequestProcessor;
import com.emc.pravega.common.netty.RequestProcessor;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.ConditionalCheckFailed;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.service.contracts.AttributeUpdate;
import com.emc.pravega.service.contracts.AttributeUpdateType;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.contracts.WrongHostException;
import com.emc.pravega.service.server.SegmentMetadata;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static com.emc.pravega.common.SegmentStoreMetricsNames.PENDING_APPEND_BYTES;
import static com.emc.pravega.service.contracts.Attributes.EVENT_COUNT;

/**
 * Process incomming Append requests and write them to the appropriate store.
 */
@Slf4j
public class AppendProcessor extends DelegatingRequestProcessor {

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    static final int HIGH_WATER_MARK = 128 * 1024;
    static final int LOW_WATER_MARK = 64 * 1024;

    static AtomicLong pendBytes = new AtomicLong();
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("HOST");

    static {
        STATS_LOGGER.registerGauge(PENDING_APPEND_BYTES, pendBytes::get);
    }

    private final StreamSegmentStore store;
    private final ServerConnection connection;
    private final RequestProcessor next;
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final LinkedListMultimap<UUID, Append> waitingAppends = LinkedListMultimap.create(2);
    @GuardedBy("lock")
    private final HashMap<UUID, Long> latestEventNumbers = new HashMap<>();
    @GuardedBy("lock")
    private Append outstandingAppend = null;

    public AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next) {
        this.store = store;
        this.connection = connection;
        this.next = next;
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
        UUID newConnection = setupAppend.getConnectionId();
        store.getStreamSegmentInfo(newSegment, true, TIMEOUT)
                .whenComplete((info, u) -> {
                    try {
                        if (u != null) {
                            handleException(newSegment, u);
                        } else {
                            long eventNumber = info.getAttributes().getOrDefault(newConnection, SegmentMetadata.NULL_ATTRIBUTE_VALUE);
                            if (eventNumber == SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                                // First append to this segment.
                                eventNumber = 0;
                            }

                            synchronized (lock) {
                                latestEventNumbers.putIfAbsent(newConnection, eventNumber);
                            }
                            connection.send(new AppendSetup(newSegment, newConnection, eventNumber));
                        }
                    } catch (Throwable e) {
                        handleException(newSegment, e);
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
                
                String segment = Optional.ofNullable(last).map(Append::getSegment).orElse(first.getSegment());
                long eventNumber = Optional.ofNullable(last).map(Append::getEventNumber).orElse(first.getEventNumber());
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
        ByteBuf buf = Unpooled.unmodifiableBuffer(toWrite.getData());
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        val attributes = Sets.newHashSet(new AttributeUpdate(
                        toWrite.getConnectionId(),
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
                boolean conditionalFailed = u != null
                        && (u instanceof BadOffsetException || u.getCause() instanceof BadOffsetException);
                synchronized (lock) {
                    if (outstandingAppend != toWrite) {
                        throw new IllegalStateException(
                                "Synchronization error in: " + AppendProcessor.this.getClass().getName());
                    }

                    toWrite.getData().release();
                    outstandingAppend = null;
                    if (u != null && !conditionalFailed) {
                        waitingAppends.removeAll(toWrite.getConnectionId());
                        latestEventNumbers.remove(toWrite.getConnectionId());
                    }
                }

                if (u != null) {
                    if (conditionalFailed) {
                        connection.send(new ConditionalCheckFailed(toWrite.getConnectionId(), toWrite.getEventNumber()));
                    } else {
                        handleException(segment, u);
                    }
                } else {
                    connection.send(new DataAppended(toWrite.getConnectionId(), toWrite.getEventNumber()));
                }

                pauseOrResumeReading();
                performNextWrite();
            } catch (Throwable e) {
                handleException(segment, e);
            }
        });
    }

    private void handleException(String segment, Throwable u) {
        if (u == null) {
            IllegalStateException exception = new IllegalStateException("Neither offset nor exception!?");
            log.error("Error on segment: " + segment, exception);
            throw exception;
        }
        if (u instanceof CompletionException) {
            u = u.getCause();
        }
        if (u instanceof StreamSegmentExistsException) {
            connection.send(new SegmentAlreadyExists(segment));
        } else if (u instanceof StreamSegmentNotExistsException) {
            connection.send(new NoSuchSegment(segment));
        } else if (u instanceof StreamSegmentSealedException) {
            connection.send(new SegmentIsSealed(segment));
        } else if (u instanceof WrongHostException) {
            WrongHostException wrongHost = (WrongHostException) u;
            connection.send(new WrongHost(wrongHost.getStreamSegmentName(), wrongHost.getCorrectHost()));
        } else {
            // TODO: don't know what to do here...
            connection.close();
            log.error("Unknown exception on append for segment " + segment, u);
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
        // Registered gauge value
        pendBytes.set(bytesWaiting);

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
            UUID id = append.getConnectionId();
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
