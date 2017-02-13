/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.contracts.WrongHostException;
import com.google.common.collect.LinkedListMultimap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static com.emc.pravega.service.server.host.PravegaRequestStats.PENDING_APPEND_BYTES;

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
        CompletableFuture<AppendContext> future = store.getLastAppendContext(newSegment, newConnection, TIMEOUT);
        future.handle(new BiFunction<AppendContext, Throwable, Void>() {
            @Override
            public Void apply(AppendContext info, Throwable u) {
                try {
                    if (u != null) {
                        handleException(newSegment, u);
                        return null;
                    }
                    long eventNumber;
                    if (info == null) {
                        eventNumber = 0;
                    } else {
                        if (!info.getClientId().equals(newConnection)) {
                            throw new IllegalStateException("Wrong connection Info returned");
                        }
                        eventNumber = info.getEventNumber();
                    }
                    synchronized (lock) {
                        latestEventNumbers.putIfAbsent(newConnection, eventNumber);
                    }
                    connection.send(new AppendSetup(newSegment, newConnection, eventNumber));
                } catch (Throwable e) {
                    handleException(newSegment, e);
                }
                return null;
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
                Append last = null;
                int i = -1;
                for (Iterator<Append> iterator = appends.iterator(); iterator.hasNext();) {
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
                append = new Append(last.getSegment(), writer, last.getEventNumber(), data, null);
            }
            outstandingAppend = append;
        }
        write(append);
    }

    /**
     * Write the provided append to the store, and upon completion ack it back to the producer.
     */
    private void write(Append toWrite) {
        ByteBuf buf = Unpooled.unmodifiableBuffer(toWrite.getData());
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        AppendContext context = new AppendContext(toWrite.getConnectionId(), toWrite.getEventNumber());
        CompletableFuture<Void> future;
        String segment = toWrite.getSegment();
        if (toWrite.isConditional()) {
            future = store.append(segment, toWrite.getExpectedLength(), bytes, context, TIMEOUT);
        } else {
            future = store.append(segment, bytes, context, TIMEOUT);
        }
        future.handle(new BiFunction<Void, Throwable, Void>() {
            @Override
            public Void apply(Void t, Throwable u) {
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
                            waitingAppends.removeAll(context.getClientId());
                            latestEventNumbers.remove(context.getClientId());
                        }
                    }
                    if (u != null) {
                        if (conditionalFailed) {
                            connection.send(new ConditionalCheckFailed(context.getClientId(), context.getEventNumber()));
                        } else {
                            handleException(segment, u);
                        }
                    } else {
                        connection.send(new DataAppended(context.getClientId(),  context.getEventNumber()));
                    }
                    pauseOrResumeReading();
                    performNextWrite();
                } catch (Throwable e) {
                    handleException(segment, e);
                }
                return null;
            }
        });
    }

    // TODO: Duplicated in LogServiceRequestProcessor.
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
