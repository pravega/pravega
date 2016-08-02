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

import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.contracts.WrongHostException;
import com.emc.nautilus.common.netty.DelegatingRequestProcessor;
import com.emc.nautilus.common.netty.RequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommands.Append;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AppendProcessor extends DelegatingRequestProcessor {

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    static final int HIGH_WATER_MARK = 128 * 1024;
    static final int LOW_WATER_MARK = 64 * 1024;

    private final StreamSegmentStore store;
    private final ServerConnection connection;
    private final RequestProcessor next;

    private final Object lock = new Object();

    private final LinkedHashMap<UUID, List<Append>> waitingAppends = new LinkedHashMap<>();
    private final HashMap<UUID, Long> latestEventNumbers = new HashMap<>();
    private Append outstandingAppend = null;

    public AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next) {
        this.store = store;
        this.connection = connection;
        this.next = next;
    }

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

    public void performNextWrite() {
        Append append;
        synchronized (lock) {
            if (outstandingAppend != null || waitingAppends.isEmpty()) {
                return;
            }
            Entry<UUID, List<Append>> entry = removeFirst(waitingAppends);
            UUID writer = entry.getKey();
            List<Append> appends = entry.getValue();
            // NOTE: Not sorting the events because they should already be in order
            ByteBuf data = wrappedBuffer(appends.stream().map(a -> a.getData()).toArray(ByteBuf[]::new));
            Append lastAppend = appends.get(appends.size() - 1);
            append = new Append(lastAppend.getSegment(), writer, lastAppend.getEventNumber(), data);
            outstandingAppend = append;
        }
        write(append);
    }

    private static <K, V> Entry<K, V> removeFirst(LinkedHashMap<K, V> map) {
        Iterator<Entry<K, V>> iter = map.entrySet().iterator();
        Entry<K, V> first = iter.next();
        iter.remove();
        return first;
    }

    private void write(Append toWrite) {
        ByteBuf buf = Unpooled.unmodifiableBuffer(toWrite.getData());
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        CompletableFuture<Long> future = store
            .append(toWrite.getSegment(),
                    bytes,
                    new AppendContext(toWrite.getConnectionId(), toWrite.getEventNumber()),
                    TIMEOUT);
        future.handle(new BiFunction<Long, Throwable, Void>() {
            @Override
            public Void apply(Long t, Throwable u) {
                try {
                    synchronized (lock) {
                        if (outstandingAppend != toWrite) {
                            throw new IllegalStateException(
                                    "Synchronization error in: " + AppendProcessor.this.getClass().getName());
                        }
                        toWrite.getData().release();
                        outstandingAppend = null;
                        if (u != null) {
                            waitingAppends.remove(toWrite.getConnectionId());
                            latestEventNumbers.remove(toWrite.getConnectionId());
                        }
                    }
                    if (t == null) {
                        handleException(toWrite.getSegment(), u);
                    } else {
                        connection.send(new DataAppended(toWrite.getConnectionId(), toWrite.getEventNumber()));
                    }
                    pauseOrResumeReading();
                    performNextWrite();
                } catch (Throwable e) {
                    handleException(toWrite.getSegment(), e);
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
            log.error("Unknown excpetion on append for segment " + segment, u);
        }
    }

    private void pauseOrResumeReading() {
        int bytesWaiting;
        synchronized (lock) {
            bytesWaiting = waitingAppends.values()
                .stream()
                .flatMap(List::stream)
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
            List<Append> waiting = waitingAppends.get(id);
            if (waiting == null) {
                waiting = new ArrayList<>(2);
                waitingAppends.put(id, waiting);
            }
            waiting.add(append);
        }
        pauseOrResumeReading();
        performNextWrite();
    }

    @Override
    public RequestProcessor getNextRequestProcessor() {
        return next;
    }
}
