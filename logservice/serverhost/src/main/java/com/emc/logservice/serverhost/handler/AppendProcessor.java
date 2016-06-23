/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.serverhost.handler;

import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.StreamSegmentExistsException;
import com.emc.logservice.contracts.StreamSegmentNotExistsException;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.contracts.WrongHostException;
import com.emc.nautilus.common.netty.DelegatingRequestProcessor;
import com.emc.nautilus.common.netty.RequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

@Slf4j
public class AppendProcessor extends DelegatingRequestProcessor {

    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    static final int HIGH_WATER_MARK = 128 * 1024;
    static final int LOW_WATER_MARK = 64 * 1024;

    private final StreamSegmentStore store;
    private final ServerConnection connection;
    private final RequestProcessor next;

    private final Object lock = new Object();
    private String segment;
    private UUID connectionId;
    private long connectionOffset = 0L;
    private List<ByteBuf> waiting = new ArrayList<>();
    private OutstandingWrite outstandingWrite = null;

    public AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next) {
        this.store = store;
        this.connection = connection;
        this.next = next;
    }

    @Data
    private class OutstandingWrite {
        final ByteBuf[] dataToBeAppended;
        final String segment;
        final UUID connectionId;
        final long ackOffset;
    }

    @Override
    public void setupAppend(SetupAppend setupAppend) {
        String newSegment = setupAppend.getSegment();
        // String owner = store.whoOwnStreamSegment(newSegment);
        // if (owner != null) {
        //connection.send(new WrongHost(newSegment, owner));
        //return;
        //}
        UUID newConnection = setupAppend.getConnectionId();
        CompletableFuture<AppendContext> future = store.getLastAppendContext(newSegment, newConnection);
        future.handle(new BiFunction<AppendContext, Throwable, Void>() {
            @Override
            public Void apply(AppendContext info, Throwable u) {
                try {
                    if (u != null) {
                        handleException(newSegment, u);
                        return null;
                    }
                    long offset;
                    if (info == null) {
                        offset = 0;
                    } else {
                        if (!info.getClientId().equals(connectionId)) {
                            throw new IllegalStateException("Wrong connection Info returned");
                        }
                        offset = info.getClientOffset();
                    }
                    synchronized (lock) {
                        segment = newSegment;
                        connectionId = newConnection;
                        connectionOffset = offset;
                    }
                    connection.send(new AppendSetup(newSegment, newConnection, offset));
                } catch (Throwable e) {
                    handleException(newSegment, e);
                }
                return null;
            }
        });
    }

    public void performNextWrite() {
        OutstandingWrite write;
        synchronized (lock) {
            if (outstandingWrite != null || waiting.isEmpty()) {
                return;
            }
            ByteBuf[] data = new ByteBuf[waiting.size()];
            waiting.toArray(data);
            write = new OutstandingWrite(data, segment, connectionId, connectionOffset);
            waiting.clear();
            outstandingWrite = write;
        }
        write(write);
    }

    private void write(OutstandingWrite toWrite) {
        ByteBuf buf = Unpooled.unmodifiableBuffer(toWrite.getDataToBeAppended());
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        CompletableFuture<Long> future = store.append(toWrite.getSegment(), bytes, new AppendContext(toWrite.getConnectionId(), toWrite.ackOffset), TIMEOUT);
        future.handle(new BiFunction<Long, Throwable, Void>() {
            @Override
            public Void apply(Long t, Throwable u) {
                try {
                    synchronized (lock) {
                        if (outstandingWrite != toWrite) {
                            throw new IllegalStateException(
                                    "Synchronization error in: " + AppendProcessor.this.getClass().getName());
                        }
                        outstandingWrite = null;
                        if (u != null) {
                            segment = null;
                            connectionId = null;
                            connectionOffset = 0;
                            waiting.clear();
                        }
                    }
                    if (t == null) {
                        handleException(toWrite.getSegment(), u);
                    } else {
                        connection.send(new DataAppended(toWrite.segment, toWrite.ackOffset));
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

    //TODO: Duplicated in LogServiceRequestProcessor.
    private void handleException(String segment, Throwable u) {
        if (u == null) {
            throw new IllegalStateException("Neither offset nor exception!?");
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
            //TODO: don't know what to do here...
            connection.drop();
            log.error("Unknown excpetion on append for segment " + segment, u);
            throw new IllegalStateException("Unknown exception.", u);
        }
    }

    private void pauseOrResumeReading() {
        int bytesWaiting;
        synchronized (lock) {
            bytesWaiting = waiting.stream().mapToInt((ByteBuf b) -> b.readableBytes()).sum();
        }
        if (bytesWaiting > HIGH_WATER_MARK) {
            connection.pauseReading();
        }
        if (bytesWaiting < LOW_WATER_MARK) {
            connection.resumeReading();
        }
    }

    @Override
    public void appendData(AppendData appendData) {
        synchronized (lock) {
            ByteBuf data = appendData.getData();
            UUID id = appendData.getConnectionId();
            if (connectionId == null || !connectionId.equals(id)) {
                throw new IllegalStateException("Data from unexpected connection: " + id);
            }
            long expectedOffset = appendData.getConnectionOffset();
            waiting.add(data);
            connectionOffset += data.readableBytes();
            if (connectionOffset != expectedOffset) {
                throw new IllegalStateException("Data appended so far was not of the expected length.");
            }
        }
        pauseOrResumeReading();
        performNextWrite();
    }

    @Override
    public RequestProcessor getNextRequestProcessor() {
        return next;
    }
}
