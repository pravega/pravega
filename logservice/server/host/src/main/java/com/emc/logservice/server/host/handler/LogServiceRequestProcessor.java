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

package com.emc.logservice.server.host.handler;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.SegmentProperties;
import com.emc.logservice.contracts.StreamSegmentExistsException;
import com.emc.logservice.contracts.StreamSegmentNotExistsException;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.contracts.WrongHostException;
import com.emc.nautilus.common.netty.FailingRequestProcessor;
import com.emc.nautilus.common.netty.RequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogServiceRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    private final StreamSegmentStore segmentStore;

    private final ServerConnection connection;

    public LogServiceRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection) {
        this.segmentStore = segmentStore;
        this.connection = connection;
    }

    @Override
    public void readSegment(ReadSegment readSegment) {
        CompletableFuture<ReadResult> future = segmentStore
                .read(readSegment.getSegment(), readSegment.getOffset(), readSegment.getSuggestedLength(), TIMEOUT);
        future.handle(new BiFunction<ReadResult, Throwable, Void>() {
            @Override
            public Void apply(ReadResult t, Throwable u) {
                try {
                    if (t != null) {
                        // TODO: Return data...
                        // This really should stream the data out in multiple results as
                        // it is available.
                    } else {
                        handleException(readSegment.getSegment(), "Read segment", u);
                    }
                } catch (Throwable e) {
                    handleException(readSegment.getSegment(), "Read segment", e);
                }
                return null;
            }
        });
    }

    @Override
    public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamSegmentInfo) {
        String segmentName = getStreamSegmentInfo.getSegmentName();
        CompletableFuture<SegmentProperties> future = segmentStore.getStreamSegmentInfo(segmentName, TIMEOUT);
        future.handle(new BiFunction<SegmentProperties, Throwable, Void>() {
            @Override
            public Void apply(SegmentProperties properties, Throwable u) {
                try {
                    if (properties != null) {
                        StreamSegmentInfo result = new StreamSegmentInfo(properties.getName(),
                                true,
                                properties.isSealed(),
                                properties.isDeleted(),
                                properties.getLastModified().getTime(),
                                properties.getLength());
                        connection.send(result);
                    } else {
                        connection.send(new StreamSegmentInfo(segmentName, false, true, true, 0, 0));
                    }
                } catch (Throwable e) {
                    handleException(segmentName, "Get segment info", e);
                }
                return null;
            }
        });
    }

    @Override
    public void createSegment(CreateSegment createStreamsSegment) {
        CompletableFuture<Void> future = segmentStore.createStreamSegment(createStreamsSegment.getSegment(), TIMEOUT);
        future.handle(new BiFunction<Void, Throwable, Void>() {
            @Override
            public Void apply(Void t, Throwable u) {
                try {
                    if (u == null) {
                        connection.send(new SegmentCreated(createStreamsSegment.getSegment()));
                    } else {
                        handleException(createStreamsSegment.getSegment(), "Create segment", u);
                    }
                } catch (Throwable e) {
                    handleException(createStreamsSegment.getSegment(), "Create segment", e);
                }
                return null;
            }
        });
    }

    // TODO: Duplicated in AppendProcessor.
    private void handleException(String segment, String operation, Throwable u) {
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
            // TODO: don't know what to do here...
            connection.close();
            log.error("Unknown excpetion on " + operation + " for segment " + segment, u);
            throw new IllegalStateException("Unknown exception.", u);
        }
    }

    //
    // @Override
    // public void createBatch(CreateBatch createBatch) {
    // getNextRequestProcessor().createBatch(createBatch);
    // }
    //
    // @Override
    // public void mergeBatch(MergeBatch mergeBatch) {
    // getNextRequestProcessor().mergeBatch(mergeBatch);
    // }
    //
    // @Override
    // public void sealSegment(SealSegment sealSegment) {
    // getNextRequestProcessor().sealSegment(sealSegment);
    // }
    //
    // @Override
    // public void deleteSegment(DeleteSegment deleteSegment) {
    // getNextRequestProcessor().deleteSegment(deleteSegment);
    // }
}
