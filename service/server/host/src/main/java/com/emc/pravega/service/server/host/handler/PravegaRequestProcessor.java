/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

import com.emc.pravega.common.netty.FailingRequestProcessor;
import com.emc.pravega.common.netty.RequestProcessor;
import com.emc.pravega.common.netty.ServerConnection;
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.contracts.WrongHostException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

    static final Duration TIMEOUT = Duration.ofMinutes(1);

    private final StreamSegmentStore segmentStore;

    private final ServerConnection connection;

    public PravegaRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection) {
        this.segmentStore = segmentStore;
        this.connection = connection;
    }

    @Override
    public void readSegment(ReadSegment readSegment) {
        final String segment = readSegment.getSegment();
        CompletableFuture<ReadResult> future = segmentStore
            .read(segment, readSegment.getOffset(), readSegment.getSuggestedLength(), TIMEOUT);
        future.thenApply((ReadResult t) -> {
            ArrayList<ReadResultEntry> cachedEntries = new ArrayList<>();
            ReadResultEntry nonCachedEntry = null;
            while (t.hasNext()) {
                ReadResultEntry entry = t.next();
                if (entry.getType() == ReadResultEntryType.Cache) {
                    cachedEntries.add(entry);
                } else {
                    nonCachedEntry = entry;
                    break;
                }
            }
            boolean endOfSegment = nonCachedEntry != null && nonCachedEntry.getType() == ReadResultEntryType.EndOfStreamSegment;
            boolean atTail = !endOfSegment && (nonCachedEntry == null || nonCachedEntry.getType() == ReadResultEntryType.Future);

            SegmentRead reply = createSegmentRead(segment, readSegment.getOffset(), atTail, endOfSegment, cachedEntries);

            if (reply != null) {
                connection.send(reply);
            }
            // If the data we returned is short or non-existent, additional data should be sent when available.
            if ((reply == null || reply.getData().remaining() < readSegment.getSuggestedLength() / 2) && nonCachedEntry != null) {
                nonCachedEntry.requestContent(TIMEOUT);
                final long offset = nonCachedEntry.getStreamSegmentOffset();
                nonCachedEntry.getContent().thenApply((ReadResultEntryContents contents) -> {
                    ByteBuffer data = ByteBuffer.allocate(contents.getLength());
                    try {
                        contents.getData().read(data.array());
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                    connection.send(new SegmentRead(segment, offset, atTail, endOfSegment, data));
                    return null;
                }).exceptionally((Throwable e) -> {
                    handleException(segment, "Read segment", e);
                    return null;
                });
            }
            return null;
        }).exceptionally((Throwable t) -> {
            handleException(segment, "Read segment", t);
            return null;
        });
    }

    private SegmentRead createSegmentRead(String segment, long initailOffset, boolean atTail, boolean endOfSegment,
            List<ReadResultEntry> entries) {
        if (entries.isEmpty()) {
            return null;
        }
        long expectedOffset = initailOffset;
        int totalSize = 0;
        //Validate and find total size
        for (ReadResultEntry entry : entries) {
            if (entry.getType() != ReadResultEntryType.Cache) {
                throw new IllegalArgumentException("Only cached entries supported.");
            }
            if (entry.getStreamSegmentOffset() != expectedOffset) {
                throw new IllegalStateException(
                        "There was a gap in the data read between: " + expectedOffset + " and " + entry.getStreamSegmentOffset());
            }
            ReadResultEntryContents content = entry.getContent().getNow(null);
            expectedOffset += content.getLength();
            totalSize += content.getLength();
        }
        //Copy Data
        ByteBuffer data = ByteBuffer.allocate(totalSize);
        int bytesCopied = 0;
        for (ReadResultEntry entry : entries) {
            ReadResultEntryContents content = entry.getContent().getNow(null);
            int copied;
            try {
                copied = content.getData().read(data.array(), bytesCopied, totalSize-bytesCopied);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (copied != content.getLength()) {
                throw new IllegalStateException("Bug in InputStream.read!?!");
            }
            bytesCopied += copied;
        }
        return new SegmentRead(segment, initailOffset, atTail, endOfSegment, data);

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
