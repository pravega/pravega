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

import com.emc.pravega.common.Timer;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.common.metrics.Counter;
import com.emc.pravega.common.metrics.DynamicLogger;
import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.common.metrics.OpStatsLogger;
import com.emc.pravega.common.metrics.StatsLogger;
import com.emc.pravega.common.netty.FailingRequestProcessor;
import com.emc.pravega.common.netty.RequestProcessor;
import com.emc.pravega.common.netty.WireCommands.AbortTransaction;
import com.emc.pravega.common.netty.WireCommands.CommitTransaction;
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.CreateTransaction;
import com.emc.pravega.common.netty.WireCommands.DeleteSegment;
import com.emc.pravega.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.GetTransactionInfo;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SealSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.SegmentDeleted;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.SegmentSealed;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.TransactionAborted;
import com.emc.pravega.common.netty.WireCommands.TransactionCommitted;
import com.emc.pravega.common.netty.WireCommands.TransactionCreated;
import com.emc.pravega.common.netty.WireCommands.TransactionInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.common.segment.StreamSegmentNameUtils;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.contracts.WrongHostException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.extern.slf4j.Slf4j;

import static com.emc.pravega.common.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static com.emc.pravega.service.contracts.ReadResultEntryType.Cache;
import static com.emc.pravega.service.contracts.ReadResultEntryType.EndOfStreamSegment;
import static com.emc.pravega.service.contracts.ReadResultEntryType.Future;
import static com.emc.pravega.service.server.host.PravegaRequestStats.ALL_READ_BYTES;
import static com.emc.pravega.service.server.host.PravegaRequestStats.CREATE_SEGMENT;
import static com.emc.pravega.service.server.host.PravegaRequestStats.READ_SEGMENT;
import static com.emc.pravega.service.server.host.PravegaRequestStats.SEGMENT_READ_BYTES;
import static java.lang.Math.max;
import static java.lang.Math.min;

@Slf4j
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    static final int MAX_READ_SIZE = 2 * 1024 * 1024;

    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("HOST");
    // A dynamic logger
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    public static class Metrics {
        static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(CREATE_SEGMENT);
        static final OpStatsLogger READ_STREAM_SEGMENT = STATS_LOGGER.createStats(READ_SEGMENT);
        static final OpStatsLogger READ_BYTES_STATS = STATS_LOGGER.createStats(SEGMENT_READ_BYTES);
        static final Counter READ_BYTES = STATS_LOGGER.createCounter(ALL_READ_BYTES);
    }

    private final StreamSegmentStore segmentStore;

    private final ServerConnection connection;

    public PravegaRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection) {
        this.segmentStore = segmentStore;
        this.connection = connection;
    }

    @Override
    public void readSegment(ReadSegment readSegment) {
        Timer timer = new Timer();
        final String segment = readSegment.getSegment();
        final int readSize = min(MAX_READ_SIZE, max(TYPE_PLUS_LENGTH_SIZE, readSegment.getSuggestedLength()));
        // A dynamic gauge records read offset of each readSegment for a segment
        DYNAMIC_LOGGER.reportGaugeValue("readSegment." + segment, readSegment.getOffset());

        CompletableFuture<ReadResult> future = segmentStore.read(segment, readSegment.getOffset(), readSize, TIMEOUT);
        future.thenApply((ReadResult t) -> {
            Metrics.READ_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
            DYNAMIC_LOGGER.incCounterValue("readSegment." + segment, 1);
            handleReadResult(readSegment, t);
            return null;
        }).exceptionally((Throwable t) -> {
            Metrics.READ_STREAM_SEGMENT.reportFailEvent(timer.getElapsed());
            handleException(segment, "Read segment", t);
            return null;
        });
    }

    /**
     * Handles a readResult.
     * If there are cached entries that can be returned without blocking only these are returned.
     * Otherwise the call will request the data and setup a callback to return the data when it is available.
     */
    private void handleReadResult(ReadSegment request, ReadResult result) {
        String segment = request.getSegment();
        ArrayList<ReadResultEntryContents> cachedEntries = new ArrayList<>();
        ReadResultEntry nonCachedEntry = collectCachedEntries(request.getOffset(), result, cachedEntries);

        boolean endOfSegment = nonCachedEntry != null && nonCachedEntry.getType() == EndOfStreamSegment;
        boolean atTail = nonCachedEntry != null && nonCachedEntry.getType() == Future;

        if (!cachedEntries.isEmpty()) {
            ByteBuffer data = copyData(cachedEntries);
            SegmentRead reply = new SegmentRead(segment, request.getOffset(), atTail, endOfSegment, data);
            connection.send(reply);
        } else {
            Preconditions.checkState(nonCachedEntry != null, "No ReadResultEntries returned from read!?");
            nonCachedEntry.requestContent(TIMEOUT);
            nonCachedEntry.getContent().thenApply((ReadResultEntryContents contents) -> {
                ByteBuffer data = copyData(Collections.singletonList(contents));
                SegmentRead reply = new SegmentRead(segment, nonCachedEntry.getStreamSegmentOffset(), false, endOfSegment, data);
                connection.send(reply);
                return null;
            }).exceptionally((Throwable e) -> {
                handleException(segment, "Read segment", e);
                return null;
            });
        }
    }

    /**
     * Reads all of the cachedEntries from the ReadResult and puts their content into the cachedEntries list.
     * Upon encountering a non-cached entry, it stops iterating and returns it.
     */
    private ReadResultEntry collectCachedEntries(long initialOffset, ReadResult readResult,
                                                 ArrayList<ReadResultEntryContents> cachedEntries) {
        long expectedOffset = initialOffset;
        while (readResult.hasNext()) {
            ReadResultEntry entry = readResult.next();
            if (entry.getType() == Cache) {
                Preconditions.checkState(entry.getStreamSegmentOffset() == expectedOffset,
                        "Data returned from read was not contiguous.");
                ReadResultEntryContents content = entry.getContent().getNow(null);
                expectedOffset += content.getLength();
                cachedEntries.add(content);
            } else {
                return entry;
            }
        }
        return null;
    }

    /**
     * Copy all of the contents provided into a byteBuffer and return it.
     */
    private ByteBuffer copyData(List<ReadResultEntryContents> contents) {
        int totalSize = contents.stream().mapToInt(ReadResultEntryContents::getLength).sum();

        Metrics.READ_BYTES_STATS.reportSuccessValue(totalSize);
        Metrics.READ_BYTES.add(totalSize);

        ByteBuffer data = ByteBuffer.allocate(totalSize);
        int bytesCopied = 0;
        for (ReadResultEntryContents content : contents) {
            try {
                int copied = StreamHelpers.readAll(content.getData(), data.array(), bytesCopied, totalSize - bytesCopied);
                Preconditions.checkState(copied == content.getLength(), "Read fewer bytes than available.");
                bytesCopied += copied;
            } catch (IOException e) {
                //Not possible
                throw new RuntimeException(e);
            }
        }
        return data;
    }

    @Override
    public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamSegmentInfo) {
        String segmentName = getStreamSegmentInfo.getSegmentName();
        CompletableFuture<SegmentProperties> future = segmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT);
        future.thenApply(properties -> {
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
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(segmentName, "Get segment info", e);
            return null;
        });
    }

    @Override
    public void getTransactionInfo(GetTransactionInfo request) {
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(request.getSegment(), request.getTxid());
        CompletableFuture<SegmentProperties> future = segmentStore.getStreamSegmentInfo(transactionName, false, TIMEOUT);
        future.thenApply(properties -> {
            if (properties != null) {
                TransactionInfo result = new TransactionInfo(request.getSegment(),
                        request.getTxid(),
                        transactionName,
                        !properties.isDeleted(),
                        properties.isSealed(),
                        properties.getLastModified().getTime(),
                        properties.getLength());
                connection.send(result);
            } else {
                connection.send(new TransactionInfo(request.getSegment(), request.getTxid(), transactionName, false, true, 0, 0));
            }
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(transactionName, "Get transaction info", e);
            return null;
        });
    }

    @Override
    public void createSegment(CreateSegment createStreamsSegment) {
        Timer timer = new Timer();
        CompletableFuture<Void> future = segmentStore.createStreamSegment(createStreamsSegment.getSegment(), null, TIMEOUT);
        future.thenApply((Void v) -> {
            Metrics.CREATE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
            connection.send(new SegmentCreated(createStreamsSegment.getSegment()));
            return null;
        }).exceptionally((Throwable e) -> {
            Metrics.CREATE_STREAM_SEGMENT.reportFailEvent(timer.getElapsed());
            handleException(createStreamsSegment.getSegment(), "Create segment", e);
            return null;
        });
    }

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
            log.error("Unknown exception on " + operation + " for segment " + segment, u);
            throw new IllegalStateException("Unknown exception.", u);
        }
    }

    @Override
    public void createTransaction(CreateTransaction createTransaction) {
        CompletableFuture<String> future = segmentStore.createTransaction(createTransaction.getSegment(), createTransaction.getTxid(), null, TIMEOUT);
        future.thenApply((String txName) -> {
            connection.send(new TransactionCreated(createTransaction.getSegment(), createTransaction.getTxid()));
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(createTransaction.getSegment(), "Create transaction", e);
            return null;
        });
    }

    @Override
    public void commitTransaction(CommitTransaction commitTx) {
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(commitTx.getSegment(), commitTx.getTxid());
        segmentStore.sealStreamSegment(transactionName, TIMEOUT).thenApply((Long length) -> {
            segmentStore.mergeTransaction(transactionName, TIMEOUT).thenApply((Long offset) -> {
                connection.send(new TransactionCommitted(commitTx.getSegment(), commitTx.getTxid()));
                return null;
            }).exceptionally((Throwable e) -> {
                handleException(transactionName, "Commit transaction", e);
                return null;
            });
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(transactionName, "Commit transaction", e);
            return null;
        });
    }

    @Override
    public void abortTransaction(AbortTransaction abortTx) {
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(abortTx.getSegment(), abortTx.getTxid());
        CompletableFuture<Void> future = segmentStore.deleteStreamSegment(transactionName, TIMEOUT);
        future.thenRun(() -> {
            connection.send(new TransactionAborted(abortTx.getSegment(), abortTx.getTxid()));
        }).exceptionally((Throwable e) -> {
            if (e instanceof CompletionException && e.getCause() instanceof StreamSegmentNotExistsException) {
                connection.send(new TransactionAborted(abortTx.getSegment(), abortTx.getTxid()));
            } else {
                handleException(transactionName, "Drop transaction", e);
            }
            return null;
        });
    }

    @Override
    public void sealSegment(SealSegment sealSegment) {
        String segment = sealSegment.getSegment();
        CompletableFuture<Long> future = segmentStore.sealStreamSegment(segment, TIMEOUT);
        future.thenAccept(size -> {
            connection.send(new SegmentSealed(segment));
        }).exceptionally(e -> {
            handleException(segment, "Seal segment", e);
            return null;
        });
    }

    @Override
    public void deleteSegment(DeleteSegment deleteSegment) {
        String segment = deleteSegment.getSegment();
        CompletableFuture<Void> future = segmentStore.deleteStreamSegment(segment, TIMEOUT);
        future.thenRun(() -> {
            connection.send(new SegmentDeleted(segment));
        }).exceptionally(e -> {
            handleException(segment, "Delete segment", e);
            return null;
        });
    }
}
