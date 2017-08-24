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
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.segment.StreamSegmentNameUtils;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.WrongHostException;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AbortTransaction;
import io.pravega.shared.protocol.netty.WireCommands.CommitTransaction;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransaction;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.GetTransactionInfo;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SealSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentDeleted;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentPolicyUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.shared.protocol.netty.WireCommands.SegmentSealed;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.TransactionAborted;
import io.pravega.shared.protocol.netty.WireCommands.TransactionCommitted;
import io.pravega.shared.protocol.netty.WireCommands.TransactionCreated;
import io.pravega.shared.protocol.netty.WireCommands.TransactionInfo;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentPolicy;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static io.pravega.segmentstore.contracts.Attributes.CREATION_TIME;
import static io.pravega.segmentstore.contracts.Attributes.SCALE_POLICY_RATE;
import static io.pravega.segmentstore.contracts.Attributes.SCALE_POLICY_TYPE;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Cache;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.EndOfStreamSegment;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Future;
import static io.pravega.shared.MetricsNames.SEGMENT_CREATE_LATENCY;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_LATENCY;
import static io.pravega.shared.MetricsNames.nameFromSegment;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static java.lang.Math.max;
import static java.lang.Math.min;

@Slf4j
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    static final int MAX_READ_SIZE = 2 * 1024 * 1024;

    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    @VisibleForTesting
    static final OpStatsLogger CREATE_STREAM_SEGMENT = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
    static final OpStatsLogger READ_STREAM_SEGMENT = STATS_LOGGER.createStats(SEGMENT_READ_LATENCY);

    private final StreamSegmentStore segmentStore;

    private final ServerConnection connection;

    private final SegmentStatsRecorder statsRecorder;

    public PravegaRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection) {
        this(segmentStore, connection, null);
    }

    public PravegaRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection, SegmentStatsRecorder statsRecorder) {
        this.segmentStore = segmentStore;
        this.connection = connection;
        this.statsRecorder = statsRecorder;
    }

    @Override
    public void readSegment(ReadSegment readSegment) {
        Timer timer = new Timer();
        final String segment = readSegment.getSegment();
        final int readSize = min(MAX_READ_SIZE, max(TYPE_PLUS_LENGTH_SIZE, readSegment.getSuggestedLength()));
        long trace = LoggerHelpers.traceEnter(log, "readSegment", readSegment);
        CompletableFuture<ReadResult> future = segmentStore.read(segment, readSegment.getOffset(), readSize, TIMEOUT);
        future.thenApply((ReadResult t) -> {
            LoggerHelpers.traceLeave(log, "readSegment", trace, t);
            handleReadResult(readSegment, t);
            DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_READ_BYTES, segment), t.getConsumedLength());
            READ_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
            return null;
        }).exceptionally((Throwable t) -> {
            handleException(readSegment.getOffset(), segment, "Read segment", t);
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

        if (!cachedEntries.isEmpty() || endOfSegment) {
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
                handleException(nonCachedEntry.getStreamSegmentOffset(), segment, "Read segment", e);
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
    public void updateSegmentAttribute(UpdateSegmentAttribute updateSegmentAttribute) {
        long requestId = updateSegmentAttribute.getRequestId();
        String segmentName = updateSegmentAttribute.getSegmentName();
        UUID attributeId = updateSegmentAttribute.getAttributeId();
        long newValue = updateSegmentAttribute.getNewValue();
        long expectedValue = updateSegmentAttribute.getExpectedValue();
        long trace = LoggerHelpers.traceEnter(log, "updateSegmentAttribute", updateSegmentAttribute);
        val update = new AttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, newValue, expectedValue);
        segmentStore.updateAttributes(segmentName, Collections.singletonList(update), TIMEOUT).whenComplete((v, e) -> {
            LoggerHelpers.traceLeave(log, "updateSegmentAttribute", trace, e);
            if (e == null) {
                connection.send(new SegmentAttributeUpdated(requestId, true));
            } else {
                if (ExceptionHelpers.getRealException(e) instanceof BadAttributeUpdateException) {
                    log.debug("Updating segment attribute {} failed due to: {}", update, e.getMessage());
                    connection.send(new SegmentAttributeUpdated(requestId, false));
                } else {
                    handleException(requestId, segmentName, "Update attribute", e);
                }
            }
        }).exceptionally((Throwable e) -> {
            handleException(requestId, segmentName, "Update attribute", e);
            return null;
        });
    }

    @Override
    public void getSegmentAttribute(GetSegmentAttribute getSegmentAttribute) {
        long requestId = getSegmentAttribute.getRequestId();
        String segmentName = getSegmentAttribute.getSegmentName();
        UUID attributeId = getSegmentAttribute.getAttributeId();
        long trace = LoggerHelpers.traceEnter(log, "getSegmentAttribute", getSegmentAttribute);
        segmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT).thenApply(properties -> {
            LoggerHelpers.traceLeave(log, "getSegmentAttribute", trace, properties);
            if (properties == null) {
                connection.send(new NoSuchSegment(requestId, segmentName));
            } else {
                Map<UUID, Long> attributes = properties.getAttributes();
                Long value = attributes.get(attributeId);
                if (value == null) {
                    value = WireCommands.NULL_ATTRIBUTE_VALUE;
                }
                connection.send(new SegmentAttribute(requestId, value));
            }
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(requestId, segmentName, "Get attribute", e);
            return null;
        });
    }
    
    @Override
    public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamSegmentInfo) {
        String segmentName = getStreamSegmentInfo.getSegmentName();
        CompletableFuture<SegmentProperties> future = segmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT);
        future.thenApply(properties -> {
            if (properties != null) {
                StreamSegmentInfo result = new StreamSegmentInfo(getStreamSegmentInfo.getRequestId(),
                                                                 properties.getName(),
                                                                 true,
                                                                 properties.isSealed(),
                                                                 properties.isDeleted(),
                                                                 properties.getLastModified().getTime(),
                                                                 properties.getLength());
                log.trace("Read stream segment info: {}", result);
                connection.send(result);
            } else {
                log.trace("getStreamSegmentInfo could not find segment {}", segmentName);
                connection.send(new StreamSegmentInfo(getStreamSegmentInfo.getRequestId(), segmentName, false, true, true, 0, 0));
            }
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(getStreamSegmentInfo.getRequestId(), segmentName, "Get segment info", e);
            return null;
        });
    }

    @Override
    public void getTransactionInfo(GetTransactionInfo request) {
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(request.getSegment(), request.getTxid());
        CompletableFuture<SegmentProperties> future = segmentStore.getStreamSegmentInfo(transactionName, false, TIMEOUT);
        future.thenApply(properties -> {
            if (properties != null) {
                TransactionInfo result = new TransactionInfo(request.getRequestId(), request.getSegment(),
                        request.getTxid(), transactionName, !properties.isDeleted(), properties.isSealed(),
                        properties.getLastModified().getTime(), properties.getLength());
                log.trace("Read transaction segment info: {}", result);
                connection.send(result);
            } else {
                log.trace("getTransactionInfo could not find segment {}", transactionName);
                connection.send(new TransactionInfo(request.getRequestId(), request.getSegment(), request.getTxid(),
                        transactionName, false, true, 0, 0));
            }
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(request.getRequestId(), transactionName, "Get transaction info", e);
            return null;
        });
    }

    @Override
    public void createSegment(CreateSegment createStreamsSegment) {
        Timer timer = new Timer();
        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(SCALE_POLICY_TYPE, AttributeUpdateType.Replace, ((Byte) createStreamsSegment.getScaleType()).longValue()),
                new AttributeUpdate(SCALE_POLICY_RATE, AttributeUpdateType.Replace, ((Integer) createStreamsSegment.getTargetRate()).longValue())
        );

        CompletableFuture<Void> future = segmentStore.createStreamSegment(createStreamsSegment.getSegment(), attributes, TIMEOUT);
        future.thenAccept((Void v) -> {
            CREATE_STREAM_SEGMENT.reportSuccessEvent(timer.getElapsed());
            connection.send(new SegmentCreated(createStreamsSegment.getRequestId(), createStreamsSegment.getSegment()));
        }).whenComplete((res, e) -> {
            if (e == null) {
                if (statsRecorder != null) {
                    statsRecorder.createSegment(createStreamsSegment.getSegment(),
                            createStreamsSegment.getScaleType(), createStreamsSegment.getTargetRate());
                }
            } else {
                CREATE_STREAM_SEGMENT.reportFailEvent(timer.getElapsed());
                handleException(createStreamsSegment.getRequestId(), createStreamsSegment.getSegment(), "Create segment", e);
            }
        });
    }

    private void handleException(long requestId, String segment, String operation, Throwable u) {
        if (u == null) {
            IllegalStateException exception = new IllegalStateException("No exception to handle.");
            log.error("Error (Segment = '{}', Operation = '{}')", segment, operation, exception);
            throw exception;
        }

        if (u instanceof CompletionException) {
            u = u.getCause();
        }

        log.error("Error (Segment = '{}', Operation = '{}')", segment, operation, u);
        if (u instanceof StreamSegmentExistsException) {
            connection.send(new SegmentAlreadyExists(requestId, segment));
        } else if (u instanceof StreamSegmentNotExistsException) {
            connection.send(new NoSuchSegment(requestId, segment));
        } else if (u instanceof StreamSegmentSealedException) {
            connection.send(new SegmentIsSealed(requestId, segment));
        } else if (u instanceof WrongHostException) {
            WrongHostException wrongHost = (WrongHostException) u;
            connection.send(new WrongHost(requestId, wrongHost.getStreamSegmentName(), wrongHost.getCorrectHost()));
        }  else if (u instanceof ContainerNotFoundException) {
            connection.send(new WrongHost(requestId, segment, ""));
        } else if (u instanceof CancellationException) {
            log.info("Closing connection due to: ", u.getMessage());
            connection.close();
        } else {
            // TODO: don't know what to do here...
            connection.close();
            throw new IllegalStateException("Unknown exception.", u);
        }
    }

    @Override
    public void createTransaction(CreateTransaction createTransaction) {
        Collection<AttributeUpdate> attributes = Collections.singleton(new AttributeUpdate(CREATION_TIME, AttributeUpdateType.None,
                System.currentTimeMillis()));
        log.debug("Creating transaction {} ", createTransaction);
        CompletableFuture<String> future = segmentStore.createTransaction(createTransaction.getSegment(), createTransaction.getTxid(),
                                                                          attributes, TIMEOUT);
        long requestId = createTransaction.getRequestId();
        future.thenApply((String txName) -> {
            connection.send(new TransactionCreated(requestId, createTransaction.getSegment(), createTransaction.getTxid()));
            return null;
        }).exceptionally((Throwable e) -> {
            handleException(requestId, createTransaction.getSegment(), "Create transaction", e);
            return null;
        });
    }

    @Override
    public void commitTransaction(CommitTransaction commitTx) {
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(commitTx.getSegment(), commitTx.getTxid());
        long requestId = commitTx.getRequestId();
        log.debug("Commiting transaction {} ", commitTx);
        segmentStore.sealStreamSegment(transactionName, TIMEOUT)
                    .exceptionally(this::ignoreSegmentSealed)
                    .thenCompose(v -> recordStatForTransaction(transactionName, commitTx.getSegment())
                                 .exceptionally((Throwable e) -> {
                                     // gobble up any errors from state recording so we do not affect rest of the flow.
                                     log.error("exception while computing stats while merging txn {}", e);
                                     return null;
                                 }))
                    .thenApply(result -> {
                        segmentStore.mergeTransaction(transactionName, TIMEOUT).thenAccept(v -> {
                            connection.send(new TransactionCommitted(requestId, commitTx.getSegment(),
                                    commitTx.getTxid()));
                        }).exceptionally((Throwable e) -> {
                            handleException(requestId, transactionName, "Commit transaction", e);
                            return null;
                        });
                        return null;
                    })
                    .exceptionally((Throwable e) -> {
                        handleException(requestId, transactionName, "Commit transaction", e);
                        return null;
                    });
    }

    @Override
    public void abortTransaction(AbortTransaction abortTx) {
        long requestId = abortTx.getRequestId();
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(abortTx.getSegment(), abortTx.getTxid());
        log.debug("Aborting transaction {} ", abortTx);
        CompletableFuture<Void> future = segmentStore.deleteStreamSegment(transactionName, TIMEOUT);
        future.thenRun(() -> {
            connection.send(new TransactionAborted(requestId, abortTx.getSegment(), abortTx.getTxid()));
        }).exceptionally((Throwable e) -> {
            if (e instanceof CompletionException && e.getCause() instanceof StreamSegmentNotExistsException) {
                connection.send(new TransactionAborted(requestId, abortTx.getSegment(), abortTx.getTxid()));
            } else {
                handleException(requestId, transactionName, "Drop transaction", e);
            }
            return null;
        });
    }

    @Override
    public void sealSegment(SealSegment sealSegment) {
        String segment = sealSegment.getSegment();
        log.debug("Sealing segment {} ", sealSegment);
        CompletableFuture<Long> future = segmentStore.sealStreamSegment(segment, TIMEOUT);
        future.thenAccept(size -> {
            connection.send(new SegmentSealed(sealSegment.getRequestId(), segment));
        }).whenComplete((r, e) -> {
            if (e != null) {
                handleException(sealSegment.getRequestId(), segment, "Seal segment", e);
            } else {
                if (statsRecorder != null) {
                    statsRecorder.sealSegment(sealSegment.getSegment());
                }
            }
        });
    }

    @Override
    public void deleteSegment(DeleteSegment deleteSegment) {
        String segment = deleteSegment.getSegment();
        log.debug("Deleting segment {} ", deleteSegment);
        CompletableFuture<Void> future = segmentStore.deleteStreamSegment(segment, TIMEOUT);
        future.thenRun(() -> {
            connection.send(new SegmentDeleted(deleteSegment.getRequestId(), segment));
        }).exceptionally(e -> {
            handleException(deleteSegment.getRequestId(), segment, "Delete segment", e);
            return null;
        });
    }

    @Override
    public void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy) {
        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(SCALE_POLICY_TYPE, AttributeUpdateType.Replace, (long) updateSegmentPolicy.getScaleType()),
                new AttributeUpdate(SCALE_POLICY_RATE, AttributeUpdateType.Replace, updateSegmentPolicy.getTargetRate()));
        log.debug("Updating segment policy {} ", updateSegmentPolicy);
        CompletableFuture<Void> future = segmentStore.updateAttributes(updateSegmentPolicy.getSegment(), attributes, TIMEOUT);
        future.thenAccept((Void v) -> {
            connection.send(new SegmentPolicyUpdated(updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getSegment()));
        }).whenComplete((r, e) -> {
            if (e != null) {
                handleException(updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getSegment(), "Update segment", e);
            } else {
                if (statsRecorder != null) {
                    statsRecorder.policyUpdate(updateSegmentPolicy.getSegment(), updateSegmentPolicy.getScaleType(),
                                               updateSegmentPolicy.getTargetRate());
                }
            }
        });
    }

    private CompletableFuture<Void> recordStatForTransaction(String transactionName, String parentSegmentName) {
        return segmentStore.getStreamSegmentInfo(transactionName, false, TIMEOUT)
                .thenAccept(prop -> {
                    if (prop != null &&
                            prop.getAttributes().containsKey(Attributes.CREATION_TIME) &&
                            prop.getAttributes().containsKey(Attributes.EVENT_COUNT)) {
                        long creationTime = prop.getAttributes().get(Attributes.CREATION_TIME);
                        int numOfEvents = prop.getAttributes().get(Attributes.EVENT_COUNT).intValue();
                        long len = prop.getLength();

                        if (statsRecorder != null) {
                            statsRecorder.merge(parentSegmentName, len, numOfEvents, creationTime);
                        }
                    }
                });
    }

    /**
     * Ignores StreamSegmentSealedException, re-throws anything else.
     */
    @SneakyThrows
    private Long ignoreSegmentSealed(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        if (!(ex instanceof StreamSegmentSealedException)) {
            throw ex;
        }

        return null;
    }
}
