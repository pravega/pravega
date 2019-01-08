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
import com.google.common.base.Throwables;
import io.pravega.auth.AuthenticationException;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.tracing.TagLogger;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.MergeSegments;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SealSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentDeleted;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsTruncated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentPolicyUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.shared.protocol.netty.WireCommands.SegmentSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentTruncated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.TruncateSegment;
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
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.slf4j.LoggerFactory;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.common.function.Callbacks.invokeSafely;
import static io.pravega.segmentstore.contracts.Attributes.CREATION_TIME;
import static io.pravega.segmentstore.contracts.Attributes.SCALE_POLICY_RATE;
import static io.pravega.segmentstore.contracts.Attributes.SCALE_POLICY_TYPE;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Cache;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.EndOfStreamSegment;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Future;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Truncated;
import static io.pravega.shared.MetricsNames.SEGMENT_CREATE_LATENCY;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_LATENCY;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_EVENTS;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsNames.nameFromSegment;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A Processor for all non-append operations on the Pravega SegmentStore Service.
 */
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

    //region Members

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaRequestProcessor.class));
    private static final int MAX_READ_SIZE = 2 * 1024 * 1024;
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
    private static final String EMPTY_STACK_TRACE = "";
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger createStreamSegment = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
    private final OpStatsLogger readStreamSegment = STATS_LOGGER.createStats(SEGMENT_READ_LATENCY);
    private final StreamSegmentStore segmentStore;
    private final ServerConnection connection;
    private final SegmentStatsRecorder statsRecorder;
    private final DelegationTokenVerifier tokenVerifier;
    private final boolean replyWithStackTraceOnError;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PravegaRequestProcessor class with no Metrics StatsRecorder.
     *
     * @param segmentStore The StreamSegmentStore to attach to (and issue requests to).
     * @param connection   The ServerConnection to attach to (and send responses to).
     */
    @VisibleForTesting
    public PravegaRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection) {
        this(segmentStore, connection, null, new PassingTokenVerifier(), false);
    }

    /**
     * Creates a new instance of the PravegaRequestProcessor class.
     *
     * @param segmentStore  The StreamSegmentStore to attach to (and issue requests to).
     * @param connection    The ServerConnection to attach to (and send responses to).
     * @param statsRecorder (Optional) A StatsRecorder for Metrics.
     * @param tokenVerifier  Verifier class that verifies delegation token.
     * @param replyWithStackTraceOnError Whether client replies upon failed requests contain server-side stack traces or not.
     */
    PravegaRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection, SegmentStatsRecorder statsRecorder,
                            DelegationTokenVerifier tokenVerifier, boolean replyWithStackTraceOnError) {
        this.segmentStore = Preconditions.checkNotNull(segmentStore, "segmentStore");
        this.connection = Preconditions.checkNotNull(connection, "connection");
        this.tokenVerifier = Preconditions.checkNotNull(tokenVerifier, "tokenVerifier");
        this.statsRecorder = statsRecorder;
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
    }

    //endregion

    //region RequestProcessor Implementation

    @Override
    public void readSegment(ReadSegment readSegment) {
        Timer timer = new Timer();
        final String segment = readSegment.getSegment();
        final String operation = "readSegment";

        if (!verifyToken(segment, readSegment.getOffset(), readSegment.getDelegationToken(), operation)) {
            return;
        }

        final int readSize = min(MAX_READ_SIZE, max(TYPE_PLUS_LENGTH_SIZE, readSegment.getSuggestedLength()));
        long trace = LoggerHelpers.traceEnter(log, operation, readSegment);
        segmentStore.read(segment, readSegment.getOffset(), readSize, TIMEOUT)
                .thenAccept(readResult -> {
                    LoggerHelpers.traceLeave(log, operation, trace, readResult);
                    handleReadResult(readSegment, readResult);
                    readStreamSegment.reportSuccessEvent(timer.getElapsed());
                })
                .exceptionally(ex -> handleException(readSegment.getOffset(), segment, operation, wrapCancellationException(ex)));
    }

    private boolean verifyToken(String segment, long requestId, String delegationToken, String operation) {
        if (!tokenVerifier.verifyToken(segment, delegationToken, READ)) {
            log.warn(requestId, "Delegation token verification failed.");
            handleException(requestId, segment, operation, new AuthenticationException("Token verification failed"));
            return false;
        }
        return true;
    }

    /**
     * Handles a readResult.
     * If there are cached entries that can be returned without blocking only these are returned.
     * Otherwise the call will request the data and setup a callback to return the data when it is available.
     * If no data is available but it was detected that the Segment had been truncated beyond the current offset,
     * an appropriate message is sent back over the connection.
     */
    private void handleReadResult(ReadSegment request, ReadResult result) {
        String segment = request.getSegment();
        ArrayList<ReadResultEntryContents> cachedEntries = new ArrayList<>();
        ReadResultEntry nonCachedEntry = collectCachedEntries(request.getOffset(), result, cachedEntries);
        final String operation = "readSegment";

        boolean truncated = nonCachedEntry != null && nonCachedEntry.getType() == Truncated;
        boolean endOfSegment = nonCachedEntry != null && nonCachedEntry.getType() == EndOfStreamSegment;
        boolean atTail = nonCachedEntry != null && nonCachedEntry.getType() == Future;

        if (!cachedEntries.isEmpty() || endOfSegment) {
            // We managed to collect some data. Send it.
            ByteBuffer data = copyData(cachedEntries);
            SegmentRead reply = new SegmentRead(segment, request.getOffset(), atTail, endOfSegment, data);
            connection.send(reply);
            DYNAMIC_LOGGER.incCounterValue(globalMetricName(SEGMENT_READ_BYTES), reply.getData().array().length);
            DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_READ_BYTES, segment), reply.getData().array().length);
        } else if (truncated) {
            // We didn't collect any data, instead we determined that the current read offset was truncated.
            // Determine the current Start Offset and send that back.
            segmentStore.getStreamSegmentInfo(segment, false, TIMEOUT)
                    .thenAccept(info ->
                            connection.send(new SegmentIsTruncated(nonCachedEntry.getStreamSegmentOffset(), segment, info.getStartOffset(), EMPTY_STACK_TRACE)))
                    .exceptionally(e -> handleException(nonCachedEntry.getStreamSegmentOffset(), segment, operation, wrapCancellationException(e)));
        } else {
            Preconditions.checkState(nonCachedEntry != null, "No ReadResultEntries returned from read!?");
            nonCachedEntry.requestContent(TIMEOUT);
            nonCachedEntry.getContent()
                    .thenAccept(contents -> {
                        ByteBuffer data = copyData(Collections.singletonList(contents));
                        SegmentRead reply = new SegmentRead(segment, nonCachedEntry.getStreamSegmentOffset(), false, endOfSegment, data);
                        connection.send(reply);
                        DYNAMIC_LOGGER.incCounterValue(globalMetricName(SEGMENT_READ_BYTES), reply.getData().array().length);
                        DYNAMIC_LOGGER.incCounterValue(nameFromSegment(SEGMENT_READ_BYTES, segment), reply.getData().array().length);
                    })
                    .exceptionally(e -> {
                        if (Exceptions.unwrap(e) instanceof StreamSegmentTruncatedException) {
                            // The Segment may have been truncated in Storage after we got this entry but before we managed
                            // to make a read. In that case, send the appropriate error back.
                            final String clientReplyStackTrace = replyWithStackTraceOnError ? e.getMessage() : EMPTY_STACK_TRACE;
                            connection.send(new SegmentIsTruncated(nonCachedEntry.getStreamSegmentOffset(), segment,
                                    nonCachedEntry.getStreamSegmentOffset(), clientReplyStackTrace));
                        } else {
                            handleException(nonCachedEntry.getStreamSegmentOffset(), segment, operation, wrapCancellationException(e));
                        }
                        return null;
                    })
                    .exceptionally(e -> handleException(nonCachedEntry.getStreamSegmentOffset(), segment, operation, wrapCancellationException(e)));
        }
    }

    /**
     * Wrap a {@link CancellationException} to {@link ReadCancellationException}
     */
    private Throwable wrapCancellationException(Throwable u) {
        Throwable wrappedException = null;
        if (u != null) {
            wrappedException = Exceptions.unwrap(u);
            if (wrappedException instanceof CancellationException) {
                wrappedException = new ReadCancellationException(wrappedException);
            }
        }
        return wrappedException;
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
    @SneakyThrows(IOException.class)
    private ByteBuffer copyData(List<ReadResultEntryContents> contents) {
        int totalSize = contents.stream().mapToInt(ReadResultEntryContents::getLength).sum();

        ByteBuffer data = ByteBuffer.allocate(totalSize);
        int bytesCopied = 0;
        for (ReadResultEntryContents content : contents) {
            int copied = StreamHelpers.readAll(content.getData(), data.array(), bytesCopied, totalSize - bytesCopied);
            Preconditions.checkState(copied == content.getLength(), "Read fewer bytes than available.");
            bytesCopied += copied;
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
        final String operation = "updateSegmentAttribute";

        if (!verifyToken(segmentName, updateSegmentAttribute.getRequestId(), updateSegmentAttribute.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, updateSegmentAttribute);
        val update = new AttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, newValue, expectedValue);
        segmentStore.updateAttributes(segmentName, Collections.singletonList(update), TIMEOUT)
                    .whenComplete((v, e) -> {
                        LoggerHelpers.traceLeave(log, operation, trace, e);
                        final Consumer<Throwable> failureHandler = t -> {
                            log.error(requestId, "Error (Segment = '{}', Operation = '{}')", segmentName, "handling result of " + operation, t);
                            connection.close();
                        };
                        if (e == null) {
                            invokeSafely(connection::send, new SegmentAttributeUpdated(requestId, true), failureHandler);
                        } else {
                            if (Exceptions.unwrap(e) instanceof BadAttributeUpdateException) {
                                log.debug("Updating segment attribute {} failed due to: {}", update, e.getMessage());
                                invokeSafely(connection::send, new SegmentAttributeUpdated(requestId, false), failureHandler);
                            } else {
                                handleException(requestId, segmentName, operation, e);
                            }
                        }
                    });
    }

    @Override
    public void getSegmentAttribute(GetSegmentAttribute getSegmentAttribute) {
        long requestId = getSegmentAttribute.getRequestId();
        String segmentName = getSegmentAttribute.getSegmentName();
        UUID attributeId = getSegmentAttribute.getAttributeId();
        final String operation = "getSegmentAttribute";

        if (!verifyToken(segmentName, getSegmentAttribute.getRequestId(), getSegmentAttribute.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, getSegmentAttribute);
        segmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT)
                .thenAccept(properties -> {
                    LoggerHelpers.traceLeave(log, operation, trace, properties);
                    if (properties == null) {
                        connection.send(new NoSuchSegment(requestId, segmentName, EMPTY_STACK_TRACE));
                    } else {
                        Map<UUID, Long> attributes = properties.getAttributes();
                        Long value = attributes.get(attributeId);
                        if (value == null) {
                            value = WireCommands.NULL_ATTRIBUTE_VALUE;
                        }
                        connection.send(new SegmentAttribute(requestId, value));
                    }
                })
                .exceptionally(e -> handleException(requestId, segmentName, operation, e));
    }

    @Override
    public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamSegmentInfo) {
        String segmentName = getStreamSegmentInfo.getSegmentName();
        final String operation = "getStreamSegmentInfo";

        if (!verifyToken(segmentName, getStreamSegmentInfo.getRequestId(), getStreamSegmentInfo.getDelegationToken(), operation)) {
            return;
        }

        segmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT)
                .thenAccept(properties -> {
                    if (properties != null) {
                        StreamSegmentInfo result = new StreamSegmentInfo(getStreamSegmentInfo.getRequestId(),
                                properties.getName(), true, properties.isSealed(), properties.isDeleted(),
                                properties.getLastModified().getTime(), properties.getLength(), properties.getStartOffset());
                        log.trace("Read stream segment info: {}", result);
                        connection.send(result);
                    } else {
                        log.trace("getStreamSegmentInfo could not find segment {}", segmentName);
                        connection.send(new StreamSegmentInfo(getStreamSegmentInfo.getRequestId(), segmentName, false, true, true, 0, 0, 0));
                    }
                })
                .exceptionally(e -> handleException(getStreamSegmentInfo.getRequestId(), segmentName, operation, e));
    }

    @Override
    public void createSegment(CreateSegment createStreamSegment) {
        Timer timer = new Timer();
        final String operation = "createSegment";

        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(SCALE_POLICY_TYPE, AttributeUpdateType.Replace, ((Byte) createStreamSegment.getScaleType()).longValue()),
                new AttributeUpdate(SCALE_POLICY_RATE, AttributeUpdateType.Replace, ((Integer) createStreamSegment.getTargetRate()).longValue()),
                new AttributeUpdate(CREATION_TIME, AttributeUpdateType.None, System.currentTimeMillis())
        );

       if (!verifyToken(createStreamSegment.getSegment(), createStreamSegment.getRequestId(), createStreamSegment.getDelegationToken(), operation)) {
            return;
       }

       log.info(createStreamSegment.getRequestId(), "Creating stream segment {}.", createStreamSegment);
       segmentStore.createStreamSegment(createStreamSegment.getSegment(), attributes, TIMEOUT)
                .thenAccept(v -> {
                    this.createStreamSegment.reportSuccessEvent(timer.getElapsed());
                    connection.send(new SegmentCreated(createStreamSegment.getRequestId(), createStreamSegment.getSegment()));
                })
                .whenComplete((res, e) -> {
                    if (e == null) {
                        if (statsRecorder != null) {
                            statsRecorder.createSegment(createStreamSegment.getSegment(),
                                    createStreamSegment.getScaleType(), createStreamSegment.getTargetRate());
                        }
                    } else {
                        this.createStreamSegment.reportFailEvent(timer.getElapsed());
                        handleException(createStreamSegment.getRequestId(), createStreamSegment.getSegment(), operation, e);
                    }
                });
    }

    @Override
    public void mergeSegments(MergeSegments mergeSegments) {
        final String operation = "mergeSegments";

        if (!verifyToken(mergeSegments.getSource(), mergeSegments.getRequestId(), mergeSegments.getDelegationToken(), operation)) {
            return;
        }

        log.info(mergeSegments.getRequestId(), "Merging Segments {} ", mergeSegments);
        segmentStore.mergeStreamSegment(mergeSegments.getTarget(), mergeSegments.getSource(), TIMEOUT)
                    .thenAccept(txnProp -> {
                        recordStatForTransaction(txnProp, mergeSegments.getTarget());
                        connection.send(new WireCommands.SegmentsMerged(mergeSegments.getRequestId(), mergeSegments.getTarget(), mergeSegments.getSource()));
                    })
                    .exceptionally(e -> {
                        if (Exceptions.unwrap(e) instanceof StreamSegmentMergedException) {
                            log.info(mergeSegments.getRequestId(), "Stream segment is already merged '{}'.",
                                    mergeSegments.getSource());
                            connection.send(new WireCommands.SegmentsMerged(mergeSegments.getRequestId(), mergeSegments.getTarget(), mergeSegments.getSource()));
                            return null;
                        } else {
                            return handleException(mergeSegments.getRequestId(), mergeSegments.getSource(), operation, e);
                        }
                    });
    }

    @Override
    public void sealSegment(SealSegment sealSegment) {
        String segment = sealSegment.getSegment();
        final String operation = "sealSegment";

        if (!verifyToken(segment, sealSegment.getRequestId(), sealSegment.getDelegationToken(), operation)) {
            return;
        }

        log.info(sealSegment.getRequestId(), "Sealing segment {} ", sealSegment);
        segmentStore.sealStreamSegment(segment, TIMEOUT)
                .thenAccept(size -> connection.send(new SegmentSealed(sealSegment.getRequestId(), segment)))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        handleException(sealSegment.getRequestId(), segment, operation, e);
                    } else {
                        DYNAMIC_LOGGER.freezeCounter(nameFromSegment(SEGMENT_WRITE_BYTES, segment));
                        DYNAMIC_LOGGER.freezeCounter(nameFromSegment(SEGMENT_WRITE_EVENTS, segment));
                        if (statsRecorder != null) {
                            statsRecorder.sealSegment(sealSegment.getSegment());
                        }
                    }
                });
    }

    @Override
    public void truncateSegment(TruncateSegment truncateSegment) {
        String segment = truncateSegment.getSegment();
        final String operation = "truncateSegment";

        if (!verifyToken(segment, truncateSegment.getRequestId(), truncateSegment.getDelegationToken(), operation)) {
            return;
        }

        long offset = truncateSegment.getTruncationOffset();
        log.info(truncateSegment.getRequestId(), "Truncating segment {} at offset {}.",
                segment, offset);
        segmentStore.truncateStreamSegment(segment, offset, TIMEOUT)
                .thenAccept(v -> connection.send(new SegmentTruncated(truncateSegment.getRequestId(), segment)))
                .exceptionally(e -> handleException(truncateSegment.getRequestId(), segment, operation, e));
    }

    @Override
    public void deleteSegment(DeleteSegment deleteSegment) {
        String segment = deleteSegment.getSegment();
        final String operation = "deleteSegment";

        if (!verifyToken(segment, deleteSegment.getRequestId(), deleteSegment.getDelegationToken(), operation)) {
            return;
        }

        log.info(deleteSegment.getRequestId(), "Deleting segment {} ", deleteSegment);
        segmentStore.deleteStreamSegment(segment, TIMEOUT)
                .thenRun(() -> {
                    connection.send(new SegmentDeleted(deleteSegment.getRequestId(), segment));
                    DYNAMIC_LOGGER.freezeCounter(nameFromSegment(SEGMENT_WRITE_BYTES, segment));
                    DYNAMIC_LOGGER.freezeCounter(nameFromSegment(SEGMENT_WRITE_EVENTS, segment));
                    DYNAMIC_LOGGER.freezeCounter(nameFromSegment(SEGMENT_READ_BYTES, segment));
                })
                .exceptionally(e -> handleException(deleteSegment.getRequestId(), segment, operation, e));
    }

    @Override
    public void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy) {
        final String operation = "updateSegmentPolicy";

        if (!verifyToken(updateSegmentPolicy.getSegment(), updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getDelegationToken(), operation)) {
            return;
        }

        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(SCALE_POLICY_TYPE, AttributeUpdateType.Replace, (long) updateSegmentPolicy.getScaleType()),
                new AttributeUpdate(SCALE_POLICY_RATE, AttributeUpdateType.Replace, updateSegmentPolicy.getTargetRate()));

        log.info(updateSegmentPolicy.getRequestId(), "Updating segment policy {} ", updateSegmentPolicy);
        segmentStore.updateAttributes(updateSegmentPolicy.getSegment(), attributes, TIMEOUT)
                .thenRun(() ->
                        connection.send(new SegmentPolicyUpdated(updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getSegment())))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        handleException(updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getSegment(), operation, e);
                    } else {
                        if (statsRecorder != null) {
                            statsRecorder.policyUpdate(updateSegmentPolicy.getSegment(),
                                    updateSegmentPolicy.getScaleType(), updateSegmentPolicy.getTargetRate());
                        }
                    }
                });
    }

    //endregion

    private Void handleException(long requestId, String segment, String operation, Throwable u) {
        if (u == null) {
            IllegalStateException exception = new IllegalStateException("No exception to handle.");
            log.error(requestId, "Error (Segment = '{}', Operation = '{}')", segment, operation, exception);
            throw exception;
        }

        u = Exceptions.unwrap(u);
        String clientReplyStackTrace = replyWithStackTraceOnError ? Throwables.getStackTraceAsString(u) : EMPTY_STACK_TRACE;
        final Consumer<Throwable> failureHandler = t -> {
            log.error(requestId, "Error (Segment = '{}', Operation = '{}')", segment, "handling result of " + operation, t);
            connection.close();
        };

        if (u instanceof StreamSegmentExistsException) {
            log.info(requestId, "Segment '{}' already exists and cannot perform operation '{}'.",
                     segment, operation);
            invokeSafely(connection::send, new SegmentAlreadyExists(requestId, segment, clientReplyStackTrace), failureHandler);

        } else if (u instanceof StreamSegmentNotExistsException) {
            log.warn(requestId, "Segment '{}' does not exist and cannot perform operation '{}'.",
                     segment, operation);
            invokeSafely(connection::send, new NoSuchSegment(requestId, segment, clientReplyStackTrace), failureHandler);
        } else if (u instanceof StreamSegmentSealedException) {
            log.info(requestId, "Segment '{}' is sealed and cannot perform operation '{}'.",
                     segment, operation);
            invokeSafely(connection::send, new SegmentIsSealed(requestId, segment, clientReplyStackTrace), failureHandler);
        } else if (u instanceof ContainerNotFoundException) {
            int containerId = ((ContainerNotFoundException) u).getContainerId();
            log.warn(requestId, "Wrong host. Segment = '{}' (Container {}) is not owned. Operation = '{}').",
                     segment, containerId, operation);
            invokeSafely(connection::send, new WrongHost(requestId, segment, "", clientReplyStackTrace), failureHandler);
        } else if (u instanceof ReadCancellationException) {
            log.info(requestId, "Closing connection {} while reading segment {} due to CancellationException.",
                     connection, segment);
            invokeSafely(connection::send, new SegmentRead(segment, requestId, true, false, EMPTY_BYTE_BUFFER), failureHandler);
        } else if (u instanceof CancellationException) {
            log.info(requestId, "Closing connection {} while performing {} due to {}.",
                     connection, operation, u.getMessage());
            connection.close();
        } else if (u instanceof AuthenticationException) {
            log.warn(requestId, "Authentication error during '{}'.", operation);
            invokeSafely(connection::send, new AuthTokenCheckFailed(requestId, clientReplyStackTrace), failureHandler);
            connection.close();
        } else if (u instanceof UnsupportedOperationException) {
            log.warn(requestId, "Unsupported Operation '{}'.", operation, u);
            invokeSafely(connection::send, new OperationUnsupported(requestId, operation, clientReplyStackTrace), failureHandler);
        } else if (u instanceof BadOffsetException) {
            BadOffsetException badOffset = (BadOffsetException) u;
            invokeSafely(connection::send, new SegmentIsTruncated(requestId, segment, badOffset.getExpectedOffset(), clientReplyStackTrace), failureHandler);
        } else {
            log.error(requestId, "Error (Segment = '{}', Operation = '{}')", segment, operation, u);
            connection.close(); // Closing connection should reinitialize things, and hopefully fix the problem
            throw new IllegalStateException("Unknown exception.", u);
        }

        return null;
    }

    private void recordStatForTransaction(SegmentProperties sourceInfo, String targetSegmentName) {
        try {
            if (sourceInfo != null &&
                    sourceInfo.getAttributes().containsKey(Attributes.CREATION_TIME) &&
                            sourceInfo.getAttributes().containsKey(Attributes.EVENT_COUNT)) {
                long creationTime = sourceInfo.getAttributes().get(Attributes.CREATION_TIME);
                int numOfEvents = sourceInfo.getAttributes().get(Attributes.EVENT_COUNT).intValue();
                long len = sourceInfo.getLength();

                if (statsRecorder != null) {
                    statsRecorder.merge(targetSegmentName, len, numOfEvents, creationTime);
                }
            }
        } catch (Exception ex) {
            // gobble up any errors from stat recording so we do not affect rest of the flow.
            log.error("exception while computing stats while merging txn {}", sourceInfo.getName(), ex);
        }
    }

    /**
     * Custom exception to indicate a {@link CancellationException} during a Read segment operation.
     */
    private class ReadCancellationException extends RuntimeException {
        ReadCancellationException(Throwable wrappedException) {
            super("CancellationException during operation Read segment", wrappedException);
        }
    }
}
