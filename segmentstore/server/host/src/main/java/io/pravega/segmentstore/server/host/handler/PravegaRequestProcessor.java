/**
 * Copyright Pravega Authors.
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
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableSegmentNotEmptyException;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.segmentstore.server.tables.DeltaIteratorState;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.CreateTableSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteTableSegment;
import io.pravega.shared.protocol.netty.WireCommands.ErrorMessage.ErrorCode;
import io.pravega.shared.protocol.netty.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.LocateOffset;
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
import io.pravega.shared.protocol.netty.WireCommands.TableSegmentNotEmpty;
import io.pravega.shared.protocol.netty.WireCommands.TruncateSegment;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentPolicy;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;
import org.slf4j.LoggerFactory;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static io.pravega.common.function.Callbacks.invokeSafely;
import static io.pravega.segmentstore.contracts.Attributes.ATTRIBUTE_SEGMENT_TYPE;
import static io.pravega.segmentstore.contracts.Attributes.CREATION_TIME;
import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.segmentstore.contracts.Attributes.EXPECTED_INDEX_SEGMENT_EVENT_SIZE;
import static io.pravega.segmentstore.contracts.Attributes.ROLLOVER_SIZE;
import static io.pravega.segmentstore.contracts.Attributes.SCALE_POLICY_RATE;
import static io.pravega.segmentstore.contracts.Attributes.SCALE_POLICY_TYPE;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Cache;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.EndOfStreamSegment;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Future;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Truncated;
import static io.pravega.shared.NameUtils.INDEX_APPEND_EVENT_SIZE;
import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static io.pravega.shared.NameUtils.isTransientSegment;
import static io.pravega.shared.NameUtils.isUserStreamSegment;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;

/**
 * A Processor for all non-append operations on the Pravega SegmentStore Service.
 */
public class PravegaRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

    //region Members

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaRequestProcessor.class));
    private static final int MAX_READ_SIZE = 2 * 1024 * 1024;
    private static final String EMPTY_STACK_TRACE = "";
    @Getter(AccessLevel.PROTECTED)
    private final StreamSegmentStore segmentStore;
    private final TableStore tableStore;
    private final SegmentStatsRecorder statsRecorder;
    private final TableSegmentStatsRecorder tableStatsRecorder;
    private final DelegationTokenVerifier tokenVerifier;
    private final boolean replyWithStackTraceOnError;
    @Getter(AccessLevel.PROTECTED)
    private final TrackedConnection connection;
    private final IndexAppendProcessor indexAppendProcessor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PravegaRequestProcessor class with no Metrics StatsRecorder.
     *
     * @param segmentStore The StreamSegmentStore to attach to (and issue requests to).
     * @param tableStore The TableStore to attach to (and issue requests to).
     * @param connection   The ServerConnection to attach to (and send responses to).
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    @VisibleForTesting
    public PravegaRequestProcessor(StreamSegmentStore segmentStore, TableStore tableStore, ServerConnection connection,
                                   IndexAppendProcessor indexAppendProcessor) {
        this(segmentStore, tableStore, new TrackedConnection(connection, new ConnectionTracker()), SegmentStatsRecorder.noOp(),
                TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), false, indexAppendProcessor);
    }

    /**
     * Creates a new instance of the PravegaRequestProcessor class.
     *
     * @param segmentStore  The StreamSegmentStore to attach to (and issue requests to).
     * @param tableStore    The TableStore to attach to (and issue requests to).
     * @param connection    The ServerConnection to attach to (and send responses to).
     * @param statsRecorder A StatsRecorder for Metrics for Stream Segments.
     * @param tableStatsRecorder A TableSegmentStatsRecorder for Metrics for Table Segments.
     * @param tokenVerifier  Verifier class that verifies delegation token.
     * @param replyWithStackTraceOnError Whether client replies upon failed requests contain server-side stack traces or not.
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    PravegaRequestProcessor(@NonNull StreamSegmentStore segmentStore, @NonNull TableStore tableStore, @NonNull TrackedConnection connection,
                            @NonNull SegmentStatsRecorder statsRecorder, @NonNull TableSegmentStatsRecorder tableStatsRecorder,
                            @NonNull DelegationTokenVerifier tokenVerifier, boolean replyWithStackTraceOnError,
                            @NonNull IndexAppendProcessor indexAppendProcessor) {
        this.segmentStore = segmentStore;
        this.tableStore = tableStore;
        this.connection = connection;
        this.tokenVerifier = tokenVerifier;
        this.statsRecorder = statsRecorder;
        this.tableStatsRecorder = tableStatsRecorder;
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
        this.indexAppendProcessor = indexAppendProcessor;
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
                        this.statsRecorder.readComplete(timer.getElapsed());
                    })
                    .exceptionally(ex -> handleException(readSegment.getRequestId(), segment, readSegment.getOffset(), operation,
                                                         wrapCancellationException(ex)));
    }

    protected boolean verifyToken(String segment, long requestId, String delegationToken, String operation) {
        boolean isTokenValid = false;
        try {
            tokenVerifier.verifyToken(segment, delegationToken, READ);
            isTokenValid = true;
        } catch (TokenException e) {
            handleException(requestId, segment, operation, e);
        }
        return isTokenValid;
    }

    protected boolean verifyTokenForUpdate(String segment, long requestId, String delegationToken, String operation) {
        boolean isTokenValid = false;
        try {
            tokenVerifier.verifyToken(segment, delegationToken, READ_UPDATE);
            isTokenValid = true;
        } catch (TokenException e) {
            handleException(requestId, segment, operation, e);
        }
        return isTokenValid;
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
        ArrayList<BufferView> cachedEntries = new ArrayList<>();
        ReadResultEntry nonCachedEntry = collectCachedEntries(request.getOffset(), result, cachedEntries);
        final String operation = "readSegment";

        boolean truncated = nonCachedEntry != null && nonCachedEntry.getType() == Truncated;
        boolean endOfSegment = nonCachedEntry != null && nonCachedEntry.getType() == EndOfStreamSegment;
        boolean atTail = nonCachedEntry != null && nonCachedEntry.getType() == Future;

        if (!cachedEntries.isEmpty() || endOfSegment) {
            // We managed to collect some data. Send it.
            ByteBuf data = toByteBuf(cachedEntries);
            SegmentRead reply = new SegmentRead(segment, request.getOffset(), atTail, endOfSegment, data, request.getRequestId());
            connection.send(reply);
            this.statsRecorder.read(segment, reply.getData().readableBytes());
        } else if (truncated) {
            // We didn't collect any data, instead we determined that the current read offset was truncated.
            // Determine the current Start Offset and send that back.
            segmentStore.getStreamSegmentInfo(segment, TIMEOUT)
                    .thenAccept(info ->
                            connection.send(new SegmentIsTruncated(request.getRequestId(), segment,
                                                                   info.getStartOffset(), EMPTY_STACK_TRACE, nonCachedEntry.getStreamSegmentOffset())))
                    .exceptionally(e -> handleException(request.getRequestId(), segment, nonCachedEntry.getStreamSegmentOffset(), operation,
                                                        wrapCancellationException(e)));
        } else {
            Preconditions.checkState(nonCachedEntry != null, "No ReadResultEntries returned from read!?");
            nonCachedEntry.requestContent(TIMEOUT);
            nonCachedEntry.getContent()
                    .thenAccept(contents -> {
                        ByteBuf data = toByteBuf(Collections.singletonList(contents));
                        SegmentRead reply = new SegmentRead(segment, nonCachedEntry.getStreamSegmentOffset(),
                                atTail, endOfSegment,
                                data, request.getRequestId());
                        connection.send(reply);
                        this.statsRecorder.read(segment, reply.getData().readableBytes());
                    })
                    .exceptionally(exception -> {
                        Throwable e = Exceptions.unwrap(exception);
                        if (e instanceof StreamSegmentTruncatedException) {
                            // The Segment may have been truncated in Storage after we got this entry but before we managed
                            // to make a read. In that case, send the appropriate error back.
                            final String clientReplyStackTrace = replyWithStackTraceOnError ? e.getMessage() : EMPTY_STACK_TRACE;
                            connection.send(new SegmentIsTruncated(request.getRequestId(),
                                                                   segment,
                                                                   ((StreamSegmentTruncatedException) e).getStartOffset(),
                                                                   clientReplyStackTrace,
                                                                   nonCachedEntry.getStreamSegmentOffset()));
                        } else {
                            handleException(request.getRequestId(), segment, nonCachedEntry.getStreamSegmentOffset(), operation,
                                            wrapCancellationException(e));
                        }
                        return null;
                    })
                    .exceptionally(e -> handleException(request.getRequestId(), segment, nonCachedEntry.getStreamSegmentOffset(), operation,
                                                        wrapCancellationException(e)));
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
    private ReadResultEntry collectCachedEntries(long initialOffset, ReadResult readResult, ArrayList<BufferView> cachedEntries) {
        long expectedOffset = initialOffset;
        while (readResult.hasNext()) {
            ReadResultEntry entry = readResult.next();
            if (entry.getType() == Cache) {
                Preconditions.checkState(entry.getStreamSegmentOffset() == expectedOffset,
                        "Data returned from read was not contiguous.");
                BufferView content = entry.getContent().getNow(null);
                expectedOffset += content.getLength();
                cachedEntries.add(content);
            } else {
                return entry;
            }
        }
        return null;
    }

    /**
     * Collect all the data from the given contents into a {@link ByteBuf}.
     */
    private ByteBuf toByteBuf(List<BufferView> contents) {
        val iterators = Iterators.concat(Iterators.transform(contents.iterator(), BufferView::iterateBuffers));
        val b = Iterators.transform(iterators, Unpooled::wrappedBuffer);
        return Unpooled.wrappedUnmodifiableBuffer(Iterators.toArray(b, ByteBuf.class));
    }

    private ByteBuf toByteBuf(BufferView bufferView) {
        val iterators = Iterators.transform(bufferView.iterateBuffers(), Unpooled::wrappedBuffer);
        return Unpooled.wrappedUnmodifiableBuffer(Iterators.toArray(iterators, ByteBuf.class));
    }

    @Override
    public void updateSegmentAttribute(UpdateSegmentAttribute updateSegmentAttribute) {
        long requestId = updateSegmentAttribute.getRequestId();
        String segmentName = updateSegmentAttribute.getSegmentName();
        AttributeId attributeId = updateSegmentAttribute.getAttributeId() == null ? null : AttributeId.fromUUID(updateSegmentAttribute.getAttributeId());
        long newValue = updateSegmentAttribute.getNewValue();
        long expectedValue = updateSegmentAttribute.getExpectedValue();
        final String operation = "updateSegmentAttribute";

        if (!verifyToken(segmentName, updateSegmentAttribute.getRequestId(), updateSegmentAttribute.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, updateSegmentAttribute);
        val update = new AttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, newValue, expectedValue);
        segmentStore.updateAttributes(segmentName, AttributeUpdateCollection.from(update), TIMEOUT)
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
        AttributeId attributeId = getSegmentAttribute.getAttributeId() == null ? null : AttributeId.fromUUID(getSegmentAttribute.getAttributeId());
        final String operation = "getSegmentAttribute";

        if (!verifyToken(segmentName, getSegmentAttribute.getRequestId(), getSegmentAttribute.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, getSegmentAttribute);
        segmentStore.getStreamSegmentInfo(segmentName, TIMEOUT)
                .thenAccept(properties -> {
                    LoggerHelpers.traceLeave(log, operation, trace, properties);
                    if (properties == null) {
                        connection.send(new NoSuchSegment(requestId, segmentName, EMPTY_STACK_TRACE, -1L));
                    } else {
                        Long value = properties.getAttributes().get(attributeId);
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

        segmentStore.getStreamSegmentInfo(segmentName, TIMEOUT)
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

        if (createStreamSegment.getRolloverSizeBytes() < 0) {
            log.warn("Segment rollover size bytes cannot be less than 0, actual is {}, fall back to default value", createStreamSegment.getRolloverSizeBytes());
        }
        final long rolloverSizeBytes = createStreamSegment.getRolloverSizeBytes() < 0 ? 0 : createStreamSegment.getRolloverSizeBytes();

        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(SCALE_POLICY_TYPE, AttributeUpdateType.Replace, ((Byte) createStreamSegment.getScaleType()).longValue()),
                new AttributeUpdate(SCALE_POLICY_RATE, AttributeUpdateType.Replace, ((Integer) createStreamSegment.getTargetRate()).longValue()),
                new AttributeUpdate(ROLLOVER_SIZE, AttributeUpdateType.Replace, rolloverSizeBytes),
                new AttributeUpdate(CREATION_TIME, AttributeUpdateType.None, System.currentTimeMillis())
        );

        if (!verifyToken(createStreamSegment.getSegment(), createStreamSegment.getRequestId(), createStreamSegment.getDelegationToken(), operation)) {
            return;
        }

        log.info(createStreamSegment.getRequestId(), "Creating stream segment {}.", createStreamSegment);
        segmentStore.createStreamSegment(createStreamSegment.getSegment(), SegmentType.STREAM_SEGMENT, attributes, TIMEOUT)
                    .thenCompose(result -> createIndexSegment(createStreamSegment.getSegment())) // this creates the index segment
                    .thenAccept(v -> connection.send(new SegmentCreated(createStreamSegment.getRequestId(), createStreamSegment.getSegment())))
                    .whenComplete((res, e) -> {
                    if (e == null) {
                        log.debug(createStreamSegment.getRequestId(), "Stream segment {} created successfully.",
                                createStreamSegment);
                        statsRecorder.createSegment(createStreamSegment.getSegment(),
                                createStreamSegment.getScaleType(), createStreamSegment.getTargetRate(), timer.getElapsed());
                    } else {
                        handleException(createStreamSegment.getRequestId(), createStreamSegment.getSegment(), operation, e);
                    }
                });
    }

    private CompletableFuture<Void> createIndexSegment(final String segmentName) {
        if (!isUserStreamSegment(segmentName)) {
            log.info("No need to create index segment for segment {}.", segmentName);
            return CompletableFuture.completedFuture(null);
        }
        log.info("Creating index segment {}.", getIndexSegmentName(segmentName));
        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(CREATION_TIME, AttributeUpdateType.None, System.currentTimeMillis()),
                new AttributeUpdate(ATTRIBUTE_SEGMENT_TYPE, AttributeUpdateType.None, SegmentType.STREAM_SEGMENT.getValue()),
                new AttributeUpdate(EXPECTED_INDEX_SEGMENT_EVENT_SIZE, AttributeUpdateType.None, NameUtils.INDEX_APPEND_EVENT_SIZE)
        );
        return segmentStore.createStreamSegment(getIndexSegmentName(segmentName), SegmentType.STREAM_SEGMENT,
                attributes, TIMEOUT);
    }

    @Override
    public void mergeSegments(MergeSegments mergeSegments) {
        final String operation = "mergeSegments";

        if (!verifyToken(mergeSegments.getSource(), mergeSegments.getRequestId(), mergeSegments.getDelegationToken(), operation)) {
            return;
        }

        log.info(mergeSegments.getRequestId(), "Merging Segments {} ", mergeSegments);

        // Populate the AttributeUpdates for this mergeSegments operation, if any.
        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        if (mergeSegments.getAttributeUpdates() != null) {
            for (WireCommands.ConditionalAttributeUpdate update : mergeSegments.getAttributeUpdates()) {
                attributeUpdates.add(new AttributeUpdate(AttributeId.fromUUID(update.getAttributeId()),
                    AttributeUpdateType.get(update.getAttributeUpdateType()), update.getNewValue(), update.getOldValue()));
            }
        }
        getSegmentEventCount(mergeSegments.getSource())
                .thenCompose(eventCount -> {
                    attributeUpdates.add(new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, eventCount));
                    return segmentStore.mergeStreamSegment(mergeSegments.getTarget(), mergeSegments.getSource(), attributeUpdates, TIMEOUT);
                })
                .thenAccept(mergeResult -> {
                        recordStatForTransaction(mergeResult, mergeSegments.getTarget());
                        connection.send(new WireCommands.SegmentsMerged(mergeSegments.getRequestId(),
                                                                        mergeSegments.getTarget(),
                                                                        mergeSegments.getSource(),
                                                                        mergeResult.getTargetSegmentLength()));
                }).thenAccept(v -> appendOnIndexSegment(mergeSegments.getTarget()))
                .exceptionally(e -> {
                        if (Exceptions.unwrap(e) instanceof StreamSegmentMergedException) {
                            log.info(mergeSegments.getRequestId(), "Stream segment is already merged '{}'.",
                                    mergeSegments.getSource());
                            segmentStore.getStreamSegmentInfo(mergeSegments.getTarget(), TIMEOUT)
                                        .thenAccept(properties -> {
                                            connection.send(new WireCommands.SegmentsMerged(mergeSegments.getRequestId(),
                                                                                            mergeSegments.getTarget(),
                                                                                            mergeSegments.getSource(),
                                                                                            properties.getLength()));
                                        });
                            return null;
                        } else if (Exceptions.unwrap(e) instanceof BadAttributeUpdateException) {
                            log.debug(mergeSegments.getRequestId(), "Conditional merge failed (Source segment={}, " +
                                    "Target segment={}): {}", mergeSegments.getSource(), mergeSegments.getTarget(), e.toString());
                            connection.send(new SegmentAttributeUpdated(mergeSegments.getRequestId(), false));
                            return null;
                        } else {
                            return handleException(mergeSegments.getRequestId(), mergeSegments.getSource(), operation, e);
                        }
                });
    }

    @Override
    public void mergeSegmentsBatch(WireCommands.MergeSegmentsBatch mergeSegments) {
        final String operation = "mergeSegmentsBatch";
        List<String> sources = mergeSegments.getSourceSegmentIds();
        if (!verifyTokenForUpdate(mergeSegments.getTargetSegmentId(), mergeSegments.getRequestId(), mergeSegments.getDelegationToken(), operation)) {
            return;
        }
        for (String s : sources) {
            if (!verifyToken(s, mergeSegments.getRequestId(), mergeSegments.getDelegationToken(), operation)) {
                return;
            }
        }
        log.info(mergeSegments.getRequestId(), "Merging Segments Batch in-order {} ", mergeSegments);
        Futures.allOfWithResults(sources.stream().map(source -> Futures.handleCompose(getSegmentEventCount(source)
            .thenCompose(eventCount -> {
                AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
                attributeUpdates.add(new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, eventCount));
                return segmentStore.mergeStreamSegment(mergeSegments.getTargetSegmentId(), source, attributeUpdates, TIMEOUT);
            }), (r, e) -> {
                if (e != null) {
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof StreamSegmentMergedException) {
                        log.info(mergeSegments.getRequestId(), "Stream segment already merged '{}'.", source);
                        return segmentStore.getStreamSegmentInfo(mergeSegments.getTargetSegmentId(), TIMEOUT).thenApply(SegmentProperties::getLength);
                    }
                    if (unwrap instanceof StreamSegmentNotExistsException) {
                        StreamSegmentNotExistsException ex = (StreamSegmentNotExistsException) unwrap;
                        if (ex.getStreamSegmentName().equals(source)) {
                            log.info(mergeSegments.getRequestId(), "Stream segment already merged '{}'.", source);
                            return segmentStore.getStreamSegmentInfo(mergeSegments.getTargetSegmentId(), TIMEOUT).thenApply(SegmentProperties::getLength);
                        }
                    }
                    throw new CompletionException(e);
                } else {
                    recordStatForTransaction(r, mergeSegments.getTargetSegmentId());
                    return appendOnIndexSegment(mergeSegments.getTargetSegmentId()).thenApply(v -> r.getTargetSegmentLength());
                }
            })).collect(Collectors.toUnmodifiableList())).thenAccept(mergeResults -> {
                connection.send(new WireCommands.SegmentsBatchMerged(mergeSegments.getRequestId(),
                    mergeSegments.getTargetSegmentId(),
                    sources,
                    mergeResults));
            })
        .exceptionally(e -> {
            log.debug("error");
            return handleException(mergeSegments.getRequestId(), mergeSegments.getTargetSegmentId(), operation, e);
        });
    }

    private CompletableFuture<Void> appendOnIndexSegment(String segmentName) {
        return segmentStore.getAttributes(getIndexSegmentName(segmentName), Collections.singleton(EXPECTED_INDEX_SEGMENT_EVENT_SIZE), true, TIMEOUT)
                .exceptionally(e -> {
                    log.warn("Exception occurred while getting max event size for index segment {}, exception: {}", getIndexSegmentName(segmentName), e);
                    if (Exceptions.unwrap(e) instanceof StreamSegmentNotExistsException) {
                        return Map.of(EXPECTED_INDEX_SEGMENT_EVENT_SIZE, -1L);
                    }
                    return Map.of(EXPECTED_INDEX_SEGMENT_EVENT_SIZE, 0L);
                })
                .thenCompose(attributes -> {
                    long size = attributes.getOrDefault(EXPECTED_INDEX_SEGMENT_EVENT_SIZE, 0L);
                    if (size != -1L) {
                        return CompletableFuture.completedFuture(size);
                    } 
                    log.info("Creating index segment {} as it doesn't exist.", getIndexSegmentName(segmentName));
                    return createIndexSegment(segmentName).thenApply(v -> {
                        log.info("Index segment {} created successfully.", getIndexSegmentName(segmentName));
                        return Long.valueOf(INDEX_APPEND_EVENT_SIZE);
                    }).exceptionally(ex -> {
                        log.warn("Exception occurred while creating the index segment {}.", getIndexSegmentName(segmentName));
                        return 0L;
                    });
                }).thenAccept(eventSize -> indexAppendProcessor.processAppend(segmentName, eventSize));
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
                        statsRecorder.sealSegment(sealSegment.getSegment());
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
                .thenCompose(v -> truncateIndexSegment(segment, offset))
                .thenAccept(v -> connection.send(new SegmentTruncated(truncateSegment.getRequestId(), segment)))
                .exceptionally(e -> handleException(truncateSegment.getRequestId(), segment, offset, operation, e));
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
                .thenAccept(v -> segmentStore.deleteStreamSegment(getIndexSegmentName(segment), TIMEOUT))
                .thenRun(() -> {
                    connection.send(new SegmentDeleted(deleteSegment.getRequestId(), segment));
                    this.statsRecorder.deleteSegment(segment);
                })
                .exceptionally(e -> handleException(deleteSegment.getRequestId(), segment, operation, e));
    }

    @Override
    public void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy) {
        final String operation = "updateSegmentPolicy";

        if (!verifyToken(updateSegmentPolicy.getSegment(), updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getDelegationToken(), operation)) {
            return;
        }

        AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
                new AttributeUpdate(SCALE_POLICY_TYPE, AttributeUpdateType.Replace, updateSegmentPolicy.getScaleType()),
                new AttributeUpdate(SCALE_POLICY_RATE, AttributeUpdateType.Replace, updateSegmentPolicy.getTargetRate()));

        log.info(updateSegmentPolicy.getRequestId(), "Updating segment policy {} ", updateSegmentPolicy);
        segmentStore.updateAttributes(updateSegmentPolicy.getSegment(), attributes, TIMEOUT)
                .thenRun(() ->
                        connection.send(new SegmentPolicyUpdated(updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getSegment())))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        handleException(updateSegmentPolicy.getRequestId(), updateSegmentPolicy.getSegment(), operation, e);
                    } else {
                        statsRecorder.policyUpdate(updateSegmentPolicy.getSegment(),
                                updateSegmentPolicy.getScaleType(), updateSegmentPolicy.getTargetRate());
                    }
                });
    }

    @Override
    public void locateOffset(LocateOffset locateOffset) {
        final String operation = "locateOffset";
        long requestId = locateOffset.getRequestId();
        String segment = locateOffset.getSegment();
        if (!verifyToken(segment, requestId, locateOffset.getDelegationToken(), operation)) {
            return;
        }
        
        log.debug(requestId, "Locating offset {} ", locateOffset);
        IndexRequestProcessor.findNearestIndexedOffset(segmentStore, segment, locateOffset.getTargetOffset(), true).thenAccept(offset -> {
            connection.send(new WireCommands.OffsetLocated(requestId, segment, offset));
        }).exceptionally(e -> {
            if (Exceptions.unwrap(e) instanceof StreamSegmentNotExistsException) {
                log.info("Index segment does not exist for segment : {}, hence returning the segment length as offset", segment);
                segmentStore.getStreamSegmentInfo(segment, TIMEOUT).thenAccept(info -> {
                    connection.send(new WireCommands.OffsetLocated(requestId, segment, info.getLength()));
                }).exceptionally(ex -> handleException(requestId, segment, operation, ex));
            } else {
                handleException(requestId, segment, operation, e);
            }
            return null;
        });
    }

    @Override
    public void getTableSegmentInfo(WireCommands.GetTableSegmentInfo getInfo) {
        final String operation = "getTableSegmentInfo";

        if (!verifyToken(getInfo.getSegmentName(), getInfo.getRequestId(), getInfo.getDelegationToken(), operation)) {
            return;
        }

        val timer = new Timer();
        log.debug(getInfo.getRequestId(), "Get Table Segment Info {}.", getInfo.getSegmentName());
        tableStore.getInfo(getInfo.getSegmentName(), TIMEOUT)
                .thenAccept(info -> {
                    connection.send(new WireCommands.TableSegmentInfo(getInfo.getRequestId(), getInfo.getSegmentName(),
                            info.getStartOffset(), info.getLength(), info.getEntryCount(), info.getKeyLength()));
                    this.tableStatsRecorder.getInfo(getInfo.getSegmentName(), timer.getElapsed());
                })
                .exceptionally(e -> handleException(getInfo.getRequestId(), getInfo.getSegmentName(), operation, e));
    }

    @Override
    public void createTableSegment(final CreateTableSegment createTableSegment) {
        final String operation = "createTableSegment";

        if (!verifyToken(createTableSegment.getSegment(), createTableSegment.getRequestId(), createTableSegment.getDelegationToken(), operation)) {
            return;
        }

        log.info(createTableSegment.getRequestId(), "Creating table segment {}.", createTableSegment);
        val timer = new Timer();

        val typeBuilder = SegmentType.builder().tableSegment();
        val configBuilder = TableSegmentConfig.builder();
        if (createTableSegment.getKeyLength() > 0) {
            typeBuilder.fixedKeyLengthTableSegment();
            configBuilder.keyLength(createTableSegment.getKeyLength());
        }

        if (createTableSegment.getRolloverSizeBytes() < 0) {
            log.warn("Table segment rollover size bytes cannot be less than 0, actual is {}, fall back to default value", createTableSegment.getRolloverSizeBytes());
        }
        final long rolloverSizeByes = createTableSegment.getRolloverSizeBytes() < 0 ? 0 : createTableSegment.getRolloverSizeBytes();
        configBuilder.rolloverSizeBytes(rolloverSizeByes);

        tableStore.createSegment(createTableSegment.getSegment(), typeBuilder.build(), configBuilder.build(), TIMEOUT)
                .thenAccept(v -> {
                    connection.send(new SegmentCreated(createTableSegment.getRequestId(), createTableSegment.getSegment()));
                    this.tableStatsRecorder.createTableSegment(createTableSegment.getSegment(), timer.getElapsed());
                })
                .exceptionally(e -> handleException(createTableSegment.getRequestId(), createTableSegment.getSegment(), operation, e));
    }

    @Override
    public void deleteTableSegment(final DeleteTableSegment deleteTableSegment) {
        String segment = deleteTableSegment.getSegment();
        final String operation = "deleteTableSegment";

        if (!verifyToken(segment, deleteTableSegment.getRequestId(), deleteTableSegment.getDelegationToken(), operation)) {
            return;
        }

        log.info(deleteTableSegment.getRequestId(), "Deleting table segment {}.", deleteTableSegment);
        val timer = new Timer();
        tableStore.deleteSegment(segment, deleteTableSegment.isMustBeEmpty(), TIMEOUT)
                  .thenRun(() -> {
                      connection.send(new SegmentDeleted(deleteTableSegment.getRequestId(), segment));
                      this.tableStatsRecorder.deleteTableSegment(segment, timer.getElapsed());
                  })
                  .exceptionally(e -> handleException(deleteTableSegment.getRequestId(), segment, operation, e));
    }

    @Override
    public void updateTableEntries(final WireCommands.UpdateTableEntries updateTableEntries) {
        String segment = updateTableEntries.getSegment();
        final String operation = "updateTableEntries";

        if (!verifyToken(segment, updateTableEntries.getRequestId(), updateTableEntries.getDelegationToken(), operation)) {
            updateTableEntries.release();
            return;
        }

        log.debug(updateTableEntries.getRequestId(), "Update Table Segment Entries: Segment={}, Offset={}, Count={}.",
                updateTableEntries.getSegment(), updateTableEntries.getTableSegmentOffset(), updateTableEntries.getTableEntries().getEntries().size());
        val entries = new ArrayList<TableEntry>(updateTableEntries.getTableEntries().getEntries().size());
        val conditional = new AtomicBoolean(false);
        val size = new AtomicInteger(0);
        for (val e : updateTableEntries.getTableEntries().getEntries()) {
            val v = TableEntry.versioned(new ByteBufWrapper(e.getKey().getData()), new ByteBufWrapper(e.getValue().getData()), e.getKey().getKeyVersion());
            entries.add(v);
            size.addAndGet(v.getKey().getKey().getLength() + v.getValue().getLength());
            if (v.getKey().hasVersion()) {
                conditional.set(true);
            }
        }

        val timer = new Timer();
        this.connection.adjustOutstandingBytes(size.get());
        tableStore.put(segment, entries, updateTableEntries.getTableSegmentOffset(), TIMEOUT)
                .thenAccept(versions -> {
                    connection.send(new WireCommands.TableEntriesUpdated(updateTableEntries.getRequestId(), versions));
                    this.tableStatsRecorder.updateEntries(updateTableEntries.getSegment(), entries.size(), conditional.get(), timer.getElapsed());
                })
                .exceptionally(e -> handleException(updateTableEntries.getRequestId(), segment, updateTableEntries.getTableSegmentOffset(), operation, e))
                .whenComplete((r, ex) -> {
                    this.connection.adjustOutstandingBytes(-size.get());
                    updateTableEntries.release();
                });
    }

    @Override
    public void removeTableKeys(final WireCommands.RemoveTableKeys removeTableKeys) {
        String segment = removeTableKeys.getSegment();
        final String operation = "removeTableKeys";

        if (!verifyToken(segment, removeTableKeys.getRequestId(), removeTableKeys.getDelegationToken(), operation)) {
            removeTableKeys.release();
            return;
        }

        log.debug(removeTableKeys.getRequestId(), "Remove Table Segment Keys: Segment={}, Offset={}, Count={}.",
                removeTableKeys.getSegment(), removeTableKeys.getTableSegmentOffset(), removeTableKeys.getKeys().size());
        val keys = new ArrayList<TableKey>(removeTableKeys.getKeys().size());
        val conditional = new AtomicBoolean(false);
        val size = new AtomicInteger(0);
        for (val k : removeTableKeys.getKeys()) {
            val v = TableKey.versioned(new ByteBufWrapper(k.getData()), k.getKeyVersion());
            keys.add(v);
            size.addAndGet(v.getKey().getLength());
            if (v.hasVersion()) {
                conditional.set(true);
            }
        }

        val timer = new Timer();
        this.connection.adjustOutstandingBytes(size.get());
        tableStore.remove(segment, keys, removeTableKeys.getTableSegmentOffset(), TIMEOUT)
                .thenRun(() -> {
                    connection.send(new WireCommands.TableKeysRemoved(removeTableKeys.getRequestId(), segment));
                    this.tableStatsRecorder.removeKeys(removeTableKeys.getSegment(), keys.size(), conditional.get(), timer.getElapsed());
                })
                .exceptionally(e -> handleException(removeTableKeys.getRequestId(), segment, removeTableKeys.getTableSegmentOffset(), operation, e))
                .whenComplete((r, ex) -> {
                    this.connection.adjustOutstandingBytes(-size.get());
                    removeTableKeys.release();
                });
    }

    @Override
    public void readTable(final WireCommands.ReadTable readTable) {
        final String segment = readTable.getSegment();
        final String operation = "readTable";

        if (!verifyToken(segment, readTable.getRequestId(), readTable.getDelegationToken(), operation)) {
            readTable.release();
            return;
        }

        log.debug(readTable.getRequestId(), "Get Table Segment Keys: Segment={}, Count={}.",
                readTable.getSegment(), readTable.getKeys());

        final List<BufferView> keys = readTable.getKeys().stream()
                .map(k -> new ByteBufWrapper(k.getData()))
                .collect(Collectors.toList());
        val timer = new Timer();
        tableStore.get(segment, keys, TIMEOUT)
                .thenAccept(values -> {
                    connection.send(new WireCommands.TableRead(readTable.getRequestId(), segment, getTableEntriesCommand(keys, values)));
                    this.tableStatsRecorder.getKeys(readTable.getSegment(), keys.size(), timer.getElapsed());
                })
                .exceptionally(e -> handleException(readTable.getRequestId(), segment, operation, e))
                .whenComplete((r, ex) -> readTable.release());
    }

    @Override
    public void readTableKeys(WireCommands.ReadTableKeys readTableKeys) {
        final String segment = readTableKeys.getSegment();
        final String operation = "readTableKeys";

        if (!verifyToken(segment, readTableKeys.getRequestId(), readTableKeys.getDelegationToken(), operation)) {
            return;
        }

        log.debug(readTableKeys.getRequestId(), "Iterate Table Segment Keys: Segment={}, Count={}.",
                readTableKeys.getSegment(), readTableKeys.getSuggestedKeyCount());

        final int suggestedKeyCount = readTableKeys.getSuggestedKeyCount();
        final IteratorArgs args = getIteratorArgs(readTableKeys.getArgs());

        val result = new IteratorResult<WireCommands.TableKey>(segment.getBytes().length + WireCommands.TableKeysRead.HEADER_BYTES);
        val timer = new Timer();
        tableStore.keyIterator(segment, args)
                .thenCompose(itr -> itr.collectRemaining(e -> {
                    synchronized (result) {
                        if (result.getItemCount() >= suggestedKeyCount || result.getSizeBytes() >= MAX_READ_SIZE) {
                            return false;
                        }

                        // Store all TableKeys.
                        for (val key : e.getEntries()) {
                            val k = new WireCommands.TableKey(toByteBuf(key.getKey()), key.getVersion());
                            result.add(k, k.size());
                        }

                        // Update the continuation token.
                        result.setContinuationToken(e.getState());
                        return true;
                    }
                }))
                .thenAccept(v -> {
                    log.debug(readTableKeys.getRequestId(), "Iterate Table Segment Keys complete ({}).", result.getItemCount());
                    connection.send(new WireCommands.TableKeysRead(readTableKeys.getRequestId(), segment, result.getItems(), toByteBuf(result.getContinuationToken())));
                    this.tableStatsRecorder.iterateKeys(readTableKeys.getSegment(), result.getItemCount(), timer.getElapsed());
                }).exceptionally(e -> handleException(readTableKeys.getRequestId(), segment, operation, e));
    }

    @Override
    public void readTableEntries(WireCommands.ReadTableEntries readTableEntries) {
        final String segment = readTableEntries.getSegment();
        final String operation = "readTableEntries";

        if (!verifyToken(segment, readTableEntries.getRequestId(), readTableEntries.getDelegationToken(), operation)) {
            return;
        }

        log.debug(readTableEntries.getRequestId(), "Iterate Table Segment Entries: Segment={}, Count={}.",
                readTableEntries.getSegment(), readTableEntries.getSuggestedEntryCount());

        final int suggestedEntryCount = readTableEntries.getSuggestedEntryCount();
        final IteratorArgs args = getIteratorArgs(readTableEntries.getArgs());

        val result = new IteratorResult<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>>(segment.getBytes().length + WireCommands.TableEntriesRead.HEADER_BYTES);
        val timer = new Timer();
        tableStore.entryIterator(segment, args)
                .thenCompose(itr -> itr.collectRemaining(
                        e -> {
                            if (result.getItemCount() >= suggestedEntryCount || result.getSizeBytes() >= MAX_READ_SIZE) {
                                return false;
                            }

                            // Store all TableEntries.
                            for (val entry : e.getEntries()) {
                                val k = new WireCommands.TableKey(toByteBuf(entry.getKey().getKey()), entry.getKey().getVersion());
                                val v = new WireCommands.TableValue(toByteBuf(entry.getValue()));
                                result.add(new AbstractMap.SimpleImmutableEntry<>(k, v), k.size() + v.size());
                            }

                            // Update the continuation token.
                            result.setContinuationToken(e.getState());
                            return true;
                        }))
                .thenAccept(v -> {
                    log.debug(readTableEntries.getRequestId(), "Iterate Table Segment Entries complete ({}).", result.getItemCount());
                    connection.send(new WireCommands.TableEntriesRead(readTableEntries.getRequestId(), segment,
                            new WireCommands.TableEntries(result.getItems()), toByteBuf(result.getContinuationToken())));
                    this.tableStatsRecorder.iterateEntries(readTableEntries.getSegment(), result.getItemCount(), timer.getElapsed());
                }).exceptionally(e -> handleException(readTableEntries.getRequestId(), segment, operation, e));
    }

    private IteratorArgs getIteratorArgs(WireCommands.TableIteratorArgs rawArgs) {
        // NOTE: Prefix filter is deprecated, hence it is ignored.
        return IteratorArgs.builder().fetchTimeout(TIMEOUT)
                .continuationToken(wrap(rawArgs.getContinuationToken()))
                .from(wrap(rawArgs.getFromKey()))
                .to(wrap(rawArgs.getToKey()))
                .build();
    }

    private ByteBufWrapper wrap(ByteBuf buf) {
        return buf == null || buf.equals(EMPTY_BUFFER) ? null : new ByteBufWrapper(buf);
    }

    private CompletableFuture<Long> getSegmentEventCount(String segmentName) {
        if (isTransientSegment(segmentName)) {
            return CompletableFuture.completedFuture(1L);
        } else {
            return segmentStore.getAttributes(segmentName, Collections.singleton(EVENT_COUNT), false, TIMEOUT)
                    .thenApply(properties -> properties.getOrDefault(EVENT_COUNT, 0L));
        }
    }

    @Override
    public void readTableEntriesDelta(WireCommands.ReadTableEntriesDelta readTableEntriesDelta) {
        final String segment = readTableEntriesDelta.getSegment();
        final String operation = "readTableEntriesDelta";

        if (!verifyToken(segment, readTableEntriesDelta.getRequestId(), readTableEntriesDelta.getDelegationToken(), operation)) {
            return;
        }

        final int suggestedEntryCount = readTableEntriesDelta.getSuggestedEntryCount();
        final long fromPosition = readTableEntriesDelta.getFromPosition();

        log.info(readTableEntriesDelta.getRequestId(), "Iterate Table Entries Delta: Segment={} Count={} FromPositon={}.",
                readTableEntriesDelta.getSegment(),
                readTableEntriesDelta.getSuggestedEntryCount(),
                readTableEntriesDelta.getFromPosition());

        val timer = new Timer();
        val result = new DeltaIteratorResult<BufferView, Map.Entry<WireCommands.TableKey, WireCommands.TableValue>>(
                segment.getBytes().length + WireCommands.TableEntriesRead.HEADER_BYTES);
        tableStore.entryDeltaIterator(segment, fromPosition, TIMEOUT)
                .thenCompose(itr -> itr.collectRemaining(
                        e -> {
                            if (result.getItemCount() >= suggestedEntryCount || result.getSizeBytes() >= MAX_READ_SIZE) {
                                return  false;
                            }
                            TableEntry entry = e.getEntries().iterator().next();
                            DeltaIteratorState state = DeltaIteratorState.deserialize(e.getState());
                            // Store all TableEntries.
                            val k = new WireCommands.TableKey(toByteBuf(entry.getKey().getKey()), entry.getKey().getVersion());
                            val v = new WireCommands.TableValue(toByteBuf(entry.getValue()));
                            if (state.isDeletionRecord()) {
                                result.remove(entry.getKey().getKey(), k.size() + v.size());
                            } else {
                                Map.Entry<WireCommands.TableKey, WireCommands.TableValue> old = result.getItem(entry.getKey().getKey());
                                if (old != null && old.getKey().getKeyVersion() < entry.getKey().getVersion()) {
                                    int sizeBytes = (k.size() + v.size()) - (old.getKey().size() + old.getValue().size());
                                    result.add(entry.getKey().getKey(), new AbstractMap.SimpleImmutableEntry<>(k, v), sizeBytes);
                                } else {
                                    result.add(entry.getKey().getKey(), new AbstractMap.SimpleImmutableEntry<>(k, v), k.size() + v.size());
                                }
                            }
                            result.setState(state);
                            // Update total read data.
                            return true;
                        }))
                .thenAccept(v -> {
                    log.debug(readTableEntriesDelta.getRequestId(), "Iterate Table Segment Entries Delta complete ({}).", result.getItemCount());
                    connection.send(new WireCommands.TableEntriesDeltaRead(
                            readTableEntriesDelta.getRequestId(),
                            segment,
                            new WireCommands.TableEntries(result.getItems()),
                            result.getState().isShouldClear(),
                            result.getState().isReachedEnd(),
                            result.getState().getFromPosition()));
                    this.tableStatsRecorder.iterateEntries(readTableEntriesDelta.getSegment(), result.getItemCount(), timer.getElapsed());
                }).exceptionally(e -> handleException(readTableEntriesDelta.getRequestId(), segment, operation, e));

    }

    private CompletableFuture<Void> truncateIndexSegment(String segment, long offset) {
        String indexSegment = getIndexSegmentName(segment);
        if (!isUserStreamSegment(segment)) {
            log.debug("No need to perform truncation of index segment for {}.", segment);
            return CompletableFuture.completedFuture(null);
        }
        return IndexRequestProcessor.locateTruncateOffsetInIndexSegment(segmentStore, segment, offset).exceptionally(e -> {
            Throwable ex = Exceptions.unwrap(e);
            if (ex instanceof StreamSegmentNotExistsException) {
                log.info("Stream segment {} does not exists, so skipping truncation of index segment.", segment);
                return 0L;
            }
            log.warn("Unable to locate offset for index segment {} for offset {} due to ", indexSegment, offset, e);
            // throw  exception to the caller.
            throw new CompletionException(ex); 
        }).thenCompose(indexSegmentOffset -> {
            if (indexSegmentOffset == 0) {
                log.debug("Index Segment {} offset is 0. No need to truncate it.", indexSegment);
                return CompletableFuture.completedFuture(null);
            }
            log.info("Truncating index segment {} at offset {}.", indexSegment, indexSegmentOffset);
            return segmentStore.truncateStreamSegment(indexSegment, indexSegmentOffset, TIMEOUT);
        });
    }

    private WireCommands.TableEntries getTableEntriesCommand(final List<BufferView> inputKeys, final List<TableEntry> resultEntries) {
        Preconditions.checkArgument(resultEntries.size() == inputKeys.size(), "Number of input keys should match result entry count.");
        final List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries =
                IntStream.range(0, resultEntries.size())
                         .mapToObj(i -> {
                             TableEntry resultTableEntry = resultEntries.get(i);
                             if (resultTableEntry == null) { // no entry for key at index i.
                                 BufferView k = inputKeys.get(i); // key for which the read result was null.
                                 val keyWireCommand = new WireCommands.TableKey(toByteBuf(k), TableKey.NOT_EXISTS);
                                 return new AbstractMap.SimpleImmutableEntry<>(keyWireCommand, WireCommands.TableValue.EMPTY);
                             } else {
                                 TableEntry te = resultEntries.get(i);
                                 TableKey k = te.getKey();
                                 val keyWireCommand = new WireCommands.TableKey(toByteBuf(k.getKey()), k.getVersion());
                                 val valueWireCommand = new WireCommands.TableValue(toByteBuf(te.getValue()));
                                 return new AbstractMap.SimpleImmutableEntry<>(keyWireCommand, valueWireCommand);

                             }
                         }).collect(toList());

        return new WireCommands.TableEntries(entries);
    }

    //endregion

    Void handleException(long requestId, String segment, String operation, Throwable u) {
        // use offset as -1L to handle exceptions when offset data is not available.
        return handleException(requestId, segment, -1L, operation, u);
    }

    private Void handleException(long requestId, String segment, long offset, String operation, Throwable u) {
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
            invokeSafely(connection::send, new NoSuchSegment(requestId, segment, clientReplyStackTrace, offset), failureHandler);
        } else if (u instanceof StreamSegmentSealedException) {
            log.info(requestId, "Segment '{}' is sealed and cannot perform operation '{}'.",
                     segment, operation);
            invokeSafely(connection::send, new SegmentIsSealed(requestId, segment, clientReplyStackTrace, offset), failureHandler);
        } else if (u instanceof ContainerNotFoundException) {
            int containerId = ((ContainerNotFoundException) u).getContainerId();
            log.warn(requestId, "Wrong host. Segment = '{}' (Container {}) is not owned. Operation = '{}').",
                     segment, containerId, operation);
            invokeSafely(connection::send, new WrongHost(requestId, segment, "", clientReplyStackTrace), failureHandler);
        } else if (u instanceof ReadCancellationException) {
            log.info(requestId, "Sending empty response on connection {} while reading segment {} due to CancellationException.",
                    connection, segment);
            invokeSafely(connection::send, new SegmentRead(segment, offset, true, false, EMPTY_BUFFER, requestId), failureHandler);
        } else if (u instanceof CancellationException) {
            log.info(requestId, "Closing connection {} while performing {} due to {}.",
                    connection, operation, u.toString());
            connection.close();
        } else if (u instanceof TokenExpiredException) {
            log.warn(requestId, "Expired token during operation {}", operation);
            invokeSafely(connection::send, new AuthTokenCheckFailed(requestId, clientReplyStackTrace,
                    AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED), failureHandler);
        } else if (u instanceof TokenException) {
            log.warn(requestId, "Token exception encountered during operation {}.", operation, u);
            invokeSafely(connection::send, new AuthTokenCheckFailed(requestId, clientReplyStackTrace,
                    AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED), failureHandler);
        } else if (u instanceof UnsupportedOperationException) {
            log.warn(requestId, "Unsupported Operation '{}'.", operation, u);
            invokeSafely(connection::send, new OperationUnsupported(requestId, operation, clientReplyStackTrace), failureHandler);
        } else if (u instanceof BadOffsetException) {
            BadOffsetException badOffset = (BadOffsetException) u;
            log.info(requestId, "Segment '{}' is truncated and cannot perform operation '{}' at offset '{}'", segment, operation, offset);
            invokeSafely(connection::send, new SegmentIsTruncated(requestId, segment, badOffset.getExpectedOffset(),
                                                                  clientReplyStackTrace, offset), failureHandler);
        } else if (u instanceof TableSegmentNotEmptyException) {
            log.warn(requestId, "Table segment '{}' is not empty to perform '{}'.", segment, operation);
            invokeSafely(connection::send, new TableSegmentNotEmpty(requestId, segment, clientReplyStackTrace), failureHandler);
        } else if (u instanceof KeyNotExistsException) {
            log.warn(requestId, "Conditional update on Table segment '{}' failed as the key does not exist.", segment);
            invokeSafely(connection::send, new WireCommands.TableKeyDoesNotExist(requestId, segment, clientReplyStackTrace), failureHandler);
        } else if (u instanceof BadKeyVersionException) {
            log.warn(requestId, "Conditional update on Table segment '{}' failed due to bad key version.", segment);
            invokeSafely(connection::send, new WireCommands.TableKeyBadVersion(requestId, segment, clientReplyStackTrace), failureHandler);
        } else if (u instanceof IndexRequestProcessor.SegmentTruncatedException) {
            log.warn(requestId, "Segment '{}' is truncated and cannot perform operation '{}'", segment, operation);
            invokeSafely(connection::send, new SegmentTruncated(requestId, segment), failureHandler);
        } else if (errorCodeExists(u)) {
            log.warn(requestId, "Operation on segment '{}' failed due to a {}.", segment, u.getClass());
            invokeSafely(connection::send,
                    new WireCommands.ErrorMessage(requestId, segment, u.getMessage(), WireCommands.ErrorMessage.ErrorCode.valueOf(u.getClass())),
                    failureHandler);
        } else {
            logError(requestId, segment, operation, u);
            connection.close(); // Closing connection should reinitialize things, and hopefully fix the problem
            throw new IllegalStateException("Unknown exception.", u);
        }

        return null;
    }

    private boolean errorCodeExists(Throwable e) {
        ErrorCode errorCode = WireCommands.ErrorMessage.ErrorCode.valueOf(e.getClass());
        return errorCode != WireCommands.ErrorMessage.ErrorCode.UNSPECIFIED;
    }

    private void logError(long requestId, String segment, String operation, Throwable u) {
        if (u instanceof IllegalContainerStateException) {
            log.warn(requestId, "Error (Segment = '{}', Operation = '{}'): {}", segment, operation, u.toString());
        } else {
            log.error(requestId, "Error (Segment = '{}', Operation = '{}')", segment, operation, u);
        }
    }

    private void recordStatForTransaction(MergeStreamSegmentResult mergeResult, String targetSegmentName) {
        try {
            if (mergeResult != null &&
                    mergeResult.getAttributes().containsKey(Attributes.CREATION_TIME) &&
                            mergeResult.getAttributes().containsKey(Attributes.EVENT_COUNT)) {
                long creationTime = mergeResult.getAttributes().get(Attributes.CREATION_TIME);
                int numOfEvents = mergeResult.getAttributes().get(Attributes.EVENT_COUNT).intValue();
                long len = mergeResult.getMergedDataLength();
                statsRecorder.merge(targetSegmentName, len, numOfEvents, creationTime);
            }
        } catch (Exception ex) {
            // gobble up any errors from stat recording so we do not affect rest of the flow.
            log.error("exception while computing stats while merging txn into {}", targetSegmentName, ex);
        }
    }

    /**
     * Custom exception to indicate a {@link CancellationException} during a Read segment operation.
     */
    private static class ReadCancellationException extends RuntimeException {
        ReadCancellationException(Throwable wrappedException) {
            super("CancellationException during operation Read segment", wrappedException);
        }
    }

    //region IteratorResult

    /**
     * Helps collect Iterator Items from {@link TableStore#keyIterator} or {@link TableStore#entryIterator}.
     */
    @ThreadSafe
    private static class IteratorResult<T> {
        @GuardedBy("this")
        private final ArrayList<T> items = new ArrayList<>();
        @GuardedBy("this")
        private BufferView continuationToken = BufferView.empty();
        @GuardedBy("this")
        private int sizeBytes;

        IteratorResult(int initialSizeBytes) {
            this.sizeBytes = initialSizeBytes;
        }

        synchronized void add(T item, int sizeBytes) {
            this.items.add(item);
            this.sizeBytes += sizeBytes;
        }

        synchronized int getItemCount() {
            return this.items.size();
        }

        synchronized int getSizeBytes() {
            return this.sizeBytes;
        }

        synchronized List<T> getItems() {
            // We need to make a copy of the items while holding the lock. This is because there is no collection implementation
            // available in Java that will synchronize the iterator of such collection, yet the Netty send() call will
            // invoke this iterator (when serializing the WireCommand) on a different thread, which would create a
            // thread-safety issue.
            return new ArrayList<>(this.items);
        }

        synchronized void setContinuationToken(BufferView continuationToken) {
            this.sizeBytes = this.sizeBytes - this.continuationToken.getLength() + continuationToken.getLength();
            this.continuationToken = continuationToken;
        }

        synchronized BufferView getContinuationToken() {
            return this.continuationToken;
        }
    }

    private static class DeltaIteratorResult<K, V> {
        @Getter
        @Setter
        @GuardedBy("this")
        private DeltaIteratorState state = new DeltaIteratorState();
        @GuardedBy("this")
        private final Map<K, V> items = new HashMap<>();
        @Getter
        @GuardedBy("this")
        private int sizeBytes;

        DeltaIteratorResult(int initialSizeBytes) {
            this.sizeBytes = initialSizeBytes;
        }

        synchronized void add(K key, V value, int sizeBytes) {
            this.items.put(key, value);
            this.sizeBytes += sizeBytes;
        }

        synchronized void remove(K item, int sizeBytes) {
            this.items.remove(item);
            this.sizeBytes -= sizeBytes;
        }

        synchronized V getItem(K key) {
            return this.items.get(key);
        }

        synchronized List<V> getItems() {
            return new ArrayList<>(this.items.values());
        }

        synchronized int getItemCount() {
            return this.items.size();
        }

    }


    //endregion
}
