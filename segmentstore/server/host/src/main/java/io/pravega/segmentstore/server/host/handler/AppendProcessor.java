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
import io.pravega.auth.AuthHandler;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.DelegatingRequestProcessor;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransientSegment;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import io.pravega.shared.security.token.JsonWebToken;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import static io.pravega.segmentstore.contracts.Attributes.ATTRIBUTE_SEGMENT_TYPE;
import static io.pravega.segmentstore.contracts.Attributes.CREATION_TIME;
import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.segmentstore.contracts.Attributes.EXPECTED_INDEX_SEGMENT_EVENT_SIZE;
import static io.pravega.shared.NameUtils.INDEX_APPEND_EVENT_SIZE;
import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static io.pravega.shared.NameUtils.isTransactionSegment;
import static io.pravega.shared.NameUtils.isTransientSegment;
import static io.pravega.shared.NameUtils.isUserStreamSegment;

/**
 * Process incoming Append requests and write them to the SegmentStore.
 */
public class AppendProcessor extends DelegatingRequestProcessor implements AutoCloseable {
    //region Members

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final String EMPTY_STACK_TRACE = "";
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(AppendProcessor.class));
    private final StreamSegmentStore store;
    private final TrackedConnection connection;
    @Getter
    private final RequestProcessor nextRequestProcessor;
    private final SegmentStatsRecorder statsRecorder;
    private final DelegationTokenVerifier tokenVerifier;
    private final boolean replyWithStackTraceOnError;
    private final ConcurrentHashMap<Pair<String, UUID>, WriterState> writerStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService tokenExpiryHandlerExecutor;
    private final Collection<String> transientSegmentNames;
    private final IndexAppendProcessor indexAppendProcessor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    //endregion

    //region Builder

    @Builder
    AppendProcessor(@NonNull StreamSegmentStore store, @NonNull TrackedConnection connection, @NonNull RequestProcessor nextRequestProcessor,
                    @NonNull SegmentStatsRecorder statsRecorder, DelegationTokenVerifier tokenVerifier,
                    boolean replyWithStackTraceOnError, ScheduledExecutorService tokenExpiryHandlerExecutor, IndexAppendProcessor indexAppendProcessor) {
        this.store = store;
        this.connection = connection;
        this.nextRequestProcessor = nextRequestProcessor;
        this.statsRecorder = statsRecorder;
        this.tokenVerifier = tokenVerifier;
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
        this.tokenExpiryHandlerExecutor = tokenExpiryHandlerExecutor;
        this.transientSegmentNames = Collections.synchronizedSet(new HashSet<>());
        this.indexAppendProcessor = indexAppendProcessor;
    }

    /**
     * Creates a new {@link AppendProcessorBuilder} instance with all optional arguments set to default values.
     * These default values may not be appropriate for production use and should be used for testing purposes only.
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     * @return A {@link AppendProcessorBuilder} instance.
     */
    @VisibleForTesting
    public static AppendProcessorBuilder defaultBuilder(IndexAppendProcessor indexAppendProcessor) {
        return builder()
                .nextRequestProcessor(new FailingRequestProcessor())
                .statsRecorder(SegmentStatsRecorder.noOp())
                .indexAppendProcessor(indexAppendProcessor)
                .replyWithStackTraceOnError(false);
    }

    //endregion

    //region RequestProcessor Implementation

    @Override
    public void hello(Hello hello) {
        log.info("Received hello from connection: {}", connection);
        connection.send(new Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
        if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
            log.warn(hello.getRequestId(), "Incompatible wire protocol versions {} from connection {}", hello, connection);
            close();
        }
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        log.debug("Received a keepAlive from connection: {}", connection);
        connection.send(keepAlive);
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
        UUID writer = setupAppend.getWriterId();
        log.info("Setting up appends for writer: {} on segment: {}", writer, newSegment);

        if (this.tokenVerifier != null) {
            try {
                JsonWebToken token = tokenVerifier.verifyToken(newSegment,
                        setupAppend.getDelegationToken(),
                        AuthHandler.Permissions.READ_UPDATE);
                setupTokenExpiryTask(setupAppend, token);
            } catch (TokenException e) {
                handleException(setupAppend.getWriterId(), setupAppend.getRequestId(), newSegment,
                        "Update Segment Attribute", e);
                return;
            }
        }

        // Get the last Event Number for this writer from the Store. This operation (cache=true) will automatically put
        // the value in the Store's cache so it's faster to access later.
        AttributeId writerAttributeId = AttributeId.fromUUID(writer);
        Futures.exceptionallyComposeExpecting(
                store.getAttributes(newSegment, Collections.singleton(writerAttributeId), true, TIMEOUT),
                e -> e instanceof StreamSegmentSealedException, () -> store.getAttributes(newSegment, Collections.singleton(writerAttributeId), false, TIMEOUT))
                        .whenComplete((attributes, u) -> {
                            try {
                                if (u != null) {
                                    handleException(writer, setupAppend.getRequestId(), newSegment, "setting up append", u);
                                } else {
                                    // Last event number stored according to Segment store.
                                    long eventNumber = attributes.getOrDefault(writerAttributeId, Attributes.NULL_ATTRIBUTE_VALUE);
                                    CompletableFuture<Long> indexSegmentEventsize;

                                    if (!isTransientSegment(newSegment) && !isTransactionSegment(newSegment) && isUserStreamSegment(newSegment)) {
                                        String indexSegment = getIndexSegmentName(newSegment);
                                        indexSegmentEventsize = createIndexSegmentIfNotExists(indexSegment, setupAppend.getRequestId());
                                    } else {
                                        indexSegmentEventsize = CompletableFuture.completedFuture(0L);
                                    }

                                    indexSegmentEventsize.thenAccept(eventSize -> {
                                        // Create a new WriterState object based on the attribute value for the last event number for the writer.
                                        // It should be noted that only one connection for a given segment writer is created by the client.
                                        // The event number sent by the AppendSetup command is an implicit ack, the writer acks all events
                                        // below the specified event number.
                                        WriterState current = this.writerStates.put(Pair.of(newSegment, writer), new WriterState(eventNumber, eventSize));
                                        if (current != null) {
                                            log.info("SetupAppend invoked again for writer {}. Last event number from store is {}. Prev writer state {}",
                                                    writer, eventNumber, current);
                                        }
                                        connection.send(new AppendSetup(setupAppend.getRequestId(), newSegment, writer, eventNumber));
                                    }).exceptionally(e -> handleException(writer, setupAppend.getRequestId(), getIndexSegmentName(newSegment), "creating index segment", e));
                                }
                            } catch (Throwable e) {
                                handleException(writer, setupAppend.getRequestId(), newSegment, "handling setupAppend result", e);
                            }
                        });
    }

    private CompletableFuture<Long> createIndexSegmentIfNotExists(String indexSegment, long requestId) {
        return store.getAttributes(indexSegment, List.of(EXPECTED_INDEX_SEGMENT_EVENT_SIZE), true, TIMEOUT)
                    .exceptionally(ex -> {
                        log.warn("Unable to get attribute for index segment {} due to ", indexSegment, ex);
                        ex = Exceptions.unwrap(ex);
                        if (ex instanceof NoSuchSegmentException || ex instanceof StreamSegmentNotExistsException) {
                            return Map.of(EXPECTED_INDEX_SEGMENT_EVENT_SIZE, -1L);
                        } else {
                            throw Exceptions.sneakyThrow(ex);
                        }
                    }).thenCompose(value -> {
                        Long size = value.getOrDefault(EXPECTED_INDEX_SEGMENT_EVENT_SIZE, 0L);
                        if (size != -1) {
                            return CompletableFuture.completedFuture(size);
                        } 
                        return createIndexSegmentAndFetchEventSize(indexSegment, requestId);
                    });
    }

    private CompletableFuture<Long> createIndexSegmentAndFetchEventSize(String indexSegment, long requestId) {
        log.info("Creating index segment {} while processing request: {}.", indexSegment, requestId);
        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(CREATION_TIME, AttributeUpdateType.None, System.currentTimeMillis()),
                new AttributeUpdate(ATTRIBUTE_SEGMENT_TYPE, AttributeUpdateType.None, SegmentType.STREAM_SEGMENT.getValue()),
                new AttributeUpdate(EXPECTED_INDEX_SEGMENT_EVENT_SIZE, AttributeUpdateType.None, INDEX_APPEND_EVENT_SIZE)
        );
        return store.createStreamSegment(indexSegment, SegmentType.STREAM_SEGMENT, attributes, TIMEOUT)
                    .thenApply(eventSize -> {
                        log.info("Index segment {} created successfully.", indexSegment);
                        return Long.valueOf(INDEX_APPEND_EVENT_SIZE);
                    });
    }

    @VisibleForTesting
    CompletableFuture<Void> setupTokenExpiryTask(@NonNull SetupAppend setupAppend, @NonNull JsonWebToken token) {
        String segment = setupAppend.getSegment();
        UUID writerId = setupAppend.getWriterId();
        long requestId = setupAppend.getRequestId();

        if (token.getExpirationTime() == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return Futures.delayedTask(() -> {
                if (isSetupAppendCompleted(segment, writerId)) {
                    // Closing the connection will result in client authenticating with Controller again
                    // and retrying the request with a new token.
                    log.debug("Closing client connection for writer {} due to token expiry, when processing " +
                                "request {} for segment {}", writerId, requestId, segment);
                    this.close();
                }
                return null;
            }, token.durationToExpiry(), this.tokenExpiryHandlerExecutor);
        }
    }

    @VisibleForTesting
    boolean isSetupAppendCompleted(String newSegment, UUID writer) {
        return writerStates.containsKey(Pair.of(newSegment, writer));
    }

    /**
     * Append data to the store.
     */
    @Override
    public void append(Append append) {
        long traceId = LoggerHelpers.traceEnter(log, "append", append);
        UUID id = append.getWriterId();
        WriterState state = this.writerStates.get(Pair.of(append.getSegment(), id));
        Preconditions.checkState(state != null, "Data from unexpected connection: Segment=%s, WriterId=%s.", append.getSegment(), id);
        long previousEventNumber = state.beginAppend(append.getEventNumber());
        int appendLength = append.getData().readableBytes();
        this.connection.adjustOutstandingBytes(appendLength);
        Timer timer = new Timer();
        storeAppend(append, previousEventNumber)
                .whenComplete((newLength, ex) -> {
                    handleAppendResult(append, newLength, ex, state, timer);
                    LoggerHelpers.traceLeave(log, "storeAppend", traceId, append, ex);
                })
                .whenComplete((v, e) -> {
                    this.connection.adjustOutstandingBytes(-appendLength);
                    append.getData().release();
                });
    }

    @Override
    public void createTransientSegment(CreateTransientSegment createTransientSegment) {
        String operation = "createTransientSegment";
        long traceId = LoggerHelpers.traceEnter(log, operation, createTransientSegment);
        Collection<AttributeUpdate> attributes = Arrays.asList(
                new AttributeUpdate(CREATION_TIME, AttributeUpdateType.None, System.currentTimeMillis()),
                new AttributeUpdate(ATTRIBUTE_SEGMENT_TYPE, AttributeUpdateType.None, SegmentType.TRANSIENT_SEGMENT.getValue())
        );
        String transientSegmentName = NameUtils.getTransientNameFromId(createTransientSegment.getParentSegment(), createTransientSegment.getWriterId());
        store.createStreamSegment(transientSegmentName, SegmentType.TRANSIENT_SEGMENT, attributes, TIMEOUT)
                .thenAccept(v -> {
                    transientSegmentNames.add(transientSegmentName);
                    connection.send(new SegmentCreated(createTransientSegment.getRequestId(), transientSegmentName));
                })
                .exceptionally(e -> handleException(createTransientSegment.getWriterId(),
                        createTransientSegment.getRequestId(),
                        transientSegmentName,
                        operation,
                        e));

        LoggerHelpers.traceLeave(log, operation, traceId);
    }


    private CompletableFuture<Long> storeAppend(Append append, long lastEventNumber) {
        AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
                new AttributeUpdate(AttributeId.fromUUID(append.getWriterId()), AttributeUpdateType.ReplaceIfEquals, append.getEventNumber(), lastEventNumber),
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, append.getEventCount()));
        ByteBufWrapper buf = new ByteBufWrapper(append.getData());
        if (append.isConditional()) {
            return store.append(append.getSegment(), append.getExpectedLength(), buf, attributes, TIMEOUT);
        } else {
            return store.append(append.getSegment(), buf, attributes, TIMEOUT);
        }
    }

    private void handleAppendResult(final Append append, Long newWriteOffset, Throwable exception, WriterState state, Timer elapsedTimer) {
        Preconditions.checkNotNull(state, "state");
        boolean success = exception == null;
        try {
            if (success) {
                appendOnIndexSegment(append);
                synchronized (state.getAckLock()) {
                    // Acks must be sent in order. The only way to do this is by using a lock.
                    long previousLastAcked = state.appendSuccessful(append.getEventNumber());
                    if (previousLastAcked < append.getEventNumber()) {
                        final DataAppended dataAppendedAck = new DataAppended(append.getRequestId(), append.getWriterId(), append.getEventNumber(),
                                previousLastAcked, newWriteOffset);
                        log.trace("Sending DataAppended : {}", dataAppendedAck);
                        connection.send(dataAppendedAck);
                    }
                }

                if (append.getEventNumber() > state.getLowestFailedEventNumber()) {
                    // The Store should not be successfully completing an Append that followed a failed one. If somehow
                    // this happened, record it in the log.
                    log.warn(append.getRequestId(), "Acknowledged a successful append after a failed one. Segment={}, WriterId={}, FailedEventNumber={}, AppendEventNumber={}",
                            append.getSegment(), append.getWriterId(), state.getLowestFailedEventNumber(), append.getEventNumber());
                }
            } else {
                if (append.isConditional() && Exceptions.unwrap(exception) instanceof BadOffsetException) {
                    log.debug("Conditional append failed due to incorrect offset: {}, {}", append, exception.getMessage());
                    synchronized (state.getAckLock()) {
                        // Revert the state to the last known good one. This is needed because we do not close the connection
                        // for offset-conditional append failures, hence we must revert the effects of the failed append.
                        state.conditionalAppendFailed(append.getEventNumber());
                        connection.send(new ConditionalCheckFailed(append.getWriterId(), append.getEventNumber(), append.getRequestId()));
                    }
                } else {
                    // Record the exception handling into the Writer State. It will be executed  once all the current
                    // in-flight appends are completed.
                    state.appendFailed(append.getEventNumber(), () ->
                            handleException(append.getWriterId(), append.getRequestId(), append.getSegment(), append.getEventNumber(),
                                    "appending data", exception));
                }
            }

            // After every append completes, check the WriterState and trigger any error handlers that are now eligible for execution.
            executeDelayedErrorHandler(state, append.getSegment(), append.getWriterId());
        } catch (Throwable e) {
            success = false;
            handleException(append.getWriterId(), append.getEventNumber(), append.getSegment(), "handling append result", e);
        }

        if (success) {
            // Record any necessary metrics or statistics, but after we have sent the ack back and initiated the next append.
            this.statsRecorder.recordAppend(append.getSegment(), append.getDataLength(), append.getEventCount(), elapsedTimer.getElapsed());
        }
    }

    private void appendOnIndexSegment(Append append) {
        if (!isTransientSegment(append.getSegment()) && !isTransactionSegment(append.getSegment()) && isUserStreamSegment(append.getSegment())) {
            WriterState state = this.writerStates.get(Pair.of(append.getSegment(), append.getWriterId()));
            long maxAllowedEventSize = state.getEventSizeForAppend();
            indexAppendProcessor.processAppend(append.getSegment(), maxAllowedEventSize);
        }
    }

    /**
     * Inquires the {@link WriterState} for any eligible {@link WriterState.DelayedErrorHandler} that can be executed
     * right now. If so, invokes all eligible handlers synchronously. If there are no more handlers remaining after this,
     * the {@link WriterState} is unregistered, which would essentially force the client to reinvoke {@link #setupAppend}.
     *
     * @param state       The {@link WriterState} to query.
     * @param segmentName The name of the Segment for which the append failed.
     * @param writerId    The Writer Id of the Append.
     */
    private void executeDelayedErrorHandler(WriterState state, String segmentName, UUID writerId) {
        WriterState.DelayedErrorHandler h = state.fetchEligibleDelayedErrorHandler();
        if (h == null) {
            // This WriterState is healthy - nothing to do.
            return;
        }

        synchronized (state.getAckLock()) {
            try {
                // Execute all eligible delayed handlers. Note that this may be an empty list, which is OK - it means
                // the WriterState is not healthy but there's nothing yet to execute.
                h.getHandlersToExecute().forEach(Runnable::run);
            } finally {
                if (h.getHandlersRemaining() == 0) {
                    // We've executed all handlers and have none remaining. Time to clean up this WriterState and force
                    // the Client to reinitialize it via setupAppend().
                    this.writerStates.remove(Pair.of(segmentName, writerId));
                }
            }
        }
    }

    private Void handleException(UUID writerId, long requestId, String segment, String doingWhat, Throwable u) {
        return handleException(writerId, requestId, segment, -1L, doingWhat, u);
    }

    private Void handleException(UUID writerId, long requestId, String segment, long eventNumber, String doingWhat, Throwable u) {
        if (u == null) {
            IllegalStateException exception = new IllegalStateException("No exception to handle.");
            log.error(requestId, "Append processor: Error {} on segment = '{}'", doingWhat, segment, exception);
            throw exception;
        }

        u = Exceptions.unwrap(u);
        String clientReplyStackTrace = replyWithStackTraceOnError ? Throwables.getStackTraceAsString(u) : EMPTY_STACK_TRACE;

        if (u instanceof StreamSegmentExistsException) {
            log.warn(requestId, "Segment '{}' already exists and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new SegmentAlreadyExists(requestId, segment, clientReplyStackTrace));
        } else if (u instanceof StreamSegmentNotExistsException) {
            log.warn(requestId, "Segment '{}' does not exist and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new NoSuchSegment(requestId, segment, clientReplyStackTrace, -1L));
        } else if (u instanceof StreamSegmentSealedException) {
            log.info("Segment '{}' is sealed and {} cannot perform operation '{}'.", segment, writerId, doingWhat);
            connection.send(new SegmentIsSealed(requestId, segment, clientReplyStackTrace, eventNumber));
        } else if (u instanceof ContainerNotFoundException) {
            int containerId = ((ContainerNotFoundException) u).getContainerId();
            log.warn(requestId, "Wrong host. Segment '{}' (Container {}) is not owned and {} cannot perform operation '{}'.",
                    segment, containerId, writerId, doingWhat);
            connection.send(new WrongHost(requestId, segment, "", clientReplyStackTrace));
        } else if (u instanceof BadAttributeUpdateException) {
            log.warn(requestId, "Bad attribute update by {} on segment {}.", writerId, segment, u);
            connection.send(new InvalidEventNumber(writerId, requestId, clientReplyStackTrace, eventNumber));
            close();
        } else if (u instanceof TokenExpiredException) {
            log.warn(requestId, "Token expired for writer {} on segment {}.", writerId, segment, u);
            connection.send(new WireCommands.AuthTokenCheckFailed(requestId, clientReplyStackTrace,
                WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
            close();
        } else if (u instanceof TokenException) {
            log.warn(requestId, "Token check failed or writer {} on segment {}.", writerId, segment, u);
            connection.send(new WireCommands.AuthTokenCheckFailed(requestId, clientReplyStackTrace,
                    WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
        } else if (u instanceof UnsupportedOperationException) {
            log.warn(requestId, "Unsupported Operation '{}'.", doingWhat, u);
            connection.send(new OperationUnsupported(requestId, doingWhat, clientReplyStackTrace));
        } else if (u instanceof CancellationException) {
            // Cancellation exception is thrown when the Operation processor is shutting down.
            log.info("Closing connection '{}' while performing append on Segment '{}' due to {}.", connection, segment, u.toString());
            close();
        } else {
            logError(segment, u);
            close(); // Closing connection should reinitialize things, and hopefully fix the problem
        }

        return null;
    }

    private void logError(String segment, Throwable u) {
        if (u instanceof IllegalContainerStateException) {
            log.warn("Error (Segment = '{}', Operation = 'append'): {}.", segment, u.toString());
        } else {
            log.error("Error (Segment = '{}', Operation = 'append')", segment, u);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            connection.close();
            // The AppendProcessor marks the tracked Transient Segments for deletion -- but does not synchronously wait for
            // the deletion to complete. The Transient Segments will be cleaned up at the SegmentStore's discretion.
            transientSegmentNames.forEach(name -> store.deleteStreamSegment(name, TIMEOUT));
        }
    }

    @Override
    public void connectionDropped() {
        close();
    }

    //endregion
}
