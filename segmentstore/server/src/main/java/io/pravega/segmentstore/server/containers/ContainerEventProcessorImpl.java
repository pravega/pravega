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
package io.pravega.segmentstore.server.containers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.getEventProcessorSegmentName;

/**
 * Implementation for {@link ContainerEventProcessor}. This class stores a map of {@link ContainerEventProcessor.EventProcessor}
 * identified by name. Once this component is created, it will instantiate new {@link ContainerEventProcessor.EventProcessor}
 * objects. The actual processing is performed in batches by {@link ContainerEventProcessor.EventProcessor}s. Each of
 * such processors is in charge of tailing their respective internal Segments and (safely) invoking their handler
 * functions on a list of read events (of at most maxItemsAtOnce elements). Upon a successful processing iteration, an
 * {@link ContainerEventProcessor.EventProcessor} truncates the internal Segments of the registered. If an error occurs,
 * the internal Segment is not truncated and the processing is attempted again over the same events. Therefore, this
 * class provides at-least-once processing guarantees, but events could be re-processed in the case of failures while
 * truncating the processor's internal Segment. This is important to take into account when developing handler functions
 * of {@link ContainerEventProcessor.EventProcessor}s as they should be idempotent and tolerate re-processing.
 */
@Slf4j
class ContainerEventProcessorImpl implements ContainerEventProcessor {

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    // Ensure that an Event Processor's Segment does not get throttled by making it system-critical.
    private static final SegmentType SYSTEM_CRITICAL_SEGMENT = SegmentType.builder().system().internal().critical().build();

    private final int containerId;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final Map<String, CompletableFuture<EventProcessor>> eventProcessorMap = new ConcurrentHashMap<>();
    private final Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier;
    private final Duration iterationDelay;
    private final Duration containerOperationTimeout;
    private final AtomicBoolean closed;
    private final String traceObjectId;
    private final ScheduledExecutorService executor;

    //region Constructor

    ContainerEventProcessorImpl(@NonNull SegmentContainer container, @NonNull MetadataStore metadataStore,
                                @NonNull Duration iterationDelay, @NonNull Duration containerOperationTimeout,
                                @NonNull ScheduledExecutorService executor) {
        this(container.getId(), getOrCreateInternalSegment(container, metadataStore, containerOperationTimeout), iterationDelay,
                containerOperationTimeout, executor);
    }

    @VisibleForTesting
    ContainerEventProcessorImpl(int containerId, @NonNull Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier,
                                @NonNull Duration iterationDelay, @NonNull Duration containerOperationTimeout,
                                @NonNull ScheduledExecutorService executor) {
        this.containerId = containerId;
        this.traceObjectId = String.format("ContainerEventProcessor[%d]", containerId);
        this.segmentSupplier = segmentSupplier;
        this.iterationDelay = iterationDelay;
        this.containerOperationTimeout = containerOperationTimeout;
        this.closed = new AtomicBoolean(false);
        this.executor = executor;
    }

    /**
     * This function instantiates a new Segment supplier by creating actual Segment against the {@link SegmentContainer}
     * passed as input based on the name passed to the function. Note that the Segment is only created if it does not
     * exists. Otherwise, it is just loaded and returned.
     *
     * @param container {@link SegmentContainer} to create Segments from.
     * @param timeout   Timeout for the Segment creation to complete.
     * @return A future that, when completed, contains reference to the Segment to be used by a given
     * {@link ContainerEventProcessor.EventProcessor} based on its name.
     */
    private static Function<String, CompletableFuture<DirectSegmentAccess>> getOrCreateInternalSegment(SegmentContainer container,
                                                                                                       MetadataStore metadataStore,
                                                                                                       Duration timeout) {
        return s -> Futures.exceptionallyComposeExpecting(
                container.forSegment(getEventProcessorSegmentName(container.getId(), s), timeout),
                e -> e instanceof StreamSegmentNotExistsException,
                () -> metadataStore.registerPinnedSegment(getEventProcessorSegmentName(container.getId(), s),
                        SYSTEM_CRITICAL_SEGMENT, null, timeout) // Segment should be pinned.
                        .thenCompose(l -> container.forSegment(getEventProcessorSegmentName(container.getId(), s), timeout)));
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            log.info("{}: Closing ContainerEventProcessor service.", this.traceObjectId);
            closeProcessors();
        }
    }

    private void closeProcessors() {
        synchronized (eventProcessorMap) {
            if (eventProcessorMap.isEmpty()) {
                return;
            }
            eventProcessorMap.forEach((k, v) -> {
                try {
                    v.join().close();
                } catch (Exception e) {
                    log.warn("{}: Problem closing EventProcessor {}.", this.traceObjectId, k, e);
                }
            });
            eventProcessorMap.clear();
            log.debug("{}: Closing EventProcessors complete.", this.traceObjectId);
        }
    }

    //endregion

    //region ContainerEventProcessor Implementation

    @Override
    public CompletableFuture<EventProcessor> forConsumer(@NonNull String name,
                                                         @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                                                         @NonNull EventProcessorConfig config) {
        checkEventProcessorCreatePreconditions(name);

        synchronized (eventProcessorMap) {
            // If the EventProcessor is already loaded, just return it.
            CompletableFuture<EventProcessor> processorFuture = eventProcessorMap.get(name);
            if (processorFuture == null) {
                // Instantiate the EventProcessor and put it into the map. If the EventProcessor is closed, auto-unregister.
                Runnable onClose = () -> eventProcessorMap.remove(name);
                // Put the future for this EventProcessor that will be completed upon initialization. All other callers
                // will get the same result as they will be waiting for the same future object.
                processorFuture = new CompletableFuture<>();
                eventProcessorMap.put(name, processorFuture);
                createEventProcessor(name, config, onClose, handler, true, processorFuture);
            }
            return processorFuture;
        }
    }

    @Override
    public CompletableFuture<EventProcessor> forDurableQueue(@NonNull String name) {
        checkEventProcessorCreatePreconditions(name);

        synchronized (eventProcessorMap) {
            // If the EventProcessor is already loaded, just return it.
            CompletableFuture<EventProcessor> processorFuture = eventProcessorMap.get(name);
            if (processorFuture == null) {
                // Instantiate the EventProcessor and put it into the map. If the EventProcessor is closed, auto-unregister.
                Runnable onClose = () -> eventProcessorMap.remove(name);
                // No handler needed when we use the EventProcessor as a durable queue.
                Function<List<BufferView>, CompletableFuture<Void>> noHandler = l -> CompletableFuture.completedFuture(null);
                // Do not limit the amount of outstanding bytes when there is no consumer configured.
                EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(0, Long.MAX_VALUE);
                // Put the future for this EventProcessor that will be completed upon initialization. All other callers
                // will get the same result as they will be waiting for the same future object.
                processorFuture = new CompletableFuture<>();
                eventProcessorMap.put(name, processorFuture);
                createEventProcessor(name, config, onClose, noHandler, false, processorFuture);
            }
            return processorFuture;
        }
    }

    private void checkEventProcessorCreatePreconditions(String name) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(!name.isEmpty(), "EventProcessor name cannot be empty.");
    }

    private CompletableFuture<EventProcessor> createEventProcessor(String name, EventProcessorConfig config, Runnable onClose,
                                                                   Function<List<BufferView>, CompletableFuture<Void>> handler,
                                                                   boolean startService, CompletableFuture<EventProcessor> result) {
        return segmentSupplier.apply(name)
                .thenApply(segment -> {
                    Exceptions.checkNotClosed(this.closed.get(), this);
                    EventProcessor processor = new EventProcessorImpl(name, containerId, segment, handler,
                            config, iterationDelay, containerOperationTimeout, onClose, executor);
                    if (startService) {
                        ((EventProcessorImpl) processor).startAsync().awaitRunning();
                    }
                    return processor;
                })
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        log.info("{}: EventProcessor {} created successfully.", this.traceObjectId, name);
                        result.complete(r);
                    } else {
                        log.error("{}: Problem instantiating EventProcessor {}.", this.traceObjectId, name, ex);
                        result.completeExceptionally(ex);
                        // After a failed initialization, delete the entry form the map to allow new attempts.
                        eventProcessorMap.remove(name);
                    }
                });
    }

    /**
     * Each {@link EventProcessor} service has associated an internal Segment and is uniquely identified by its name
     * within a Segment Container. An {@link EventProcessor} tails its internal Segment looking for new events. When it
     * has at least 1 event to read on its Segment, it invokes its handler. If there are multiple events available, up
     * to {@link EventProcessorConfig#getMaxItemsAtOnce()} should be used as input for the handler.
     *
     * If the handler completes normally, the items will be removed from the queue (i.e., the {@link EventProcessor}'s
     * Segment will be truncated up to that offset). If the handler completes with an exception, the items will not be
     * removed and processing will be internally retried until processing succeeds.
     */
    static class EventProcessorImpl extends AbstractThreadPoolService implements EventProcessor {

        private static final ProcessorEventData.ProcessorEventDataSerializer SERIALIZER = new ProcessorEventData.ProcessorEventDataSerializer();

        private final String name;
        private final int containerId;
        private final Function<List<BufferView>, CompletableFuture<Void>> handler;
        private final EventProcessorConfig config;
        private final Duration iterationDelay;
        private final Duration containerOperationTimeout;
        private final Runnable onClose;
        private final DirectSegmentAccess segment;
        private final SegmentStoreMetrics.EventProcessor metrics;
        private final AtomicBoolean closed;
        private final AtomicBoolean failedIteration;
        private final AtomicLong segmentStartOffset;

        //region Constructor

        public EventProcessorImpl(@NonNull String name, int containerId, @NonNull DirectSegmentAccess segment,
                                  @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                                  @NonNull EventProcessorConfig config, @NonNull Duration iterationDelay,
                                  @NonNull Duration containerOperationTimeout, @NonNull Runnable onClose,
                                  @NonNull ScheduledExecutorService executor) {
            super(String.format("EventProcessor[%d-%s]", containerId, segment.getSegmentId()), executor);
            this.name = name;
            this.containerId = containerId;
            this.segment = segment;
            this.handler = handler;
            this.config = config;
            this.iterationDelay = iterationDelay;
            this.containerOperationTimeout = containerOperationTimeout;
            this.onClose = onClose;
            this.metrics = new SegmentStoreMetrics.EventProcessor(name, containerId);
            this.closed = new AtomicBoolean(false);
            this.failedIteration = new AtomicBoolean(false);
            // Set with actual value when upon initialization.
            this.segmentStartOffset = new AtomicLong(segment.getInfo().getStartOffset());
        }

        //endregion

        //region EventProcessor implementation

        @Override
        @SneakyThrows(IOException.class)
        public CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout) throws TooManyOutstandingBytesException {
            Preconditions.checkArgument(event.getLength() > 0);
            Preconditions.checkArgument(event.getLength() + Integer.BYTES < ProcessorEventData.MAX_EVENT_SIZE);
            Exceptions.checkNotClosed(this.closed.get(), this);
            // If the EventProcessor reached the limit of outstanding bytes, throw accordingly.
            if (getOutstandingBytes() > this.config.getMaxProcessorOutstandingBytes()) {
                throw new TooManyOutstandingBytesException(this.traceObjectId);
            }

            ProcessorEventData processorEvent = ProcessorEventData.builder().data(event).build();
            return this.segment.append(SERIALIZER.serialize(processorEvent), null, timeout)
                          .thenApply(offset -> getOutstandingBytes());
        }

        /**
         * Gets the number of outstanding bytes in the {@link ContainerEventProcessor.EventProcessor}'s internal Segment.
         * 
         * @return Outstanding bytes in the {@link ContainerEventProcessor.EventProcessor}'s internal Segment.
         */
        @VisibleForTesting
        long getOutstandingBytes() {
            SegmentMetadata segmentMetadata = this.segment.getInfo();
            return segmentMetadata.getLength() - segmentMetadata.getStartOffset();
        }

        //endregion

        //region AutoCloseable implementation

        /**
         * This method stop the service (superclass), auto-unregisters from the existing set of active
         * {@link EventProcessor} instances (via onClose callback), and closes the metrics.
         */
        @Override
        public void close() {
            if (!this.closed.getAndSet(true)) {
                log.info("{}: Closing EventProcessor.", this.traceObjectId);
                super.close();
                this.metrics.close();
                this.onClose.run();
            }
        }

        //endregion

        //region Runnable implementation

        @Override
        protected Duration getShutdownTimeout() {
            return SHUTDOWN_TIMEOUT;
        }

        @Override
        public CompletableFuture<Void> doRun() {
            // An EventProcessor iteration is made of the following stages:
            // 1. Async read of data available in the internal Segment (up to 2 * ProcessorEventSerializer.MAX_TOTAL_EVENT_SIZE
            // bytes). Also, report the current outstanding bytes in the internal Segment.
            // 2. Deserialize the read data up to exhaust it or get getMaxItemsAtOnce() events.
            // 3. The collected results are passed to the handler function in EventProcessor for execution.
            // 4. If the handler function has been successfully executed, truncate the internal Segment of the EventProcessor
            // according to the last successfully processed event offset. If an error occurs, throw and re-try.
            Exceptions.checkNotClosed(this.closed.get(), this);
            log.info("{} Starting processing.", this.traceObjectId);
            return Futures.loop(
                       () -> !this.closed.get(),
                       () -> getDelayedFutureIfNeeded().thenComposeAsync(v -> processEvents(), this.executor),
                   this.executor)
                       .handle((r, ex) -> {
                           if (ex != null) {
                               log.warn("{}: Terminated due to unexpected exception.", this.traceObjectId, ex);
                           } else {
                               log.info("{}: Terminated.", this.traceObjectId);
                           }
                           return null;
                       });
        }

        /**
         * Gets a delayed future when either i) an error has been experienced in the last iteration, or ii) if there is
         * no outstanding data to process. Otherwise, we do not delay the next iteration and return a completed future.
         *
         * @return A future that may delay the next processing iteration if there has been an error in the last
         * processing iteration or there is no outstanding data to process.
         */
        private CompletableFuture<Void> getDelayedFutureIfNeeded() {
            return this.failedIteration.get() || getOutstandingBytes() == 0 ? Futures.delayedFuture(iterationDelay, this.executor) :
                    Futures.delayedFuture(Duration.ZERO, this.executor);
        }

        /**
         * Returns a {@link CompletableFuture} that results from the execution of the following tasks for each
         * {@link ContainerEventProcessor.EventProcessor}: i) Read available data in the Segment, ii) deserialize at most
         * {@link ContainerEventProcessor.EventProcessorConfig#getMaxItemsAtOnce()} events, iii) execute the handler
         * function for the {@link ContainerEventProcessor.EventProcessor} on the event list, and iv) truncate the
         * internal Segment.
         */
        private CompletableFuture<Void> processEvents() {
            final Timer iterationTime = new Timer();
            return readEvents()
                    .thenComposeAsync(this::applyProcessorHandler, this.executor)
                    .thenComposeAsync(readResult -> truncateInternalSegment(readResult, iterationTime), this.executor)
                    .handleAsync((r, ex) -> {
                        // If we got an exception different from NoDataAvailableException, report it as something is off.
                        if (ex != null && !(Exceptions.unwrap(ex) instanceof NoDataAvailableException)) {
                            log.warn("{}: Processing iteration failed, retrying.", this.traceObjectId, ex);
                            this.failedIteration.set(true);
                            reconcileStartOffset();
                        } else {
                            this.failedIteration.set(false);
                        }
                        log.debug("{}: Finished iteration for EventProcessor (Name = {}, Start offset = {}, Failed iteration = {}).",
                                this.traceObjectId, this.name, this.segmentStartOffset.get(), this.failedIteration.get());
                        return null;
                    });
        }

        /**
         * In case of a non-expected exception, we reset the segmentStartOffset value to the real one available in the
         * Segment's metadata.
         */
        private void reconcileStartOffset() {
            long newStartOffset = segment.getInfo().getStartOffset();
            log.info("{}: Reconciling start offset from {} to {}.", this.traceObjectId, this.segmentStartOffset.get(), newStartOffset);
            this.segmentStartOffset.set(newStartOffset);
        }

        /**
         * Attempts to read data from the internal Segment. The size of the read length would be the minimum between the
         * outstanding bytes and 2 * ProcessorEventData.MAX_TOTAL_EVENT_SIZE bytes.
         *
         * @return A {@link CompletableFuture} that, when completed, will contain a {@link EventsReadAndTruncationLength}
         * object with events read and the next truncation length for the internal Segment.
         */
        private CompletableFuture<EventsReadAndTruncationLength> readEvents() {
            return CompletableFuture.supplyAsync(() -> {
                                        // Report current outstanding bytes and read the available data.
                                        long outStandingBytes = getOutstandingBytes();
                                        SegmentStoreMetrics.outstandingEventProcessorBytes(this.name, containerId, outStandingBytes);
                                        int readLength = (int) Math.min(outStandingBytes, 2 * ProcessorEventData.MAX_EVENT_SIZE);
                                        return this.segment.read(this.segmentStartOffset.get(), readLength, this.containerOperationTimeout);
                                    }, this.executor)
                                    .thenCompose(rr -> AsyncReadResultProcessor.processAll(rr, this.executor, this.containerOperationTimeout))
                                    .thenApply(this::deserializeEvents);
        }

        @SneakyThrows(Exception.class)
        private EventsReadAndTruncationLength deserializeEvents(BufferView inputData) {
            List<ProcessorEventData> events = new ArrayList<>();
            long truncationLength = 0;
            Exception deserializationException = null;
            int dataLength = inputData.getLength();
            try {
                @Cleanup
                BoundedInputStream input = new BoundedInputStream(inputData.getReader(), inputData.getLength());
                while (input.getRemaining() > 0 && events.size() < this.config.getMaxItemsAtOnce()) {
                    ProcessorEventData event = SERIALIZER.deserialize(input);
                    events.add(event);
                    // Update the truncation length, which includes user data and internal VersionedSerializer metadata.
                    truncationLength = dataLength - input.getRemaining();
                }
            } catch (BufferView.Reader.OutOfBoundsException ex) {
                // Events are of arbitrary size, so it is quite possible we stopped reading in the middle of an event.
                deserializationException = new NoDataAvailableException();
            } catch (Exception ex) {
                // Some unexpected serialization error occurred here; rethrow if this prevents reading further events.
                log.error("{}: Error deserializing events (SegmentId = {}, Initial offset = {}, Read length = {}, Truncation length = {}).",
                        this.traceObjectId, this.segment.getSegmentId(), this.segmentStartOffset.get(), dataLength, truncationLength, ex);
                deserializationException = ex;
            }

            // If there are some valid events read, do not interrupt the current processing iteration and process them.
            // Note that if the error is persistent, the next iteration will hit the same exception and no events will
            // be read. In that case, we will throw and interrupt the current processing iteration.
            if (events.isEmpty()) {
                throw deserializationException != null ? deserializationException : new NoDataAvailableException();
            }
            return new EventsReadAndTruncationLength(events, truncationLength);
        }

        private CompletableFuture<EventsReadAndTruncationLength> applyProcessorHandler(EventsReadAndTruncationLength readResult) {
            return this.handler.apply(readResult.getProcessorEventsData()).thenApply(v -> readResult);
        }

        private CompletableFuture<Void> truncateInternalSegment(EventsReadAndTruncationLength readResult, Timer iterationTime) {
            long truncationOffset = this.segmentStartOffset.get() + readResult.getTruncationLength();
            return this.segment.truncate(truncationOffset, this.containerOperationTimeout)
                               .thenAccept(v -> {
                                   // Update the startSegmentOffset after truncation.
                                   this.segmentStartOffset.addAndGet(readResult.getTruncationLength());
                                   // Report latency metrics upon complete processing iteration.
                                   this.metrics.batchProcessingLatency(iterationTime.getElapsedMillis());
                               });
        }

        /**
         * Utility class to group all events reads and provide some convenience methods to operate on them (e.g., bytes
         * read accounting for headers).
         */
        @Data
        static class EventsReadAndTruncationLength {
            private final List<ProcessorEventData> eventsRead;
            private final long truncationLength;

            public List<BufferView> getProcessorEventsData() {
                return this.eventsRead.stream().map(ProcessorEventData::getData).collect(Collectors.toUnmodifiableList());
            }
        }

        static class NoDataAvailableException extends RuntimeException {
        }
    }

    /**
     * Representation of an event written to an {@link ContainerEventProcessor.EventProcessor}.
     * It consists of:
     * - Data (BufferView of length at most 1024 * 1024 bytes)
     */
    @Data
    @Builder
    static class ProcessorEventData {
        // Set a maximum length to individual events to be processed by EventProcessor (1MB).
        public static final int MAX_EVENT_SIZE = 1024 * 1024;
        private static final ProcessorEventDataSerializer SERIALIZER = new ProcessorEventDataSerializer();

        private final BufferView data;

        static class ProcessorEventDataBuilder implements ObjectBuilder<ProcessorEventData> {
        }

        /**
         * Helper class to serialize/deserialize {@link ProcessorEventData} objects.
         */
        static class ProcessorEventDataSerializer extends VersionedSerializer.WithBuilder<ProcessorEventData,
                ProcessorEventData.ProcessorEventDataBuilder> {

            @Override
            protected ProcessorEventData.ProcessorEventDataBuilder newBuilder() {
                return ProcessorEventData.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ProcessorEventData d, RevisionDataOutput output) throws IOException {
                output.writeBuffer(d.getData());
            }

            private void read00(RevisionDataInput input, ProcessorEventData.ProcessorEventDataBuilder builder) throws IOException {
                builder.data(new ByteArraySegment(input.readArray()));
            }
        }
    }

    //endregion

}