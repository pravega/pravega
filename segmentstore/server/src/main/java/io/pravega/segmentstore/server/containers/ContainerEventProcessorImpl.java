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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.ContainerEventProcessor.TooManyOutstandingBytesException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.segmentstore.server.containers.MetadataStore.SegmentInfo.newSegment;
import static io.pravega.shared.NameUtils.getEventProcessorSegmentName;

/**
 * Implementation for {@link ContainerEventProcessor}. This class stores a map of {@link ContainerEventProcessor.EventProcessor}
 * identified by name. Once this component is created, it will instantiate new {@link ContainerEventProcessor.EventProcessor}
 * objects and report metrics for the existing ones. The actual processing is performed in batches (every
 * iterationDelay ms) by {@link ContainerEventProcessor.EventProcessor}s. Each of such processors is in charge of
 * tailing their respective internal Segments and (safely) invoking their handler functions on a list of read events
 * (of at most maxItemsAtOnce elements). Upon a successful processing iteration, an {@link ContainerEventProcessor.EventProcessor}
 * truncates the internal Segments of the registered. If some error occurs, the internal Segment is not truncated and
 * the processing is attempted again over the same events. Therefore, this class provides at-least-once processing
 * guarantees, but events could be re-processed in the case of failures while truncating the processor's internal Segment.
 * This is important to take into account when developing handler functions of {@link ContainerEventProcessor.EventProcessor}s
 * as they should be idempotent and tolerate re-processing.
 */
@Slf4j
class ContainerEventProcessorImpl implements ContainerEventProcessor {

    // Ensure that an Event Processor's Segment does not get throttled by making it system-critical.
    private final static SegmentType SYSTEM_CRITICAL_SEGMENT = SegmentType.builder().system().internal().critical().build();

    private final int containerId;
    private final Map<String, EventProcessorImpl> eventProcessorMap = new ConcurrentHashMap<>();
    private final Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier;
    private final Duration iterationDelay;
    private final Duration containerOperationTimeout;
    private final AtomicBoolean closed;
    private final String traceObjectId;
    private final ScheduledFuture<?> eventProcessorMetricsReporting;
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
        // This class just reports the metrics for all the registered EventProcessor objects.
        this.eventProcessorMetricsReporting = executor.scheduleAtFixedRate(this::reportMetrics, iterationDelay.toMillis(),
                iterationDelay.toMillis(), TimeUnit.MILLISECONDS);
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
                () -> metadataStore.submitAssignment(newSegment(getEventProcessorSegmentName(container.getId(), s),
                        SYSTEM_CRITICAL_SEGMENT, Collections.emptyList()), true, timeout) // Segment should be pinned.
                        .thenCompose(l -> container.forSegment(getEventProcessorSegmentName(container.getId(), s), timeout)));
    }

    /**
     * Reports the metrics periodically for each {@link ContainerEventProcessor.EventProcessor}.
     */
    private void reportMetrics() {
        for (EventProcessorImpl ep : eventProcessorMap.values()) {
            SegmentStoreMetrics.outstandingEventProcessorBytes(ep.getName(), containerId, ep.getOutstandingBytes());
            // Only report the last iteration processing latency if it has been successful.
            if (!ep.failedIteration.get()) {
                ep.metrics.batchProcessingLatency(ep.lastIterationLatency.get());
            }
        }
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            log.info("{}: Closing ContainerEventProcessor service.", this.traceObjectId);
            closeProcessors();
            eventProcessorMetricsReporting.cancel(true);
        }
    }

    private void closeProcessors() {
        synchronized (eventProcessorMap) {
            if (eventProcessorMap.isEmpty()) {
                return;
            }
            eventProcessorMap.forEach((k, v) -> {
                try {
                    v.close();
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
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(!name.isEmpty(), "EventProcessor name cannot be empty.");

        synchronized (eventProcessorMap) {
            // If the EventProcessor is already loaded, just return it.
            if (eventProcessorMap.containsKey(name)) {
                return CompletableFuture.completedFuture(eventProcessorMap.get(name));
            }

            // Instantiate the EventProcessor and put it into the map. If the EventProcessor is closed, auto-unregister.
            Runnable onClose = () -> eventProcessorMap.remove(name);
            return segmentSupplier.apply(name)
                    .thenApply(segment -> {
                        Exceptions.checkNotClosed(this.closed.get(), this);
                        EventProcessorImpl eventProcessor = new EventProcessorImpl(name, segment, handler, config, onClose);
                        eventProcessor.run(); // Start the processing for this EventProcessor and return it.
                        eventProcessorMap.put(name, eventProcessor);
                        return eventProcessor;
                    });
        }
    }

    @Override
    public CompletableFuture<EventProcessor> forDurableQueue(@NonNull String name) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(!name.isEmpty(), "EventProcessor name cannot be empty.");

        // Do not limit the amount of outstanding bytes when there is no consumer configured.
        EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(0, Long.MAX_VALUE);
        synchronized (eventProcessorMap) {
            // If the EventProcessor is already loaded, just return it.
            if (eventProcessorMap.containsKey(name)) {
                return CompletableFuture.completedFuture(eventProcessorMap.get(name));
            }

            // Instantiate the EventProcessor and put it into the map. If the EventProcessor is closed, auto-unregister.
            Runnable onClose = () -> eventProcessorMap.remove(name);
            Function<List<BufferView>, CompletableFuture<Void>> noHandler = l -> CompletableFuture.completedFuture(null);
            return segmentSupplier.apply(name)
                    .thenApply(segment -> {
                        Exceptions.checkNotClosed(this.closed.get(), this);
                        EventProcessorImpl eventProcessor = new EventProcessorImpl(name, segment, noHandler, config, onClose);
                        eventProcessorMap.put(name, eventProcessor);
                        return eventProcessor;
                    });
        }
    }

    class EventProcessorImpl extends ContainerEventProcessor.EventProcessor implements Runnable {

        private final DirectSegmentAccess segment;
        private final AtomicLong lastIterationLatency;
        private final SegmentStoreMetrics.EventProcessor metrics;
        private final AtomicBoolean closed;
        private final AtomicBoolean failedIteration;
        private final String traceObjectId;

        private final ProcessorEventData.ProcessorEventDataSerializer serializer = new ProcessorEventData.ProcessorEventDataSerializer();

        //region Constructor

        public EventProcessorImpl(@NonNull String name, @NonNull DirectSegmentAccess segment,
                                  @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                                  @NonNull EventProcessorConfig config, @NonNull Runnable onClose) {
            super(name, handler, config, onClose);
            this.traceObjectId = String.format("EventProcessor[%d-%s]", containerId, name);
            this.segment = segment;
            this.lastIterationLatency = new AtomicLong(0);
            this.metrics = new SegmentStoreMetrics.EventProcessor(name, containerId);
            this.closed = new AtomicBoolean(false);
            this.failedIteration = new AtomicBoolean(false);
        }

        //endregion

        //region EventProcessor implementation

        @Override
        @SneakyThrows(IOException.class)
        public CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout) throws TooManyOutstandingBytesException {
            Preconditions.checkArgument(event.getLength() > 0);
            Preconditions.checkArgument(event.getLength() + Integer.BYTES < ProcessorEventData.MAX_TOTAL_EVENT_SIZE);
            Exceptions.checkNotClosed(this.closed.get(), this);
            // If the EventProcessor reached the limit of outstanding bytes, throw accordingly.
            if (getOutstandingBytes() > getConfig().getMaxProcessorOutstandingBytes()) {
                throw new TooManyOutstandingBytesException(this.traceObjectId);
            }

            ProcessorEventData processorEvent = ProcessorEventData.builder().data(event).build();
            return segment.append(serializer.serialize(processorEvent), null, timeout)
                          .thenApply(offset -> getOutstandingBytes());
        }

        /**
         * Gets the number of outstanding bytes in the {@link ContainerEventProcessor.EventProcessor}'s internal Segment.
         * 
         * @return Outstanding bytes in the {@link ContainerEventProcessor.EventProcessor}'s internal Segment.
         */
        @VisibleForTesting
        long getOutstandingBytes() {
            return segment.getInfo().getLength() - segment.getInfo().getStartOffset();
        }

        //endregion

        //region AutoCloseable implementation

        @Override
        public void close() throws Exception {
            if (!closed.getAndSet(true)) {
                log.info("{}: Closing EventProcessor.", this.traceObjectId);
                super.close();
                metrics.close();
            }
        }

        //endregion

        //region Runnable implementation

        @Override
        public void run() {
            // An EventProcessor iteration is made of the following stages:
            // 1. Async read of data available in the internal Segment (up to ProcessorEventSerializer.MAX_TOTAL_EVENT_SIZE
            // bytes).
            // 2. Deserialize the read data up to exhaust it or get getMaxItemsAtOnce() events.
            // 3. The collected results are passed to the handler function in EventProcessor for execution.
            // 4. If the handler function has been successfully executed, truncate the internal Segment of the EventProcessor
            // according to the last successfully processed event offset. If an error occurs, throw and re-try.
            Exceptions.checkNotClosed(this.closed.get(), this);
            Futures.loop(
                       () -> !closed.get(),
                       () -> getDelayedFutureIfNeeded().thenComposeAsync(v -> processEvents(), executor),
                   executor)
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
            return failedIteration.get() || getOutstandingBytes() == 0 ? Futures.delayedFuture(iterationDelay, executor) :
                    Futures.delayedFuture(Duration.ZERO, executor);
        }

        /**
         * Returns a {@link CompletableFuture} that results from the execution of the following tasks for each
         * {@link ContainerEventProcessor.EventProcessor}: i) Read available data in the Segment, ii) deserialize at most
         * {@link ContainerEventProcessor.EventProcessorConfig#getMaxItemsAtOnce()}, iii) execute the handler function
         * for the {@link ContainerEventProcessor.EventProcessor}, and iv) truncate the internal Segment.
         */
        private CompletableFuture<Void> processEvents() {
            final Timer iterationTime = new Timer();
            return readEvents()
                    .thenComposeAsync(this::applyProcessorHandler, executor)
                    .thenComposeAsync(readResult -> truncateInternalSegment(readResult, iterationTime), executor)
                    .handleAsync((r, ex) -> {
                        // If we got an exception different from NoDataAvailableException, report it as something is off.
                        if (ex != null && !(Exceptions.unwrap(ex) instanceof NoDataAvailableException)) {
                            log.warn("{}: Processing iteration failed, retrying.", this.traceObjectId, ex);
                            failedIteration.set(true);
                        } else {
                            failedIteration.set(false);
                        }
                        return null;
                    });
        }

        /**
         * Attempts to read data from the internal Segment. The size of the read length would be the minimum between the
         * outstanding bytes or twice the size of the ProcessorEventData.MAX_TOTAL_EVENT_SIZE.
         *
         * @return A {@link CompletableFuture} that, when completed, will contain a {@link EventsReadAndTruncationLength}
         * object with events read and the next truncation length for the internal Segment.
         */
        private CompletableFuture<EventsReadAndTruncationLength> readEvents() {
            return CompletableFuture.supplyAsync(() -> {
                                        int readLength = (int) Math.min(getOutstandingBytes(), 2 * ProcessorEventData.MAX_TOTAL_EVENT_SIZE);
                                        return segment.read(segment.getInfo().getStartOffset(), readLength, containerOperationTimeout);
                                    }, executor)
                                    .thenCompose(rr -> AsyncReadResultProcessor.processAll(rr, executor, containerOperationTimeout))
                                    .thenApply(this::deserializeEvents);
        }

        private EventsReadAndTruncationLength deserializeEvents(BufferView inputData) {
            List<ProcessorEventData> events = new ArrayList<>();
            long truncationLength = 0;
            try {
                // Read all the data available at this point in the BufferView and use that as a working set. Otherwise,
                // the contents of BufferView could dynamically change making the truncationLength calculation unsafe.
                InputStream input = new ByteArrayInputStream(inputData.getReader().readAllBytes());
                int availableBytes = input.available();
                while (input.available() > 0 && events.size() < getConfig().getMaxItemsAtOnce()) {
                    ProcessorEventData event = serializer.deserialize(input);
                    events.add(event);
                    // Update the truncation length, which includes user data and internal VersionedSerializer metadata.
                    truncationLength = availableBytes - input.available();
                }
            } catch (BufferView.Reader.OutOfBoundsException ex) {
                // Events are of arbitrary size, so it is quite possible we stopped reading in the middle of an event.
                // Only if we have already read some data, we do not throw.
            } catch (IOException ex) {
                // Some unexpected serialization error occurred here, rethrow.
                log.error("{}: Problem while deserializing events.", this.traceObjectId);
                throw new CompletionException(ex);
            }

            if (events.isEmpty()) {
                throw new NoDataAvailableException();
            }
            return new EventsReadAndTruncationLength(events, truncationLength);
        }

        private CompletableFuture<EventsReadAndTruncationLength> applyProcessorHandler(EventsReadAndTruncationLength readResult) {
            return getHandler().apply(readResult.getProcessorEventsData()).thenApply(v -> readResult);
        }

        private CompletableFuture<Void> truncateInternalSegment(EventsReadAndTruncationLength readResult, Timer iterationTime) {
            long truncationOffset = this.segment.getInfo().getStartOffset() + readResult.getTruncationLength();
            return this.segment.truncate(truncationOffset, containerOperationTimeout)
                               .thenAccept(v -> this.lastIterationLatency.set(iterationTime.getElapsedMillis()));
        }

        /**
         * Utility class to group all events reads and provide some convenience methods to operate on them (e.g., bytes
         * read accounting for headers).
         */
        @Data
        class EventsReadAndTruncationLength {
            private final List<ProcessorEventData> eventsRead;
            private final long truncationLength;

            public List<BufferView> getProcessorEventsData() {
                return eventsRead.stream().map(ProcessorEventData::getData).collect(Collectors.toUnmodifiableList());
            }
        }

        class NoDataAvailableException extends RuntimeException {
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
        public static final int MAX_TOTAL_EVENT_SIZE = 1024 * 1024;
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