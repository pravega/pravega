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
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.segmentstore.server.ContainerEventProcessor.ProcessorEventData.HEADER_LENGTH;
import static io.pravega.segmentstore.server.ContainerEventProcessor.ProcessorEventData.MAX_TOTAL_EVENT_SIZE;
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

    ContainerEventProcessorImpl(@NonNull SegmentContainer container, @NonNull Duration iterationDelay,
                                @NonNull Duration containerOperationTimeout, @NonNull ScheduledExecutorService executor) {
        this(container.getId(), getOrCreateInternalSegment(container, containerOperationTimeout), iterationDelay,
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
                                                                                                       Duration timeout) {
        return s -> Futures.exceptionallyComposeExpecting(
                container.forSegment(getEventProcessorSegmentName(container.getId(), s), timeout),
                e -> e instanceof StreamSegmentNotExistsException,
                () -> container.createStreamSegment(getEventProcessorSegmentName(container.getId(), s), SYSTEM_CRITICAL_SEGMENT, null, timeout)
                        .thenCompose(v -> container.forSegment(getEventProcessorSegmentName(container.getId(), s), timeout)));
    }

    /**
     * Reports the metrics periodically for each EventProcessor.
     */
    private void reportMetrics() {
        for (EventProcessorImpl ep : eventProcessorMap.values()) {
            SegmentStoreMetrics.outstandingEventProcessorBytes(ep.getName(), containerId, ep.outstandingBytes.get());
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
                        eventProcessorMap.put(name, eventProcessor);
                        eventProcessor.run(); // Start the processing for this EventProcessor and return it.
                        return eventProcessor;
                    });
        }
    }

    private class EventProcessorImpl extends ContainerEventProcessor.EventProcessor implements Runnable {

        private final DirectSegmentAccess segment;
        private final AtomicLong outstandingBytes;
        private final AtomicLong lastIterationLatency;
        private final SegmentStoreMetrics.EventProcessor metrics;
        private final AtomicBoolean closed;
        private final AtomicBoolean failedIteration;
        private final String traceObjectId;

        //region Constructor

        public EventProcessorImpl(@NonNull String name, @NonNull DirectSegmentAccess segment,
                                  @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                                  @NonNull EventProcessorConfig config, @NonNull Runnable onClose) {
            super(name, handler, config, onClose);
            this.traceObjectId = String.format("EventProcessor[%d-%s]", containerId, name);
            this.segment = segment;
            this.outstandingBytes = new AtomicLong(getInitialOutstandingBytes(segment)); // Initialize with current outstanding data.
            this.lastIterationLatency = new AtomicLong(0);
            this.metrics = new SegmentStoreMetrics.EventProcessor(name, containerId);
            this.closed = new AtomicBoolean(false);
            this.failedIteration = new AtomicBoolean(false);
        }

        /**
         * Upon recovery, set the actual number of outstanding bytes in the EventProcessor.
         *
         * @param segment Internal Segment for this EventProcessor.
         * @return Number of outstanding bytes for this EventProcessor (i.e., Segment length).
         */
        private long getInitialOutstandingBytes(DirectSegmentAccess segment) {
            try {
                return segment.getInfo().getLength();
            } catch (Exception ex) {
                log.warn("{}: Unable to get initial outstanding bytes from internal Segment.", this.traceObjectId, ex);
            }
            return 0;
        }

        //endregion

        //region EventProcessor implementation

        @Override
        public CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout) {
            Preconditions.checkArgument(event.getLength() > 0);
            Preconditions.checkArgument(event.getLength() + HEADER_LENGTH < MAX_TOTAL_EVENT_SIZE);
            Exceptions.checkNotClosed(this.closed.get(), this);
            Preconditions.checkState(outstandingBytes.get() < getConfig().getMaxProcessorOutstandingBytes(),
                    "Too many outstanding events for {}.", this.getName());

            return segment.append(ProcessorEventSerializer.serializeEvent(event), null, timeout)
                          .thenApply(offset -> outstandingBytes.addAndGet(event.getLength() + HEADER_LENGTH));
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
            return failedIteration.get() || outstandingBytes.get() == 0 ? Futures.delayedFuture(iterationDelay, executor) :
                    Futures.delayedFuture(Duration.ZERO, executor);
        }

        /**
         * Returns a {@link CompletableFuture} that results from the execution of the following tasks for each EventProcessor:
         * i) Read available data in the Segment, ii) deserialize at most {@link EventProcessorConfig#getMaxItemsAtOnce()},
         * iii) execute the handler function for the {@link EventProcessor}, and iv) truncate the internal Segment.
         */
        private CompletableFuture<Void> processEvents() {
            final Timer iterationTime = new Timer();
            return readEvents()
                    .thenApplyAsync(this::applyProcessorHandler, executor)
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

        private CompletableFuture<EventsReadAndTruncationPoints> readEvents() {
            AtomicLong startOffset = new AtomicLong(0L);
            AtomicInteger readLength = new AtomicInteger(0);
            return CompletableFuture.supplyAsync(() -> {
                                        // Need to get the start and end offsets to properly create async read request.
                                        startOffset.set(segment.getInfo().getStartOffset());
                                        readLength.set((int) Math.min(segment.getInfo().getLength() - startOffset.get(), MAX_TOTAL_EVENT_SIZE));
                                        return segment.read(startOffset.get(), readLength.get(), containerOperationTimeout);
                                    }, executor)
                                    .thenComposeAsync(rr -> AsyncReadResultProcessor.processAll(rr, executor, containerOperationTimeout), executor)
                                    .thenApply(inputData -> deserializeEvents(inputData, startOffset.get(), readLength.get()));
        }

        private EventsReadAndTruncationPoints deserializeEvents(BufferView inputData, long startOffset, int maxLength) {
            List<ProcessorEventData> events = new ArrayList<>();
            long nextOffset = startOffset;
            final long maxOffset = startOffset + maxLength;
            BufferView.Reader input = inputData.getBufferViewReader();
            try {
                while (nextOffset < maxOffset && events.size() < getConfig().getMaxItemsAtOnce()) {
                    ProcessorEventData event = ProcessorEventSerializer.deserializeEvent(input);
                    events.add(event);
                    // Update the offset to the beginning of the next event.
                    nextOffset += HEADER_LENGTH + event.getLength();
                }
            } catch (BufferView.Reader.OutOfBoundsException ex) {
                // Events are of arbitrary size, so it is quite possible we stopped reading in the middle of an event.
                // Only if we have already read some data, we do not throw.
            }

            if (events.isEmpty()) {
                throw new NoDataAvailableException();
            }
            return new EventsReadAndTruncationPoints(events);
        }

        private EventsReadAndTruncationPoints applyProcessorHandler(EventsReadAndTruncationPoints readResult) {
            getHandler().apply(readResult.getProcessorEventsData());
            return readResult;
        }

        private CompletableFuture<Void> truncateInternalSegment(EventsReadAndTruncationPoints readResult, Timer iterationTime) {
            long truncationOffset = this.segment.getInfo().getStartOffset() + readResult.getReadBytes();
            return this.segment.truncate(truncationOffset, containerOperationTimeout)
                    .thenAccept(v -> {
                        // Decrement outstanding bytes and publish the last latency value after successful truncation.
                        this.outstandingBytes.addAndGet(-readResult.getReadBytes());
                        this.lastIterationLatency.set(iterationTime.getElapsedMillis());
                    });
        }
    }

    @Data
    private static class EventsReadAndTruncationPoints {
        private final List<ProcessorEventData> eventsRead;

        public long getReadBytes() {
            return eventsRead.stream().map(d -> d.getLength() + HEADER_LENGTH).reduce(0, Integer::sum);
        }

        public List<BufferView> getProcessorEventsData() {
            return eventsRead.stream().map(ProcessorEventData::getData).collect(Collectors.toUnmodifiableList());
        }
    }

    private static class NoDataAvailableException extends RuntimeException {
    }

    /**
     * Helper class to serialize/deserialize {@link ProcessorEventData} objects.
     */
    static class ProcessorEventSerializer {

        private static final byte CURRENT_SERIALIZATION_VERSION = 0;
        private static final int VERSION_POSITION = 0;
        private static final int EVENT_LENGTH_POSITION = VERSION_POSITION + 1;

        static BufferView serializeHeader(int eventLength) {
            ByteArraySegment data = new ByteArraySegment(new byte[HEADER_LENGTH]);
            data.set(VERSION_POSITION, CURRENT_SERIALIZATION_VERSION);
            data.setInt(EVENT_LENGTH_POSITION, eventLength);
            return data;
        }

        static BufferView serializeEvent(BufferView eventData) {
            return BufferView.builder().add(serializeHeader(eventData.getLength())).add(eventData).build();
        }

        static ProcessorEventData deserializeEvent(BufferView.Reader inputData) {
            byte version = inputData.readByte();
            int length = inputData.readInt();
            return new ProcessorEventData(version, length, inputData.readSlice(length));
        }
    }

    //endregion

}