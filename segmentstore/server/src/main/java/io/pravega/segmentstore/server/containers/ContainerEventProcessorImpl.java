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
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.getEventProcessorSegmentName;

/**
 * Implementation for {@link ContainerEventProcessor}. This class stores a map of {@link ContainerEventProcessor.EventProcessor}
 * identified by name. Once this service is started, it will continuously perform processing iterations tailing the
 * internal Segments of the registered {@link ContainerEventProcessor.EventProcessor}s (every ITERATION_SLEEP ms) and
 * (safely) invoking their respective handler functions. This class durably stores the point at which the last successful
 * events has been processed in the form of an attribute in the internal Segment (i.e., LAST_PROCESSED_EVENT_OFFSET).
 * That is, before reading new events this attribute is read and after a successful processing of events this attribute
 * is updated with the value of the next position to read. This class also truncates the internal Segments of the registered
 * {@link ContainerEventProcessor.EventProcessor} based on that attribute.
 */
@Slf4j
final class ContainerEventProcessorImpl extends AbstractThreadPoolService implements ContainerEventProcessor {

    private final static Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private final int containerId;
    private final Map<String, EventProcessorImpl> eventProcessorMap = new ConcurrentHashMap<>();
    private final Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier;
    private final Duration iterationDelay;
    private final Duration containerOperationTimeout;
    private final AtomicBoolean closed;

    //region Constructor

    ContainerEventProcessorImpl(@NonNull SegmentContainer container, @NonNull Duration iterationDelay,
                                @NonNull Duration containerOperationTimeout, @NonNull ScheduledExecutorService executor) {
        super(String.format("ContainerEventProcessor[%d]", container.getId()), executor);

        this.containerId = container.getId();
        this.segmentSupplier = getOrCreateContainerProcessorSegment(container, containerOperationTimeout);
        this.iterationDelay = iterationDelay;
        this.containerOperationTimeout = containerOperationTimeout;
        this.closed = new AtomicBoolean(false);
    }

    @VisibleForTesting
    ContainerEventProcessorImpl(int containerId, @NonNull Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier,
                                @NonNull Duration iterationDelay, @NonNull Duration containerOperationTimeout,
                                @NonNull ScheduledExecutorService executor) {
        super(String.format("ContainerEventProcessor[%d]", containerId), executor);

        this.containerId = containerId;
        this.segmentSupplier = segmentSupplier;
        this.iterationDelay = iterationDelay;
        this.containerOperationTimeout = containerOperationTimeout;
        this.closed = new AtomicBoolean(false);
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
    private Function<String, CompletableFuture<DirectSegmentAccess>> getOrCreateContainerProcessorSegment(SegmentContainer container,
                                                                                                          Duration timeout) {
        // Make sure that this Segment does not get throttled by making it a system-critical one.
        final SegmentType systemCritical = SegmentType.builder().system().internal().critical().build();
        return s -> Futures.exceptionallyComposeExpecting(
                container.forSegment(getEventProcessorSegmentName(container.getId(), s), timeout),
                e -> e instanceof StreamSegmentNotExistsException,
                () -> container.createStreamSegment(getEventProcessorSegmentName(container.getId(), s), systemCritical, null, timeout)
                        .thenCompose(v -> container.forSegment(getEventProcessorSegmentName(container.getId(), s), timeout)));
    }

    //endregion

    //region AbstractThreadPoolService Implementation

    @Override
    protected Duration getShutdownTimeout() {
        return SHUTDOWN_TIMEOUT;
    }

    private void closeProcessors() {
        eventProcessorMap.forEach((k, v) -> {
            try {
                v.close();
            } catch (Exception e) {
                log.warn("{}: Problem closing EventProcessor {}.", this.traceObjectId, k, e);
            }
        });
        eventProcessorMap.clear();
        log.info("{}: Shut down of ContainerEventProcessor service complete.", this.traceObjectId);
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        // A ContainerEventProcessor iteration is made of the following stages (for each EventProcessor registered):
        // 1. Read the LAST_PROCESSED_EVENT_OFFSET attribute to know the starting read offset and read at most getMaxItemsAtOnce events.
        // 2. The collected results are passed to the handler function in EventProcessor.
        // 3. If the handler function has been successfully executed, truncate the internal Segment of the EventProcessor
        // according to the last successfully processed event. If an error occurs, throw and re-try.
        // 4. Report metrics for each EventProcessor registered and wait for ITERATION_SLEEP until the next iteration.
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.info("{}: Starting ContainerEventProcessor service.", this.traceObjectId);
        return Futures.loop(
                () -> !closed.get(),
                () -> Futures.delayedFuture(this.iterationDelay, this.executor)
                             .thenRunAsync(this::reportMetrics, this.executor),
                executor)
                    .handle((r, ex) -> {
                        this.closeProcessors();
                        return null;
                    });
    }

    /**
     * Reports the metrics for each EventProcessor once the processing iteration is complete.
     */
    private void reportMetrics() {
        for (EventProcessorImpl ep : eventProcessorMap.values()) {
            SegmentStoreMetrics.outstandingEventProcessorBytes(ep.getName(), containerId, ep.outstandingBytes.get());
            // Only report the last iteration processing latency if it has been successful.
            if (ep.lastIterationLatency.get() >= 0) {
                ep.metrics.batchProcessingLatency(ep.lastIterationLatency.get());
            }
        }
    }

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
                    .thenApply(segment -> eventProcessorMap.put(name, new EventProcessorImpl(name, segment, handler, config, onClose)))
                    .thenApply(v -> {
                        // Start the processing for this EventProcessor and return it.
                        eventProcessorMap.get(name).run();
                        return eventProcessorMap.get(name);
                    });
        }
    }

    private class EventProcessorImpl extends ContainerEventProcessor.EventProcessor implements Runnable {

        private final DirectSegmentAccess segment;
        private final AtomicLong outstandingBytes;
        private final AtomicLong lastIterationLatency;
        private final SegmentStoreMetrics.EventProcessor metrics;
        private final AtomicReference<CompletableFuture<Void>> runnable;
        private final AtomicBoolean closed;

        public EventProcessorImpl(@NonNull String name, @NonNull DirectSegmentAccess segment,
                                  @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                                  @NonNull EventProcessorConfig config, @NonNull Runnable onClose) {
            super(name, handler, config, onClose);
            this.segment = segment;
            this.outstandingBytes = new AtomicLong(0);
            this.lastIterationLatency = new AtomicLong(0);
            this.metrics = new SegmentStoreMetrics.EventProcessor(name, containerId);
            this.runnable = new AtomicReference<>();
            this.closed = new AtomicBoolean(false);
        }

        @Override
        public CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout) {
            Preconditions.checkArgument(event.getLength() > 0);
            Preconditions.checkArgument(event.getLength() +
                    ProcessorEventSerializer.HEADER_LENGTH < ProcessorEventSerializer.MAX_TOTAL_EVENT_SIZE);
            Exceptions.checkNotClosed(this.closed.get(), this);
            Preconditions.checkState(outstandingBytes.get() < getConfig().getMaxProcessorOutstandingBytes(),
                    "Too many outstanding events for {}.", this.getName());

            return segment.append(ProcessorEventSerializer.serializeEvent(event), null, timeout)
                          .thenApply(offset -> outstandingBytes.addAndGet(event.getLength() + ProcessorEventSerializer.HEADER_LENGTH));
        }

        @Override
        public void close() throws Exception {
            super.close();
            metrics.close();
            closed.set(true);
        }

        @Override
        public void run() {
            System.err.println("RUNNING EVENT PROCESSOR " + getName());
            runnable.set(Futures.loop(
                   () -> !closed.get(),
                   () -> Futures.delayedFuture(iterationDelay, executor)
                                .thenComposeAsync(v -> processEvents(), executor)
                           .handle((r, ex) -> {
                               System.err.println("EXCEPTION " + ex);
                               return null;
                           }),
                   executor));
        }

        /**
         * Returns a {@link CompletableFuture} that results from the execution of the following tasks for each EventProcessor:
         * i) getting the last processed event offset, ii) read at most {@link EventProcessorConfig#getMaxItemsAtOnce()},
         * iii) execute the handler function for the {@link EventProcessor}, and iv) truncate the internal Segment.
         */
        private CompletableFuture<Void> processEvents() {
            final Timer iterationTime = new Timer();
            return readEvents()
                    .thenComposeAsync(this::applyProcessorHandler, executor)
                    .thenApplyAsync(readResult -> truncateInternalSegment(readResult, iterationTime), executor)
                    .handleAsync((r, ex) -> {
                        // If we got an exception different from NoDataAvailableException, report it as something is off.
                        if (ex != null && !(ex instanceof NoDataAvailableException)) {
                            System.err.println("ERROR HERE " + ex.toString());
                            log.warn("Processing iteration failed for EventProcessor {}, retrying.", getName(), ex);
                            lastIterationLatency.set(-1); // Do not report latency, as there has been a failure.
                        }
                        return null;
                    });
        }

        private CompletableFuture<EventsReadAndTruncationPoints> readEvents() {
            System.err.println("READING EVENTS " + getName());
            long startOffset = segment.getInfo().getStartOffset();
            System.err.println("READING EVENTS " + getName() + " OFFSET " + startOffset);
            return CompletableFuture.supplyAsync(() -> segment.read(startOffset, ProcessorEventSerializer.MAX_TOTAL_EVENT_SIZE,
                                            containerOperationTimeout), executor)
                                    .thenComposeAsync(rr -> {
                                        System.err.println("AFTER SEGMENT READ " + getName() + " OFFSET " + startOffset);
                                        return AsyncReadResultProcessor.processAll(rr, executor, containerOperationTimeout);
                                        }, executor)
                                    .thenApply(inputData -> {
                                        System.err.println("AFTER PROCESS ALL READ " + getName() + " OFFSET " + startOffset);
                                        return deserializeEvents(inputData, startOffset,
                                                ProcessorEventSerializer.MAX_TOTAL_EVENT_SIZE);
                                    });
        }

        private EventsReadAndTruncationPoints deserializeEvents(BufferView inputData, long startOffset, int maxLength) {
            System.err.println("DESERIALIZE EVENTS " + getName());
            List<ProcessorEventData> events = new ArrayList<>();
            long nextOffset = startOffset;
            final long maxOffset = startOffset + maxLength;
            BufferView.Reader input = inputData.getBufferViewReader();
            try {
                while (nextOffset < maxOffset && events.size() < getConfig().getMaxItemsAtOnce()) {
                    ProcessorEventData event = ProcessorEventSerializer.deserializeEvent(input);
                    events.add(event);
                    // Update the offset to the beginning of the next entry.
                    nextOffset += ProcessorEventSerializer.HEADER_LENGTH + event.getLength();
                }
            } catch (BufferView.Reader.OutOfBoundsException ex) {
                // Events are of arbitrary size, so it is quite possible we stopped reading in the middle of an event.
                // Only if we have already read some data, we do not throw.
                if (events.isEmpty()) {
                    throw new NoDataAvailableException();
                }
            }

            return new EventsReadAndTruncationPoints(events);
        }

        private CompletableFuture<EventsReadAndTruncationPoints> applyProcessorHandler(EventsReadAndTruncationPoints readResult) {
            return CompletableFuture.runAsync(() -> getHandler().apply(readResult.getProcessorEventsData()), executor)
                                                                .thenApply(v -> readResult);
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

    @Override
    public void close() {
        log.info("{}: Closing ContainerEventProcessor service.", this.traceObjectId);
        closed.set(true);
    }

    //endregion

    @Data
    private static class EventsReadAndTruncationPoints {
        private final List<ProcessorEventData> eventsRead;

        public long getReadBytes() {
            return eventsRead.stream().map(d -> d.getLength() + ProcessorEventSerializer.HEADER_LENGTH).reduce(0, Integer::sum);
        }

        public List<BufferView> getProcessorEventsData() {
            return eventsRead.stream().map(ProcessorEventData::getData).collect(Collectors.toUnmodifiableList());
        }
    }

    private static class NoDataAvailableException extends RuntimeException {
    }

    //endregion

}