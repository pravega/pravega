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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import lombok.Cleanup;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
public final class ContainerEventProcessorImpl extends AbstractIdleService implements ContainerEventProcessor {

    private static final UUID LAST_PROCESSED_EVENT_OFFSET = new UUID(0, 0);
    private static final Duration ITERATION_SLEEP = Duration.ofMillis(100); // Execute 10 iterations per second at most.
    private static final Duration OPERATION_TIMEOUT = Duration.ofMillis(20);

    private final int containerId;
    private final Map<String, EventProcessorImpl> eventProcessorMap = new ConcurrentHashMap<>();
    private final Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier;
    private final AtomicBoolean closed;
    private final ScheduledExecutorService executor;

    //region Constructor

    ContainerEventProcessorImpl(int containerId, @NonNull Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier,
                                @NonNull ScheduledExecutorService executor) {
        this.containerId = containerId;
        this.segmentSupplier = segmentSupplier;
        this.executor = executor;
        this.closed = new AtomicBoolean(false);
    }

    //endregion

    //region ContainerEventProcessor Implementation

    @Override
    public EventProcessor forConsumer(@NonNull String name, @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                               @NonNull EventProcessorConfig config) {
        // If the EventProcessor is already loaded, just return it.
        if (eventProcessorMap.containsKey(name)) {
            return eventProcessorMap.get(name);
        }

        // Instantiate the EventProcessor and put it into the map.
        DirectSegmentAccess segment = segmentSupplier.apply(name).join();
        eventProcessorMap.put(name, new EventProcessorImpl(name, segment, handler, config));
        return eventProcessorMap.get(name);
    }

    class EventProcessorImpl extends ContainerEventProcessor.EventProcessor {

        // Maximum amount of bytes we allow to be non-processed before start rejecting add requests (4MBs).
        private final static int MAX_OUTSTANDING_BYTES = 4 * 1024 * 1024;

        private final DirectSegmentAccess segment;
        private final AtomicLong outstandingBytes;
        private final AtomicLong lastIterationLatency;
        private final SegmentStoreMetrics.EventProcessor metrics;

        public EventProcessorImpl(@NonNull String name, @NonNull DirectSegmentAccess segment,
                                  @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                                  @NonNull EventProcessorConfig config) {
            super(name, handler, config);
            this.segment = segment;
            this.outstandingBytes = new AtomicLong(0);
            this.lastIterationLatency = new AtomicLong(0);
            this.metrics = new SegmentStoreMetrics.EventProcessor(name, containerId);
        }

        @Override
        public CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout) {
            Preconditions.checkArgument(event.getLength() > 0);
            Preconditions.checkState(outstandingBytes.get() < MAX_OUTSTANDING_BYTES,
                    "Too many outstanding events for {}.", this.getName());

            // Appends may be variable in size. We encode each event as [EVENT_LENGTH + EVENT_DATA]. This is needed to
            // understand the boundaries of events in the absence of an actual notion of event.
            return segment.append(BufferView.builder().add(intToByteArraySegment(event.getLength())).add(event).build(), null, timeout)
                    .thenApply(offset -> outstandingBytes.addAndGet(event.getLength() + Integer.BYTES));
        }

        @Override
        public void close() {
            metrics.close();
        }
    }

    @Override
    public void close() {
        log.info("Closing ContainerEventProcessor service.");
        closed.set(true);
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void shutDown() throws Exception {
        log.info("Shutting down ContainerEventProcessor service.");
        eventProcessorMap.forEach((k, v) -> v.close());
        eventProcessorMap.clear();
        log.info("Shut down of ContainerEventProcessor service complete.");
    }

    @Override
    protected void startUp() throws Exception {
        // A ContainerEventProcessor iteration is made of the following stages (for each EventProcessor registered):
        // 1. Read the LAST_PROCESSED_EVENT_OFFSET attribute to know the starting read offset and read at most getMaxItemsAtOnce events.
        // 2. The collected results are passed to the handler function in EventProcessor.
        // 3. If the handler function has been successfully executed, truncate the internal Segment of the EventProcessor
        // according to the last successfully processed event. If an error occurs, throw and re-try.
        // 4. Report metrics for each EventProcessor registered and wait for ITERATION_SLEEP until the next iteration.
        log.info("Starting ContainerEventProcessor service.");
        Futures.loop(
                () -> !closed.get(),
                () -> Futures.delayedFuture(ITERATION_SLEEP, this.executor)
                             .thenRunAsync(this::processEvents, this.executor)
                             .thenRunAsync(this::reportMetrics, this.executor),
                executor);
    }

    /**
     * Returns a {@link CompletableFuture} that results from the execution of the following tasks for each EventProcessor:
     * i) getting the last processed event offset, ii) read at most {@link EventProcessorConfig#getMaxItemsAtOnce()},
     * iii) execute the handler function for the {@link EventProcessor}, and iv) truncate the internal Segment.
     */
    private CompletableFuture<Void> processEvents() {
        final Timer iterationTime = new Timer();
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (EventProcessorImpl ep : eventProcessorMap.values()) {
            futures.add(readEventsForEventProcessor(ep)
                    .thenApplyAsync(readResult -> applyProcessorHandler(ep, readResult), this.executor)
                    .thenApplyAsync(readResult -> updateAndTruncateInternalSegment(ep, readResult, iterationTime), this.executor)
                    .handleAsync((r, ex) -> {
                        // If we got an exception different from NoDataAvailableException, report it as something is off.
                        if (ex != null && !(ex instanceof NoDataAvailableException)) {
                            log.warn("Processing iteration failed for EventProcessor {}, retrying.", ep.getName(), ex);
                            ep.lastIterationLatency.set(-1); // Do not report latency, as there has been a failure.
                            return null;
                        }
                        return r;
                    }));
        }
        return Futures.allOf(futures);
    }

    /**
     * Reports the metrics for each EventProcessor once the processing iteration is complete.
     */
    private void reportMetrics() {
        for (EventProcessorImpl ep : eventProcessorMap.values()) {
            SegmentStoreMetrics.outstandingEventProcessorBytes(ep.getName(), this.containerId, ep.outstandingBytes.get());
            // Only report the last iteration processing latency if it has been successful.
            if (ep.lastIterationLatency.get() >= 0) {
                ep.metrics.batchProcessingLatency(ep.lastIterationLatency.get());
            }
        }
    }

    private EventsReadAndTruncationPoints applyProcessorHandler(EventProcessorImpl ep, EventsReadAndTruncationPoints readResult) {
        try {
            // This executes the handler function, which is code outside this class. Let's catch any kind of exception
            // and re-throw it as a cause for CompletionException.
            ep.getHandler().apply(readResult.getEventsRead());
            return readResult;
        } catch (Exception e) {
            log.warn("Exception invoking handler for EventProcessor {}.", ep.getName(), e);
            throw new CompletionException(e);
        }
    }

    private CompletableFuture<EventsReadAndTruncationPoints> readEventsForEventProcessor(EventProcessorImpl eventProcessor) {
        return getLastProcessedEventOffset(eventProcessor.segment)
                .thenApplyAsync(currentOffset -> readEventsFromSegment(eventProcessor, currentOffset), this.executor);
    }

    private EventsReadAndTruncationPoints readEventsFromSegment(EventProcessorImpl eventProcessor, int currentOffset) {
        List<BufferView> readEvents = new ArrayList<>();
        int finalTruncationOffset = currentOffset;
        while (readEvents.size() < eventProcessor.getConfig().getMaxItemsAtOnce()) {
            try {
                int appendLength = bufferViewToInt(readAppendOfLength(eventProcessor.segment, finalTruncationOffset, Integer.BYTES));
                finalTruncationOffset += Integer.BYTES;
                BufferView data = readAppendOfLength(eventProcessor.segment, finalTruncationOffset, appendLength);
                readEvents.add(data);
                finalTruncationOffset += data.getLength();
            } catch (Exception e) {
                // We got some data, do not throw, just stop looping.
                if (!readEvents.isEmpty()) {
                    break;
                }
                // TimeoutException is expected if there is no data in the Segment. Otherwise it may be a problem.
                throw (e instanceof TimeoutException) ? new NoDataAvailableException() : new CompletionException(e);
            }
        }
        return new EventsReadAndTruncationPoints(readEvents, currentOffset, finalTruncationOffset);
    }

    private CompletableFuture<Void> updateAndTruncateInternalSegment(EventProcessorImpl ep,
                                                                     EventsReadAndTruncationPoints readResult,
                                                                     Timer iterationTime) {
        return updateLastProcessedEventOffset(ep.segment, readResult.getFinalTruncationOffset())
                .thenCompose(v -> ep.segment.truncate(readResult.getFinalTruncationOffset(), OPERATION_TIMEOUT))
                .thenAccept(v -> {
                    // Decrement outstanding bytes and publish the last latency value after successful truncation.
                    ep.outstandingBytes.addAndGet(-readResult.getReadBytes());
                    ep.lastIterationLatency.set(iterationTime.getElapsedMillis());
                });
    }

    private BufferView readAppendOfLength(DirectSegmentAccess segment, int offset, int length) throws Exception {
        @Cleanup
        ReadResult readResult = segment.read(offset, length, OPERATION_TIMEOUT);
        ReadResultEntry entry = readResult.next();
        entry.requestContent(OPERATION_TIMEOUT);
        return entry.getContent().get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private int bufferViewToInt(BufferView appendLengthContent) throws IOException {
        return ByteBuffer.wrap(appendLengthContent.getReader().readAllBytes()).getInt();
    }

    private ByteArraySegment intToByteArraySegment(int intValue) {
        return new ByteArraySegment(ByteBuffer.allocate(Integer.BYTES).putInt(intValue).array());
    }

    private CompletableFuture<Integer> getLastProcessedEventOffset(DirectSegmentAccess segment) {
        return segment.getAttributes(Collections.singletonList(LAST_PROCESSED_EVENT_OFFSET), true, OPERATION_TIMEOUT)
                      .thenApply(attributes -> attributes.getOrDefault(LAST_PROCESSED_EVENT_OFFSET, 0L).intValue());
    }

    private CompletableFuture<Void> updateLastProcessedEventOffset(DirectSegmentAccess segment, int offset) {
        AttributeUpdate update = new AttributeUpdate(LAST_PROCESSED_EVENT_OFFSET, AttributeUpdateType.ReplaceIfGreater, offset);
        return segment.updateAttributes(Collections.singletonList(update), OPERATION_TIMEOUT);
    }

    @Data
    private static class EventsReadAndTruncationPoints {
        private final List<BufferView> eventsRead;
        private final int initialTruncationOffset;
        private final int finalTruncationOffset;

        public int getReadBytes() {
            return finalTruncationOffset - initialTruncationOffset;
        }
    }

    private static class NoDataAvailableException extends RuntimeException {}

    //endregion
}