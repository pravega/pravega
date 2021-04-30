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
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Slf4j
public final class ContainerEventProcessorImpl extends AbstractIdleService implements ContainerEventProcessor {

    private static final UUID LAST_PROCESSED_EVENT_OFFSET = new UUID(0, 0);
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(1);

    private final int containerId;
    private final Map<String, EventProcessorImpl> eventProcessorMap = new ConcurrentHashMap<>();
    private final Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier;
    private final AtomicBoolean closed;
    private final ScheduledExecutorService executor;

    ContainerEventProcessorImpl(int containerId, @NonNull Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier,
                                @NonNull ScheduledExecutorService executor) {
        this.containerId = containerId;
        this.segmentSupplier = segmentSupplier;
        this.executor = executor;
        this.closed = new AtomicBoolean(false);
    }

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

    @Override
    protected void startUp() throws Exception {
        // A ContainerEventProcessor iteration is made of the following stages:
        // 1. For each EventProcessor registered, read at most getMaxItemsAtOnce events and invoke the appropriate handler.
        // 2. The collected results are passed to the handler function in EventProcessor.
        // 3. For each EventProcessor registered, truncate its internal Segment according to the last successfully processed event.
        log.info("Starting ContainerEventProcessor.");
        Futures.loop(
                () -> !closed.get(),
                () -> Futures.delayedFuture(OPERATION_TIMEOUT, this.executor)
                             .thenRunAsync(this::processEvents, this.executor)
                             .thenRunAsync(this::reportMetrics, this.executor),
                executor);

    }

    private void processEvents() {
        final Timer iterationTime = new Timer();
        for (EventProcessorImpl ep : eventProcessorMap.values()) {
            EventsReadAndTruncationOffset readResult = readEventsForEventProcessor(ep);
            if (readResult.hasReadEvents()) {
                // Call the EventProcessor handler with the read items.
                ep.getHandler()
                        .apply(readResult.getEventsRead())
                        .handleAsync((r, ex) -> {
                            if (ex == null) {
                                // Truncate the Segment to the last successfully processed event.
                                return updateLastProcessedEventOffset(ep.segment, readResult.getFinalTruncationOffset())
                                        .thenCompose(v -> ep.segment.truncate(readResult.getFinalTruncationOffset(), OPERATION_TIMEOUT))
                                        .thenAccept(v ->{
                                            ep.outstandingBytes.addAndGet(-readResult.getReadBytes());
                                            ep.lastIterationLatency.set(iterationTime.getElapsedMillis());
                                        });
                            }
                            log.warn("Exception when invoking handler, retrying.", ex);
                            ep.lastIterationLatency.set(0);
                            return null;
                        }).join();

            }
        }
    }

    private void reportMetrics() {
        for (EventProcessorImpl ep : eventProcessorMap.values()) {
            SegmentStoreMetrics.outstandingEventProcessorBytes(ep.getName(), this.containerId, ep.outstandingBytes.get());
            // Only report the last iteration processing latency if it has been successful.
            if (ep.lastIterationLatency.get() > 0) {
                ep.metrics.batchProcessingLatency(ep.lastIterationLatency.get());
            }
        }
    }

    @Override
    protected void shutDown() throws Exception {
        eventProcessorMap.forEach((k, v) -> v.close());
        eventProcessorMap.clear();
    }

    @Override
    public void close() {
        closed.set(true);
    }

    class EventProcessorImpl extends ContainerEventProcessor.EventProcessor {

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
            Preconditions.checkState(outstandingBytes.get() < MAX_OUTSTANDING_BYTES, "Too many outstanding events for {}.", this.getName());
            return segment.append(BufferView.builder().add(intToByteArraySegment(event.getLength())).add(event).build(), null, timeout)
                          .thenApply(outstandingBytes::addAndGet);
        }


        @Override
        public void close() {
            metrics.close();
        }
    }

    private EventsReadAndTruncationOffset readEventsForEventProcessor(EventProcessorImpl eventProcessor) {
        List<BufferView> readEvents = new ArrayList<>();
        int finalTruncationOffset, initialTruncationOffset;
        finalTruncationOffset = initialTruncationOffset = getLastProcessedEventOffset(eventProcessor.segment);
        boolean isThereDataToRead = true;
        while (isThereDataToRead && readEvents.size() <= eventProcessor.getConfig().getMaxItemsAtOnce()) {
            try {
                int appendLength = bufferViewToInt(readAppendOfLength(eventProcessor.segment, finalTruncationOffset, Integer.BYTES));
                finalTruncationOffset += Integer.BYTES;
                BufferView data = readAppendOfLength(eventProcessor.segment, finalTruncationOffset, appendLength);
                readEvents.add(data);
                finalTruncationOffset += data.getLength();
            } catch (ExecutionException | InterruptedException | TimeoutException | IOException e) {
                log.debug("{}: Nothing else to read.", eventProcessor.getName());
                isThereDataToRead = false;
            }
        }

        return new EventsReadAndTruncationOffset(readEvents, initialTruncationOffset, finalTruncationOffset);
    }

    private BufferView readAppendOfLength(DirectSegmentAccess segment, int offset, int length) throws ExecutionException, InterruptedException, TimeoutException {
        ReadResult readResult = segment.read(offset, length, OPERATION_TIMEOUT);
        BufferView content = readResult.next().getContent().get(1, TimeUnit.SECONDS);
        readResult.close();
        return content;
    }

    private int bufferViewToInt(BufferView appendLengthContent) throws IOException {
        byte[] appendLengthArray = appendLengthContent.getReader().readAllBytes();
        return ByteBuffer.wrap(appendLengthArray).getInt();
    }

    private ByteArraySegment intToByteArraySegment(int intValue) {
        return new ByteArraySegment(ByteBuffer.allocate(Integer.BYTES).putInt(intValue).array());
    }

    private int getLastProcessedEventOffset(DirectSegmentAccess segment) {
        Map<UUID, Long> attributes = segment.getAttributes(Collections.singletonList(LAST_PROCESSED_EVENT_OFFSET), true, Duration.ofSeconds(1)).join();
        return attributes.getOrDefault(LAST_PROCESSED_EVENT_OFFSET, 0L).intValue();
    }

    private CompletableFuture<Void> updateLastProcessedEventOffset(DirectSegmentAccess segment, int offset) {
        AttributeUpdate update = new AttributeUpdate(LAST_PROCESSED_EVENT_OFFSET, AttributeUpdateType.ReplaceIfGreater, offset);
        return segment.updateAttributes(Collections.singletonList(update), Duration.ofSeconds(1));
    }

    @Data
    private static class EventsReadAndTruncationOffset {
        private final List<BufferView> eventsRead;
        private final int initialTruncationOffset;
        private final int finalTruncationOffset;

        public boolean hasReadEvents() {
            return eventsRead != null && eventsRead.size() > 0;
        }

        public int getReadBytes() {
            return finalTruncationOffset - initialTruncationOffset;
        }
    }
}