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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.storage.chunklayer.AbstractTaskQueueManager;
import io.pravega.segmentstore.storage.chunklayer.GarbageCollector;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link AbstractTaskQueueManager} that uses {@link io.pravega.segmentstore.server.ContainerEventProcessor.EventProcessor}
 * as underlying implementation.
 * This class acts as adaptor that converts calls on {@link io.pravega.segmentstore.server.ContainerEventProcessor.EventProcessor}
 * into appropriate calls on {@link AbstractTaskQueueManager} and vice versa.
 */
@Slf4j
public class StorageEventProcessor implements AbstractTaskQueueManager<GarbageCollector.TaskInfo> {

    private final static GarbageCollector.TaskInfo.Serializer SERIALIZER = new GarbageCollector.TaskInfo.Serializer();
    // For SLTS GC, truncating the ContainerEventProcessor internal Segment every 128KB is enough to prevent unnecessary
    // storage metadata activity that may occur if we truncate it too frequently.
    private final static long CONTAINER_EVENT_PROCESSOR_TRUNCATE_DATA_SIZE = 128 * 1024;

    private final int containerID;
    private final ContainerEventProcessor eventProcessor;
    private final Function<List<GarbageCollector.TaskInfo>, CompletableFuture<Void>> callBack;
    private final int maxItemsAtOnce;
    private final String traceObjectId;
    @Getter
    private final ConcurrentHashMap<String, ContainerEventProcessor.EventProcessor> eventProcessorMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param containerID    Container id.
     * @param eventProcessor Instance of {@link ContainerEventProcessor} to use.
     * @param callBack       Function which is called to process batch of events.
     * @param maxItemsAtOnce Maximum nuber of
     */
    public StorageEventProcessor(int containerID,
                                 ContainerEventProcessor eventProcessor,
                                 Function<List<GarbageCollector.TaskInfo>,
                                 CompletableFuture<Void>> callBack,
                                 int maxItemsAtOnce) {
        this.containerID = containerID;
        this.eventProcessor = Preconditions.checkNotNull(eventProcessor, "eventProcessor");
        this.callBack = Preconditions.checkNotNull(callBack, "callBack");
        this.maxItemsAtOnce = maxItemsAtOnce;
        this.traceObjectId = String.format("StorageEventProcessor[%d]", containerID);
    }

    /**
     * Adds a queue by the given name.
     *
     * @param queueName        Name of the queue.
     * @param ignoreProcessing Whether the processing should be ignored.
     */
    @Override
    public CompletableFuture<Void> addQueue(String queueName, Boolean ignoreProcessing) {
        Preconditions.checkNotNull(queueName, "queueName");
        val config = new ContainerEventProcessor.EventProcessorConfig(maxItemsAtOnce, Long.MAX_VALUE, CONTAINER_EVENT_PROCESSOR_TRUNCATE_DATA_SIZE);
        val f = ignoreProcessing ?
                eventProcessor.forDurableQueue(queueName) :
                eventProcessor.forConsumer(queueName, this::processEvents, config);
        return f.thenAccept(processor -> eventProcessorMap.put(queueName, processor));
    }

    /**
     * Adds a task to queue.
     *
     * @param queueName Name of the queue.
     * @param task      Task to add.
     */
    @Override
    public CompletableFuture<Void> addTask(String queueName, GarbageCollector.TaskInfo task) {
        Preconditions.checkNotNull(queueName, "queueName");
        Preconditions.checkNotNull(task, "task");
        try {
            val processor = eventProcessorMap.get(queueName);
            Preconditions.checkArgument(null != processor, "Attempt to add to non existent queue (%s).", queueName);
            return Futures.toVoid(processor.add(SERIALIZER.serialize(task), Duration.ofMillis(1000)));
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() throws Exception {
        for (val entry : eventProcessorMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                log.error("{}: Error while closing event processor name={}.", traceObjectId, entry.getKey(), e);
            }
        }
    }

    /**
     * Callback invoked by {@link io.pravega.segmentstore.server.ContainerEventProcessor.EventProcessor} when one or more
     * events have been read from the internal Segment.
     *
     * @param events List of events to process.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     * If the operation failed, it will contain the cause of the failure.
     */
    CompletableFuture<Void> processEvents(List<BufferView> events) {
        Preconditions.checkNotNull(events, "events");
        log.debug("{}: processEvents called with {} events", traceObjectId, events.size());
        ArrayList<GarbageCollector.TaskInfo> batch = new ArrayList<>();
        for (val event : events) {
            try {
                batch.add(SERIALIZER.deserialize(event));
            } catch (IOException e) {
                log.error("{}: processEvents failed while deserializing batch.", traceObjectId, e);
                return CompletableFuture.failedFuture(e);
            }
        }
        return callBack.apply(batch);
    }
}
