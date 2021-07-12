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
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.storage.chunklayer.AbstractTaskQueue;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
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

@Slf4j
public class StorageEventProcessor implements AbstractTaskQueue<GarbageCollector.TaskInfo> {
    private final int containerID;
    private final ContainerEventProcessor eventProcessor;
    private final ChunkedSegmentStorage chunkedSegmentStorage;
    private final GarbageCollector.TaskInfo.Serializer serializer = new GarbageCollector.TaskInfo.Serializer();
    private final String traceObjectId;
    @Getter
    private final ConcurrentHashMap<String, ContainerEventProcessor.EventProcessor> eventProcessorMap = new ConcurrentHashMap<>();


    public StorageEventProcessor(int containerID, ContainerEventProcessor eventProcessor, ChunkedSegmentStorage chunkedSegmentStorage) {
        this.containerID = containerID;
        this.eventProcessor = Preconditions.checkNotNull(eventProcessor, "eventProcessor");
        this.chunkedSegmentStorage = Preconditions.checkNotNull(chunkedSegmentStorage, "chunkedSegmentStorage");
        this.traceObjectId = String.format("StorageEventProcessor[%d]", containerID);
    }

    /**
     * Adds a queue by the given name.
     *
     * @param queueName Name of the queue.
     * @param ignoreProcessing Whether the processing should be ignored.
     */
    @Override
    public CompletableFuture<Void> addQueue(String queueName, Boolean ignoreProcessing) {
        val config = new ContainerEventProcessor.EventProcessorConfig(chunkedSegmentStorage.getConfig().getGarbageCollectionMaxConcurrency(),
                Long.MAX_VALUE );
        val f =  ignoreProcessing ?
                eventProcessor.forDurableQueue(queueName) :
                eventProcessor.forConsumer(queueName,  this::processEvents, config);
        return f.thenAccept( processor -> eventProcessorMap.put(queueName, processor));
    }

    /**
     * Adds a task to queue.
     *
     * @param queueName Name of the queue.
     * @param task      Task to add.
     */
    @Override
    public CompletableFuture<Void> addTask(String queueName, GarbageCollector.TaskInfo task) {
        try {
            val processor = eventProcessorMap.get(queueName);
            if (null != processor) {
                return processor.add(serializer.serialize(task), Duration.ofMillis(1000)).thenAccept( l -> {

                });
            }
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() throws Exception {
    }

    CompletableFuture<Void> processEvents(List<BufferView> events) {
        log.info("{}: processEvents called with {} events", traceObjectId, events.size());
        ArrayList<GarbageCollector.TaskInfo> batch = new ArrayList<>();
        for (val event : events) {
            try {
                batch.add(serializer.deserialize(event));
            } catch (IOException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        return chunkedSegmentStorage.getGarbageCollector().processBatch(batch);
    }
}
