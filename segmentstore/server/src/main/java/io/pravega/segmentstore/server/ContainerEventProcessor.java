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
package io.pravega.segmentstore.server;

import com.google.common.util.concurrent.Service;
import io.pravega.common.util.BufferView;
import lombok.Data;
import lombok.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The {@link ContainerEventProcessor} is a sub-service running in a Segment Container that aims at providing a durable,
 * FIFO-like queue abstraction over an internal, system-critical Segment. The {@link ContainerEventProcessor} service
 * can manage one or more {@link EventProcessor}s, which are the ones that append events to the queue and handle events
 * read. The {@link ContainerEventProcessor} tails the internal Segments related to each {@link EventProcessor}. When
 * it has at least 1 event to read on an {@link EventProcessor}'s Segment, it invokes its handler. If there are multiple
 * events available, up to {@link EventProcessorConfig#getMaxItemsAtOnce()} should be used as input for the handler.
 *
 * If the handler completes normally, the items will be removed from the queue (i.e., the {@link EventProcessor}'s
 * Segment will be truncated up to that offset). If the handler completes with an exception, the items will not be
 * removed; we will retry indefinitely. It is up to the consumer to handle any exceptions; any exceptions that bubble up
 * to us will be considered re-triable (except {@link DataCorruptionException}, etc.).
 */
public interface ContainerEventProcessor extends AutoCloseable, Service {

    /**
     * Instantiates a new {@link EventProcessor}. If the internal Segment exists, the {@link EventProcessor} will re-use
     * it. If not, a new internal Segment will be created. Multiple calls to this method for the same name should result
     * in returning the same {@link EventProcessor} object.
     *
     * @param name     Name of the {@link EventProcessor} object.
     * @param handler  Function that will be invoked when one or more events have been read from the internal Segment.
     * @param config   {@link EventProcessorConfig} for this {@link EventProcessor}.
     * @return A new {@link EventProcessor} object associated to its own internal Segment.
     */
    EventProcessor forConsumer(@NonNull String name, @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                               @NonNull EventProcessorConfig config);

    /**
     * An {@link EventProcessor} object allows to durably append events to the {@link ContainerEventProcessor} service.
     * Each {@link EventProcessor} instance has associated an internal Segment and is uniquely identified by its name
     * within a Segment Container. In addition, it also allows to compute the events stored upon a successful read via
     * the handler function. The {@link ContainerEventProcessor} is in charge to invoke the handler function for one or
     * multiple events in FIFO order.
     */
    @Data
    abstract class EventProcessor implements AutoCloseable {
        private final String name;
        private final Function<List<BufferView>, CompletableFuture<Void>> handler;
        private final EventProcessorConfig config;

        public abstract CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout);
    }

    /**
     * This class provides configuration settings for {@link EventProcessor}.
     */
    @Data
    class EventProcessorConfig {
        private final int maxItemsAtOnce;
    }
}
