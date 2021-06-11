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

import io.pravega.common.util.BufferView;
import lombok.Data;
import lombok.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The {@link ContainerEventProcessor} is a sub-component in a Segment Container that aims at providing a durable,
 * FIFO-like queue abstraction over an internal, system-critical Segment. The {@link ContainerEventProcessor} can manage
 * one or more {@link EventProcessor}s, which are the ones that append events to the queue and handle events read. The
 * also {@link ContainerEventProcessor} reports metrics for all the registered {@link EventProcessor}s.
 */
public interface ContainerEventProcessor extends AutoCloseable {

    /**
     * Instantiates a new {@link EventProcessor}. If the internal Segment exists, the {@link EventProcessor} will re-use
     * it. If not, a new internal Segment will be created. Multiple calls to this method for the same name should result
     * in returning the same {@link EventProcessor} object.
     *
     * @param name     Name of the {@link EventProcessor} object.
     * @param handler  Function that will be invoked when one or more events have been read from the internal Segment.
     * @param config   {@link EventProcessorConfig} for this {@link EventProcessor}.
     * @return A {@link CompletableFuture} that, when completed, returns a new {@link EventProcessor} object associated
     * to its own internal Segment.
     */
    CompletableFuture<EventProcessor> forConsumer(@NonNull String name,
                                                  @NonNull Function<List<BufferView>, CompletableFuture<Void>> handler,
                                                  @NonNull EventProcessorConfig config);

    /**
     * Instantiates a new {@link EventProcessor} that only enables to add new data to the internal Segment. This may be
     * useful when a user needs to store events, but it is not clear how the best approach to process them will be. This
     * append-only {@link EventProcessor} can be replaced in the future by a regular one with a handler function that
     * will consume all the events in the internal Segment. If internal Segment exists, the {@link EventProcessor} will
     * re-use it. If not, a new internal Segment will be  created. Multiple calls to this method for the same name
     * should result in returning the same {@link EventProcessor} object.
     *
     * @param name     Name of the {@link EventProcessor} object.
     * @return A {@link CompletableFuture} that, when completed, returns a new {@link EventProcessor} object associated
     * to its own internal Segment.
     */
    CompletableFuture<EventProcessor> forDurableQueue(@NonNull String name);

    /**
     * Each {@link EventProcessor} instance has associated an internal Segment and is uniquely identified by its name
     * within a Segment Container. An {@link EventProcessor} tails its internal Segment looking for new events. When it
     * has at least 1 event to read on its Segment, it invokes its handler. If there are multiple events available, up
     * to {@link EventProcessorConfig#getMaxItemsAtOnce()} should be used as input for the handler.
     *
     * If the handler completes normally, the items will be removed from the queue (i.e., the {@link EventProcessor}'s
     * Segment will be truncated up to that offset). If the handler completes with an exception, the items will not be
     * removed; we will retry indefinitely. It is up to the consumer to handle any exceptions; any exceptions that
     * bubble up to us will be considered re-triable (except {@link DataCorruptionException}, etc.).
     */
    @Data
    abstract class EventProcessor implements AutoCloseable {
        @NonNull
        private final String name;
        @NonNull
        private final Function<List<BufferView>, CompletableFuture<Void>> handler;
        @NonNull
        private final EventProcessorConfig config;
        @NonNull
        private final Runnable onClose;

        /**
         * Persist a new event to the {@link EventProcessor}'s durable queue. Once the return future completes, it
         * returns the amount of outstanding byes for this {@link EventProcessor} object.
         *
         * @param event     Event to be added to the {@link EventProcessor}'s durable queue.
         * @param timeout   Maximum amount of time for this operation to be completed.
         * @return          A {@link CompletableFuture} that, when completed, will acknowledge that the event has been
         *                  durably stored and returns the amount of outstanding bytes for this {@link EventProcessor}.
         */
        public abstract CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout);

        /**
         * When an {@link EventProcessor} is closed, it should be auto-unregistered from the existing set of active
         * {@link EventProcessor} objects.
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            this.onClose.run();
        }
    }

    /**
     * This class provides configuration settings for {@link EventProcessor}.
     */
    @Data
    class EventProcessorConfig {
        /**
         * Maximum number of events to be processed at once per processing iteration.
         */
        private final int maxItemsAtOnce;

        /**
         * Maximum number of outstanding bytes that can be accumulated for a given {@link EventProcessor}.
         */
        private final long maxProcessorOutstandingBytes;
    }

    /**
     * Representation of an event written to an {@link EventProcessor}. It consists of:
     * - Version (1 byte)
     * - Length (4 bytes)
     * - Data (BufferView of length at most 1024 * 1024 - 5 bytes)
     */
    @Data
    class ProcessorEventData {

        // Serialization Version (1 byte), Entry Length (4 bytes)
        public static final int HEADER_LENGTH = Byte.BYTES + Integer.BYTES;

        // Set a maximum length to individual events to be processed by EventProcessor (1MB).
        public static final int MAX_TOTAL_EVENT_SIZE = 1024 * 1024;

        private final byte version;
        private final int length;
        private final BufferView data;
    }
}
