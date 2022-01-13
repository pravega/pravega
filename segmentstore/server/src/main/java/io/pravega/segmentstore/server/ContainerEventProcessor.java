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
 * one or more {@link EventProcessor}s, which are the ones that append events to the queue and handle events read.
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
     * will consume all the events in the internal Segment. If the internal Segment exists, the {@link EventProcessor}
     * will re-use it. If not, a new internal Segment will be  created. Multiple calls to this method for the same name
     * should result in returning the same {@link EventProcessor} object.
     *
     * @param name     Name of the {@link EventProcessor} object.
     * @return A {@link CompletableFuture} that, when completed, returns a new {@link EventProcessor} object associated
     * to its own internal Segment.
     */
    CompletableFuture<EventProcessor> forDurableQueue(@NonNull String name);

    /**
     * The {@link EventProcessor} is a service that takes care of adding and processing events from a durable queue. It
     * is expected to be unique within a {@link ContainerEventProcessor} (e.g., identified by name), allow callers to
     * add new elements to the durable queue, and perform ordered, offline processing of events by invoking the handler.
     * The processing limits of any implementation of this class should be guided by {@link EventProcessorConfig}.
     */
    interface EventProcessor extends AutoCloseable {

        /**
         * Persist a new event to the {@link EventProcessor}'s durable queue. Once the return future completes, it
         * returns the amount of outstanding byes for this {@link EventProcessor} object.
         *
         * @param event     Event to be added to the {@link EventProcessor}'s durable queue.
         * @param timeout   Maximum amount of time for this operation to be completed.
         * @return          A {@link CompletableFuture} that, when completed, will acknowledge that the event has been
         *                  durably stored and returns the amount of outstanding bytes for this {@link EventProcessor}.
         * @throws TooManyOutstandingBytesException if the {@link EventProcessor} has reached the maximum configured
         * outstanding bytes.
         */
        CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout) throws TooManyOutstandingBytesException;
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

        /**
         * Amount of processed data to be accumulated to truncate the internal Segment.
         */
        private final long processedDataTruncationSizeInBytes;
    }

    /**
     * Exception thrown when an {@link EventProcessor} backlog reaches the maximum limit of bytes configured.
     * Usually, this may indicate that either the handler configured is failing and cannot process events or that the
     * amount of events being ingested is too much compared to the processing speed.
     */
    class TooManyOutstandingBytesException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        /**
         * Creates an new instance of the TooManyOutstandingBytesException class.
         *
         * @param eventProcessorId The Id of the {@link EventProcessor} that reached the outstanding byte limit.
         */
        public TooManyOutstandingBytesException(String eventProcessorId) {
            super(String.format("%s reached its maximum outstanding bytes limit.", eventProcessorId));
        }
    }
}
