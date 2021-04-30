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
 * read. The {@link ContainerEventProcessor} tails each of the Segments registered for each {@link EventProcessor}. When
 * it has at least 1 Event to read on an {@link EventProcessor}'s Segment, it invokes its handler. If there are multiple
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

    
    @Data
    abstract class EventProcessor implements AutoCloseable {
        private final String name;
        private final Function<List<BufferView>, CompletableFuture<Void>> handler;
        private final EventProcessorConfig config;

        public abstract CompletableFuture<Long> add(@NonNull BufferView event, Duration timeout);
    }

    @Data
    class EventProcessorConfig {
        private final int maxItemsAtOnce;
    }
}
