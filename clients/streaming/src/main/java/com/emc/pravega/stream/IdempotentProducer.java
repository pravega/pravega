package com.emc.pravega.stream;

import java.util.concurrent.Future;

/**
 * A producer can publish events to a stream. Similar to {@link Producer} but with the ability to
 * provide a {@link Sequence} on publish to prevent duplicates, in the event that the producer
 * process crashes and restarts.
 * 
 * This class is safe to use across threads, but doing so will not increase performance.
 * 
 * @param <Type> The type of events that go in this stream
 */
public interface IdempotentProducer<Type> extends AutoCloseable {
    
    /**
     * Send an event to the stream. Events that are published should appear in the stream exactly
     * once. Note that the implementation provides retry logic to handle connection failures and
     * service host failures.
     * 
     * This method takes a {@link Sequence} on each event for the purposes of preventing duplicates.
     * Sequences are user defined and assumed to be monotonically increasing. If a sequence is
     * passed to publish that is less or equal to a sequence previously passed to publish, the event
     * is acked without wiring it to the stream.
     * 
     * This allows for applications with a deterministic producer to de-dup events in the event that
     * the producer dies.
     * 
     * 
     * @param routingKey A free form string that is used to route messages to consumers. Two events
     *        published with the same routingKey are guaranteed to be consumed in order. Two events
     *        with different routing keys may be consumed in parallel.
     * @param sequence An ever increasing sequence. If this method is called with a lower sequence
     *        the event will not be written.
     * @param event The event to be published to the stream
     * @return A future that will complete when the event has been durably stored on the configured
     *         number of replicas, and is available for consumers to see. This future may complete
     *         exceptionally if this cannot happen, however these exceptions are not transient
     *         failures. Failures that occur as a result of connection drops or host death are
     *         handled internally with multiple retires and exponential backoff. So there is no need
     *         to attempt to retry in the event of an exception.
     */
    Future<Void> publish(String routingKey, Sequence sequence, Type event);

    
    /**
     * Returns the configuration that this producer was create with.
     */
    ProducerConfig getConfig();

    /**
     * Block until all events that have been passed to publish's corresponding futures have completed.
     */
    void flush();

    /**
     * Calls flush and then closes the producer. (No further methods may be called)
     */
    @Override
    void close();
}
