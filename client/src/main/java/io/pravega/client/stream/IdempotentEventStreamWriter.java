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
package io.pravega.client.stream;

import java.util.concurrent.Future;

/**
 * A writer can write events to a stream. Similar to {@link EventStreamWriter} but with the
 * ability to provide a {@link Sequence} on {@link #writeEvent(String, Sequence, Object)} to prevent
 * duplicates, in the event that the writer process crashes and restarts.
 * 
 * This class is safe to use across threads, but doing so will not increase performance.
 * 
 * @param <Type> The type of events that go in this stream
 */
public interface IdempotentEventStreamWriter<Type> extends AutoCloseable {
    
    /**
     * Send an event to the stream. Events that are written should appear in the stream exactly
     * once. Note that the implementation provides retry logic to handle connection failures and
     * service host failures.
     * 
     * This method takes a {@link Sequence} on each event for the purposes of preventing duplicates.
     * Sequences are user defined and assumed to be monotonically increasing. If a sequence is
     * passed to writeEvent that is less or equal to a sequence previously passed to writeEvent, the
     * event is acked without wiring it to the stream.
     * 
     * This allows for applications with a deterministic writer to de-dup events in the event that
     * the writer dies.
     * 
     * 
     * @param routingKey A free form string that is used to route messages to readers. Two events
     *        written with the same routingKey are guaranteed to be read in order. Two events with
     *        different routing keys may be read in parallel.
     * @param sequence An ever increasing sequence. If this method is called with a lower sequence
     *        the event will not be written.
     * @param event The event to be written to the stream
     * @return A future that will complete when the event has been durably stored on the configured
     *         number of replicas, and is available for readers to see. This future may complete
     *         exceptionally if this cannot happen, however these exceptions are not transient
     *         failures. Failures that occur as a result of connection drops or host death are
     *         handled internally with multiple retires and exponential backoff. So there is no need
     *         to attempt to retry in the event of an exception.
     */
    Future<Void> writeEvent(String routingKey, Sequence sequence, Type event);

    
    /**
     * Returns the configuration that this writer was create with.
     *
     * @return Writer configuration
     */
    EventWriterConfig getConfig();

    /**
     * Block until all events that have been passed to writeEvent's corresponding futures have completed.
     */
    void flush();

    /**
     * Calls flush and then closes the writer. (No further methods may be called)
     */
    @Override
    void close();
}
