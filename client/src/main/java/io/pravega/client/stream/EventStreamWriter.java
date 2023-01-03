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

import io.pravega.client.stream.EventWriterConfig.EventWriterConfigBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A writer can write events to a stream.
 * 
 * This class is safe to use across threads, but doing so will not increase performance.
 * 
 * @param <Type> The type of events that go in this stream
 */
public interface EventStreamWriter<Type> extends AutoCloseable {

    /**
     * Send an event to the stream. Events that are written should appear in the stream exactly once. The
     * maximum size of the serialized event supported is defined at {@link Serializer#MAX_EVENT_SIZE}.
     *
     * Note that the implementation provides retry logic to handle connection failures and service host
     * failures. The number of retries is as specified in {@link EventWriterConfig#getRetryAttempts()}.
     * Internal retries will not violate the exactly once semantic so it is better to rely on them
     * than to wrap this with custom retry logic.
     * If all the retries fail the returned completableFuture(s) will we completed with a {@link io.pravega.common.util.RetriesExhaustedException}
     * post which no new writes can happen with this writer.
     *
     * @param event The event to be written to the stream (Null is disallowed)
     * @return A completableFuture that will complete when the event has been durably stored on the configured
     *         number of replicas, and is available for readers to see. This future may complete exceptionally
     *         if this cannot happen, however these exceptions are not transient failures. Failures that occur
     *         as a result of connection drops or host death are handled internally with multiple retires and
     *         exponential backoff. So there is no need to attempt to retry in the event of an exception.
     */
    CompletableFuture<Void> writeEvent(Type event);
    
    /**
     * Write an event to the stream. Similar to {@link #writeEvent(Object)} but provides a routingKey which is
     * used to specify ordering. Events written with the same routing key will be read by readers in exactly
     * the same order they were written. The maximum size of the serialized event supported is defined at
     * {@link Serializer#MAX_EVENT_SIZE}.
     *
     * Note that the implementation provides retry logic to handle connection failures and service
     * host failures. The number of retries is as specified in {@link EventWriterConfig#getRetryAttempts()}.
     * Internal retries will not violate the exactly once semantic so it is better to
     * rely on this than to wrap this method with custom retry logic.
     * If all the retries fail the returned completableFuture(s) will we completed with a {@link io.pravega.common.util.RetriesExhaustedException}
     * post which no new writes can happen with this writer.
     * 
     * @param routingKey A free form string that is used to route messages to readers. Two events written with
     *        the same routingKey are guaranteed to be read in order. Two events with different routing keys
     *        may be read in parallel. 
     * @param event The event to be written to the stream (Null is disallowed)
     * @return A completableFuture that will complete when the event has been durably stored on the configured
     *         number of replicas, and is available for readers to see. This future may complete exceptionally
     *         if this cannot happen, however these exceptions are not transient failures. Failures that occur 
     *         as a result of connection drops or host death are handled internally with multiple retires and
     *         exponential backoff. So there is no need to attempt to retry in the event of an exception.
     */
    CompletableFuture<Void> writeEvent(String routingKey, Type event);

    /**
     * Write an ordered list of events to the stream atomically for a given routing key. 
     * Events written with the same routing key will be read by readers in exactly the same order they were written. 
     * The maximum size of the serialized event individually should be {@link Serializer#MAX_EVENT_SIZE} and the 
     * collective batch should be less than twice the {@link Serializer#MAX_EVENT_SIZE}. 
     *
     * Note that the implementation provides retry logic to handle connection failures and service
     * host failures. The number of retries is as specified in {@link EventWriterConfig#getRetryAttempts()}.
     * Internal retries will not violate the exactly once semantic so it is better to
     * rely on this than to wrap this method with custom retry logic.
     * If all the retries fail the returned completableFuture(s) will we completed with a {@link io.pravega.common.util.RetriesExhaustedException}
     * post which no new writes can happen with this writer.
     *
     * @param routingKey A free form string that is used to route messages to readers. Two events written with
     *        the same routingKey are guaranteed to be read in order. Two events with different routing keys
     *        may be read in parallel. 
     * @param events The batch of events to be written to the stream (Null is disallowed)
     * @return A completableFuture that will complete when the event has been durably stored on the configured
     *         number of replicas, and is available for readers to see. This future may complete exceptionally
     *         if this cannot happen, however these exceptions are not transient failures. Failures that occur 
     *         as a result of connection drops or host death are handled internally with multiple retires and
     *         exponential backoff. So there is no need to attempt to retry in the event of an exception.
     */
    CompletableFuture<Void> writeEvents(String routingKey, List<Type> events);

    /**
     * Notes a time that can be seen by readers which read from this stream by
     * {@link EventStreamReader#getCurrentTimeWindow(Stream)}. The semantics or meaning of the timestamp
     * is left to the application. Readers might expect timestamps to be monotonic. So this is
     * recommended but not enforced.
     * 
     * There is no requirement to call this method. Never doing so will result in readers invoking
     * {@link EventStreamReader#getCurrentTimeWindow(Stream)} receiving a null for both upper and lower times.
     * 
     * Calling this method can be automated by setting
     * {@link EventWriterConfigBuilder#automaticallyNoteTime(boolean)} to true when creating a
     * writer.
     * 
     * @param timestamp a timestamp (in milliseconds) that represents the current location in the stream.
     */
    void noteTime(long timestamp);

    /**
     * Returns the configuration that this writer was create with.
     *
     * @return Writer configuration
     */
    EventWriterConfig getConfig();

    /**
     * Block until all events that have been passed to writeEvent's corresponding futures have completed.
     * This method will throw a {@link io.pravega.common.util.RetriesExhaustedException} if all internal retries fail.
     */
    void flush();

    /**
     * Calls flush and then closes the writer. (No further methods may be called)
     */
    @Override
    void close();
}
