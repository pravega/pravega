/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventWriterConfig.EventWriterConfigBuilder;
import java.util.UUID;
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
     * Send an event to the stream. Events that are written should appear in the stream exactly once.
     *
     * Note that the implementation provides retry logic to handle connection failures and service host
     * failures. Internal retries will not violate the exactly once semantic so it is better to rely on them
     * than to wrap this with custom retry logic.
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
     * the same order they were written.  
     *
     * Note that the implementation provides retry logic to handle connection failures and service
     * host failures. Internal retries will not violate the exactly once semantic so it is better to
     * rely on this than to wrap this method with custom retry logic.
     * 
     * @param routingKey A free form string that is used to route messages to readers. Two events written with
     *        the same routingKey are guaranteed to be read in order. Two events with different routing keys
     *        may be read in parallel. 
     * @param event The event to be written to the stream (Null is disallowed)
     * @return A completableFuture that will complete when the event has been durably stored on the configured
     *         number of replicas, and is available for readers to see. This future may complete exceptionally
     *         cannot happen, however these exceptions are not transient failures. Failures that occur as a
     *         if this result of connection drops or host death are handled internally with multiple retires and
     *         exponential backoff. So there is no need to attempt to retry in the event of an exception.
     */
    CompletableFuture<Void> writeEvent(String routingKey, Type event);
    
    /**
     * Notes a time that can be seen by readers which read from this stream by
     * {@link EventStreamReader#getMostRecentWatermark()}. The semantics or meaning of the timestamp
     * is left to the application. Readers might expect timestamps to be monotonic. So this is
     * recommended but not enforced.
     * 
     * There is no requirement to call this method. Never doing so will result in readers invoking
     * {@link EventStreamReader#getMostRecentWatermark()} receiving a null.
     * 
     * Calling this method can be automated by setting
     * {@link EventWriterConfigBuilder#automaticallyNoteTime(boolean)} to true when creating a
     * writer.
     * 
     * @param timestamp a timestamp that represents the current location in the stream.
     */
    void noteTime(long timestamp);

    /**
     * Start a new transaction on this stream. This allows events written to the transaction be written an committed atomically.
     * Note that transactions can only be open for {@link EventWriterConfig#getTransactionTimeoutTime()}.
     * 
     * @return A transaction through which multiple events can be written atomically.
     * @deprecated Use {@link ClientFactory#createTransactionalEventWriter(String, Serializer, EventWriterConfig)} instead
     */
    @Deprecated
    Transaction<Type> beginTxn();

    /**
     * Returns a previously created transaction.
     * 
     * @param transactionId The result retained from calling {@link Transaction#getTxnId()}
     * @return Transaction object with given UUID
     * @deprecated Use {@link ClientFactory#createTransactionalEventWriter(String, Serializer, EventWriterConfig)} instead
     */
    @Deprecated
    Transaction<Type> getTxn(UUID transactionId);

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
