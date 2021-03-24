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

import java.util.UUID;

/**
 * Provides a mechanism for writing many events atomically.
 * A Transaction is unbounded in size but is bounded in time. If it has not been committed within a time window
 * specified  at the time of its creation it will be automatically aborted.
 * 
 * All methods on this class may block.
 *
 * @param <Type> The type of events in the associated stream.
 */
public interface Transaction<Type> {
    enum Status {
        OPEN,
        COMMITTING,
        COMMITTED,
        ABORTING,
        ABORTED
    }

    enum PingStatus {
        OPEN,
        COMMITTED,
        ABORTED
    }

    /**
     * Returns a unique ID that can be used to identify this transaction.
     *
     * @return Unique identifier of the transaction
     */
    UUID getTxnId();
    
    /**
     * Sends an event to the stream just like {@link EventStreamWriter#writeEvent} but with the caveat that
     * the message will not be visible to anyone until {@link #commit()} is called.
     *
     * A routing key will automatically be inferred from the transactionID.
     * So all events written this way will be fully ordered and contiguous when read.
     *
     * @param event The Event to write. (Null is disallowed)
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void writeEvent(Type event) throws TxnFailedException;
    
    /**
     * Sends an event to the stream just like {@link EventStreamWriter#writeEvent} but with the caveat that
     * the message will not be visible to anyone until {@link #commit()} is called.
     *
     * @param routingKey The Routing Key to use for writing.
     * @param event The Event to write. (Null is disallowed)
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void writeEvent(String routingKey, Type event) throws TxnFailedException;

    /**
     * Blocks until all events passed to {@link #writeEvent(String, Object)} make it to durable storage.
     * This is only needed if the transaction is going to be serialized.
     *
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void flush() throws TxnFailedException;

    /**
     * Causes all messages previously written to the transaction to go into the stream contiguously.
     * This operation will either fully succeed making all events consumable or fully fail such that none of them are.
     * There may be some time delay before readers see the events after this call has returned.
     *
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void commit() throws TxnFailedException;
    
    /**
     * Commits the transaction similar to {@link #commit()}, but also notes an associated timestamp.
     * Similar to {@link EventStreamWriter#noteTime(long)}.
     * 
     * @param timestamp A timestamp associated with this transaction.
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void commit(long timestamp) throws TxnFailedException;

    /**
     * Drops the transaction, causing all events written to it to be deleted.
     */
    void abort();

    /**
     * Gets the status of the transaction.
     *
     *  @return Current status of the transaction
     */
    Status checkStatus();
}
