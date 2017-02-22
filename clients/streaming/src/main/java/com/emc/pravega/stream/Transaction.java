/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

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
        SEALED,
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
     * @param event The Event to write.
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void writeEvent(Type event) throws TxnFailedException;
    
    /**
     * Sends an event to the stream just like {@link EventStreamWriter#writeEvent} but with the caveat that
     * the message will not be visible to anyone until {@link #commit()} is called.
     *
     * @param routingKey The Routing Key to use for writing.
     * @param event The Event to write.
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
