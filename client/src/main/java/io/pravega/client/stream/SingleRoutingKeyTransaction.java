/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

/**
 * Provides a mechanism for writing multiple events with the same routing key atomically. 
 * Users can create new singleRoutingKey transactions for a routing key. After writing events into the transaction
 * they can call commit on it which will atomically write all events in the stream. 
 * A SingleRoutingKey Transaction is bounded in both size and time. Maximum allowed serialized size for all the events 
 * in the transaction is 16mb. There is also a notion of time limit on the transaction - if the user does not issue a
 * commit on a transaction for the specified period then it is automatically aborted and all the events are discarded.
 * 
 * All methods on this class may block.
 *
 * @param <Type> The type of events in the associated stream.
 */
public interface SingleRoutingKeyTransaction<Type> {
    /**
     * Sends an event to the stream just like {@link EventStreamWriter#writeEvent} but with the caveat that
     * the message will not be visible to anyone until {@link #commit()} is called.
     *
     * So all events written this way will be fully ordered and contiguous when read.
     *
     * @param event The Event to write. (Null is disallowed)
     * @throws TxnFailedException thrown if the transaction has timed out. 
     */
    void writeEvent(Type event) throws TxnFailedException;
    
    /**
     * Causes all messages previously written to the transaction to go into the stream contiguously.
     * This operation will either fully succeed making all events consumable or fully fail such that none of them are.
     * Once this call returns, the readers are guaranteed to find the events in the transactions to be available for reads. 
     *
     * @throws TxnFailedException thrown if the transaction is timed out. 
     */
    void commit() throws TxnFailedException;
    
    /**
     * Drops the transaction, causing all events written to it to be discarded.
     */
    void abort();
}
