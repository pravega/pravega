/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

import java.util.UUID;

/**
 * Provides a mechanism for publishing many events atomically.
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
     */
    UUID getTransactionId();
    
    /**
     * Sends an event to the stream just like {@link Producer#publish} but with the caveat that the message will not be
     * visible to anyone until {@link #commit()} is called.
     *
     * @param routingKey The Routing Key to use for publishing.
     * @param event      The Event to publish.
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void publish(String routingKey, Type event) throws TxnFailedException;

    /**
     * Blocks until all events passed to {@link #publish} make it to durable storage.
     * This is only needed if the transaction is going to be serialized.
     *
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void flush() throws TxnFailedException;

    /**
     * Causes all messages previously published to the transaction to go into the stream contiguously.
     * This operation will either fully succeed making all events consumable or fully fail such that none of them are.
     * There may be some time delay before consumers see the events after this call has returned.
     *
     * @throws TxnFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void commit() throws TxnFailedException;

    /**
     * Drops the transaction, causing all events published to it to be deleted.
     */
    void drop();

    /**
     * Gets the status of the transaction.
     */
    Status checkStatus();
}