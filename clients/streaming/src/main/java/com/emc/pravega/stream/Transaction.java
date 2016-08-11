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

import java.io.Serializable;

/**
 * Provides a mechanism for publishing many events atomically.
 * A Transaction is unbounded in size but is bounded in time. If it has not been committed within a time window
 * specified
 * at the time of its creation it will be automatically dropped.
 * 
 * Note that transactions are serializable (to a small reference). So they can be stored in an external store or passed
 * between processes if desired.
 * 
 * All methods on this class may block.
 * 
 * @param <Type> The type of events in the associated stream.
 */
public interface Transaction<Type> extends Serializable {
    enum Status {
        COMMITTED,
        OPEN,
        DROPPED
    }

    /**
     * Send an event to the stream just like {@link Producer#publish} but with the caveat that the message will not be
     * visible to anyone until {@link #commit()} is called.
     */
    void publish(String routingKey, Type event) throws TxFailedException;

    /**
     * Block until all events passed to {@link #publish} make it to durable storage.
     * This is only needed if the transaction is going to be serialized. 
     * 
     * @throws TxFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void flush() throws TxFailedException;
    
    /**
     * Cause all messages previously published to the transaction to go into the stream contiguously. 
     * This operation will either fully succeed making all events consumable or fully fail such that none of them are. 
     * There may be some time delay before consumers see the events after this call has returned.
     *  
     * @throws TxFailedException The Transaction is no longer in state {@link Status#OPEN}
     */
    void commit() throws TxFailedException;

    /**
     * Drop the transaction, causing all events published to it to be deleted.
     */
    void drop();

    /**
     * @return The status of the transaction. 
     */
    Status checkStatus();
}