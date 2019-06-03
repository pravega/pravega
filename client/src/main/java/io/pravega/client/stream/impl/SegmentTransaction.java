/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.stream.TxnFailedException;

import java.util.UUID;

/**
 * The mirror of Transaction but that is specific to one segment.
 */
public interface SegmentTransaction<Type> extends AutoCloseable {
    UUID getId();

    /**
     * Writes the provided event to this transaction on this segment. This operation is asyncronus, the item is not
     * Guaranteed to be stored until after {@link #flush()} has been called.
     *
     * @param event The event to write.
     * @throws TxnFailedException The item could be persisted because the transaction has failed. (Timed out or aborted)
     */
    void writeEvent(Type event) throws TxnFailedException;

    /**
     * Blocks until all events passed to the write call have made it to durable storage.
     * After this the transaction can be committed.
     *
     * @throws TxnFailedException Not all of the items could be persisted because the transaction has failed. (Timed out or aborted)
     */
    void flush() throws TxnFailedException;
    
    /**
     * Calls {@link #flush()} and then closes the connection.
     */
    @Override 
    void close() throws TxnFailedException;
    
}