/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.TxnFailedException;

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