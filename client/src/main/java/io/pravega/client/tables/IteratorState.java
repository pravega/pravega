/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.impl.IteratorStateImpl;

/**
 * Defines the state of a resumable iterator. Such an iterator can be executed asynchronously and continued after an
 * interruption. Each iteration will result in a new request to the server (which is stateless). The entire state of
 * the iterator is encoded in this object and is non-transferable between different types of iterations. The server will
 * use the information within it to decide what to return for the next iteration call.
 */
public interface IteratorState {
    /**
     * No state. Providing this value will result in an iterator that starts from the beginning (i.e., not resuming an
     * existing iteration).
     */
    IteratorState EMPTY = new IteratorStateImpl(Unpooled.EMPTY_BUFFER);

    /**
     * Serializes the IteratorState instance to a compact buffer.
     *
     * @return byte representation..
     */
    ByteBuf toBytes();

    /**
     * Deserializes the IteratorState from its serialized form obtained from calling {@link #toBytes()}.
     *
     * @param serializedState A serialized IteratorState.
     * @return The IteratorState object.
     */
    static IteratorState fromBytes(ByteBuf serializedState) {
        if (serializedState == null) {
            return EMPTY;
        } else {
            return new IteratorStateImpl(serializedState);
        }
    }
}
