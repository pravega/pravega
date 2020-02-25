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
import lombok.Data;

/**
 * Represents the state of a resumable iterator. Such an iterator can be executed asynchronously and continued after an
 * interruption. Each iteration will result in a new request to the server (which is stateless). The entire state of
 * the iterator is encoded in this object and is non-transferable between different types of iterations. The server will
 * use the information within it to decide what to return for the next iteration call.
 */
@Data
public class IteratorState {
    /**
     * No state. Providing this value will result in an iterator that starts from the beginning (i.e., not resuming an
     * existing iteration).
     */
    public static final IteratorState EMPTY = new IteratorState(Unpooled.EMPTY_BUFFER);

    private final ByteBuf token;

    /**
     * Gets a value indicating whether this {@link IteratorState} instance is empty.
     *
     * @return True if empty, false otherwise.
     */
    public boolean isEmpty() {
        return this.token.readableBytes() == 0;
    }

    /**
     * Deserializes the IteratorState from its serialized form obtained from calling {@link #getToken()} .
     *
     * @param serializedState A serialized {@link IteratorState}.
     * @return The IteratorState object.
     */
    public static IteratorState fromBytes(ByteBuf serializedState) {
        if (serializedState == null) {
            return EMPTY;
        } else {
            return new IteratorState(serializedState);
        }
    }
}
