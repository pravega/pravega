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

import io.netty.buffer.Unpooled;
import io.pravega.client.tables.impl.IteratorStateImpl;
import java.nio.ByteBuffer;

/**
 * Represents the state of a resumable iterator. Such an iterator can be executed asynchronously and continued after an
 * interruption. Each iteration will result in a new request to the server (which is stateless). The entire state of
 * the iterator is encoded in this object and is non-transferable between different types of iterations. The server will
 * use the information within it to decide what to return for the next iteration call.
 */
public interface IteratorState {
    /**
     * Serializes this {@link IteratorState} to a {@link ByteBuffer}. This can later be used to create a new
     * {@link IteratorState} using {@link #fromBytes(ByteBuffer)}.
     *
     * @return A {@link ByteBuffer}.
     */
    ByteBuffer toBytes();

    /**
     * Gets a value indicating whether this {@link IteratorState} instance is empty.
     *
     * @return True if empty, false otherwise.
     */
    boolean isEmpty();

    /**
     * Creates a new {@link IteratorState} from the given {@link ByteBuffer}.
     *
     * @param buffer A {@link ByteBuffer} created via {@link #toBytes()}.
     * @return A {@link IteratorState}.
     */
    static IteratorState fromBytes(ByteBuffer buffer) {
        return buffer == null ? IteratorStateImpl.EMPTY : IteratorStateImpl.fromBytes(Unpooled.wrappedBuffer(buffer));
    }
}
