/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Defines the state of a resumable iterator.
 */
public interface IteratorState {

    IteratorState EMPTY = new IteratorStateImpl(Unpooled.EMPTY_BUFFER);

    /**
     * Serializes the IteratorState instance to a compact byte array.
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
