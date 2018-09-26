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

import com.google.common.annotations.Beta;
import java.nio.ByteBuffer;

/**
 * Defines the state of a resumable iterator.
 */
@Beta
public interface IteratorState {

    /**
     * Serializes the IteratorState instance to a compact byte array.
     */
    ByteBuffer toBytes();

    /**
     * Deserializes the IteratorState from its serialized from obtained from calling {@link #toBytes()}.
     *
     * @param serializedState A serialized IteratorState.
     * @return The IteratorState object.
     */
    static IteratorState fromBytes(ByteBuffer serializedState) {
        throw new UnsupportedOperationException("Not Implemented");
    }
}
