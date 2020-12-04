/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import io.pravega.common.util.BufferView;
import java.io.IOException;

/**
 * Defines an object meant for serializing data (usually an {@link java.io.OutputStream}) that can write various inputs
 * more efficiently than otherwise (i.e., byte-by-byte copy).
 */
public interface DirectDataOutput {
    /**
     * Includes the given {@link BufferView}.
     *
     * @param buffer The {@link BufferView} to include.
     * @throws IOException If an IO Exception occurred.
     */
    void writeBuffer(BufferView buffer) throws IOException;

    /**
     * Writes the given value as a 16 bit Short. Every effort will be made to write this using efficient techniques
     * (such as making use of intrinsic instructions).
     *
     * @param shortValue The Short value to write.
     * @throws IOException If an IO Exception occurred.
     */
    void writeShort(int shortValue) throws IOException;

    /**
     * Writes the given value as a 32 bit Integer. Every effort will be made to write this using efficient techniques
     * (such as making use of intrinsic instructions).
     *
     * @param intValue The Integer value to write.
     * @throws IOException If an IO Exception occurred.
     */
    void writeInt(int intValue) throws IOException;

    /**
     * Writes the given value as a 64 bit Long. Every effort will be made to write this using efficient techniques
     * (such as making use of intrinsic instructions).
     *
     * @param longValue The Long value to write.
     * @throws IOException If an IO Exception occurred.
     */
    void writeLong(long longValue) throws IOException;
}
