/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Defines an extension to OutputStream that allows writing to an arbitrary position.
 */
public interface RandomAccessOutputStream {
    /**
     * Writes the given byte value at the given position.
     *
     * @param byteValue The value to write.
     * @param position  The position to write at.
     * @throws IOException               If an IO Exception occurred.
     * @throws IndexOutOfBoundsException If position is outside of the current bounds of this object.
     */
    void write(int byteValue, int position) throws IOException;

    /**
     * Writes a sequence of bytes at the given position.
     *
     * NOTE: depending on the implementation of this interface, this may result in increasing the size of the stream. For
     * example, if position is smaller than size() AND position + length is larger than size() then the extra bytes may
     * be appended at the end, if the underlying OutputStream's structure permits it.
     *
     * @param buffer       The buffer to write from.
     * @param bufferOffset The offset within the buffer to start at.
     * @param length       The number of bytes to write.
     * @param position     The position within the OutputStream to write at.
     * @throws IOException               If an IO Exception occurred.
     * @throws IndexOutOfBoundsException If bufferOffset and length are invalid for the given buffer or if position is
     *                                   invalid for the current OutputStream's state.
     */
    void write(byte[] buffer, int bufferOffset, int length, int position) throws IOException;

    /**
     * Creates a new fixed-size OutputStream starting at the given position in this OutputStream and of the given length.
     *
     * @param position The position to start at.
     * @param length   The length of the OutputStream.
     * @return A new OutputStream.
     * @throws IndexOutOfBoundsException If position or position + length are outside of the current bounds of this object.
     */
    OutputStream subStream(int position, int length);

    /**
     * Gets a value indicating the size of this OutputStream.
     *
     * @return size of stream
     */
    int size();
}
