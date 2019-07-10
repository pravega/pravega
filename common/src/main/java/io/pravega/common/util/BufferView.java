/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.base.Preconditions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Defines a generic read-only view of a readable memory buffer with a known length.
 * For array-backed Buffers, see {@link ArrayView}.
 */
public interface BufferView {
    /**
     * Gets a value representing the length of this {@link BufferView}.
     *
     * @return The length.
     */
    int getLength();

    /**
     * Creates an InputStream that can be used to read the contents of this {@link BufferView}. The InputStream returned
     * spans the entire {@link BufferView}.
     *
     * @return The InputStream.
     */
    InputStream getReader();

    /**
     * Returns a copy of the contents of this {@link BufferView}.
     *
     * @return A byte array with the same length as this ArrayView, containing a copy of the data within it.
     */
    byte[] getCopy();

    /**
     * Copies the contents of this {@link BufferView} to the given {@link OutputStream} using a 4KB copy buffer.
     *
     * @param target The {@link OutputStream} to write to.
     * @throws IOException If an exception occurred.
     */
    default void copyTo(OutputStream target) throws IOException {
        copyTo(target, 4096);
    }

    /**
     * Copies the contents of this {@link BufferView} to the given {@link OutputStream} using the given buffer size.
     *
     * @param target     The {@link OutputStream} to write to.
     * @param bufferSize The size of the copy buffer to use.
     * @throws IOException If an exception occurred.
     */
    default void copyTo(OutputStream target, int bufferSize) throws IOException {
        Preconditions.checkArgument(bufferSize > 0, "bufferSize must be a positive integer.");
        int bytesLeft = getLength();
        byte[] copyBuffer = new byte[Math.min(bytesLeft, bufferSize)];
        try (InputStream s = getReader()) {
            while (bytesLeft > 0) {
                int readCount = s.read(copyBuffer, 0, copyBuffer.length);
                if (readCount < 0) {
                    throw new EOFException();
                }

                target.write(copyBuffer, 0, readCount);
                bytesLeft -= readCount;
            }
        }
    }
}
