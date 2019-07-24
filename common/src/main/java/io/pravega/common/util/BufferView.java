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
     * Creates an InputStream that can be used to read the contents of this {@link BufferView}.
     *
     * @param offset The starting offset of the section to read.
     * @param length The length of the section to read.
     * @return The InputStream.
     */
    InputStream getReader(int offset, int length);

    /**
     * Creates a new {@link BufferView} that represents a sub-range of this {@link BufferView} instance. The new instance
     * will share the same backing buffer as this one, so a change to one will be reflected in the other.
     *
     * @param offset The starting offset to begin the slice at.
     * @param length The sliced length.
     * @return A new {@link BufferView}.
     */
    BufferView slice(int offset, int length);

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
    void copyTo(OutputStream target) throws IOException;

    /**
     * When implemented in a derived class, notifies any wrapped buffer that this {@link BufferView} has a need for it.
     * Use {@link #release()} to do the opposite.
     */
    default void retain() {
        // Default implementation intentionally left blank. Any derived class may implement if needed.
    }

    /**
     * When implemented in a derived class, notifies any wrapped buffer that this {@link BufferView} no longer has a
     * need for it.
     */
    default void release() {
        // Default implementation intentionally left blank. Any derived class may implement if needed.
    }
}
