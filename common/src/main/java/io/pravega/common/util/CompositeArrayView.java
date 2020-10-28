/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

/**
 * Defines a generic view of a composite, index-based, array-like structure that is made up of one or more individual
 * arrays.
 */
public interface CompositeArrayView extends BufferView {
    /**
     * Gets the value at the specified index.
     *
     * @param index The index to query.
     * @return Byte indicating the value at the given index.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    byte get(int index);

    /**
     * Sets the value at the specified index.
     *
     * @param index The index to set the value at.
     * @param value The Byte value to set.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    void set(int index, byte value);

    /**
     * Copies a specified number of bytes from the given {@link BufferView.Reader} into this {@link CompositeArrayView}.
     *
     * @param reader       The {@link BufferView.Reader} to copy bytes from.
     * @param targetOffset The offset within this {@link CompositeArrayView} to start copying at.
     * @param length       The number of bytes to copy.
     * @throws ArrayIndexOutOfBoundsException If targetOffset or length are invalid.
     */
    void copyFrom(BufferView.Reader reader, int targetOffset, int length);

    /**
     * Creates a new {@link CompositeArrayView} that represents a sub-range of this {@link CompositeArrayView} instance.
     * The new instance will share the same backing part(s) as this one, so a change to one will be reflected in the other.
     *
     * @param offset The starting offset to begin the slice at.
     * @param length The sliced length.
     * @return A new {@link CompositeArrayView}.
     */
    @Override
    CompositeArrayView slice(int offset, int length);

    /**
     * Gets the number of components in this {@link CompositeArrayView} instance.
     *
     * @return The number of components. This is the exact number of argument invocations for {@link #collect(Collector)}.
     */
    int getComponentCount();
}
