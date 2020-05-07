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

import java.nio.ByteBuffer;
import java.util.List;

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
     * Iterates through each of the arrays that make up this {@link CompositeArrayView}, in order, and invokes the given
     * {@link Collector} on each.
     *
     * @param collectArray A {@link Collector} function that will be invoked for each array component. The arguments to
     *                     this function represent the component array, start offset within the component array and number
     *                     of bytes within the array to process.
     * @param <ExceptionT> Type of exception that the {@link Collector} function throws, if any.
     * @throws ExceptionT If the {@link Collector} function throws an exception of this type, the iteration will end
     *                    and the exception will be bubbled up.
     */
    <ExceptionT extends Exception> void collect(Collector<ExceptionT> collectArray) throws ExceptionT;

    /**
     * {@inheritDoc}
     * Gets a list of {@link ByteBuffer} that represent the contents of this {@link CompositeArrayView}. Since the
     * {@link CompositeArrayView} is a sparse array implementation, any "gaps" that are not allocated within this object
     * will be returned as {@link ByteBuffer}s containing zeroes.
     *
     * @return A List of {@link ByteBuffer}.
     */
    @Override
    List<ByteBuffer> getContents();

    /**
     * Defines a collector function that can be applied to a range of an array.
     *
     * @param <ExceptionT> Type of exception that this function can throw.
     */
    @FunctionalInterface
    interface Collector<ExceptionT extends Exception> {
        /**
         * Processes an array range.
         *
         * @param array       The array.
         * @param arrayOffset The start offset within the array.
         * @param length      The number of bytes, beginning at startOffset, that need to be processed.
         * @throws ExceptionT (Optional) Any exception to throw.
         */
        void accept(byte[] array, int arrayOffset, int length) throws ExceptionT;
    }
}
