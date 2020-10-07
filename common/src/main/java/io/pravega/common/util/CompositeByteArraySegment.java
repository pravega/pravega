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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.common.Exceptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;

/**
 * A composite, index-based array-like structure that is made up of one or more individual arrays of equal size. Each
 * component array maps to a contiguous offset range and is only allocated when the first index within its range needs
 * to be set (if unallocated, any index within its range will have a value of 0).
 */
public class CompositeByteArraySegment implements CompositeArrayView {
    //region Members
    /**
     * Default component array size. 4KB maps to the kernel's page size.
     */
    private static final int DEFAULT_ARRAY_SIZE = 4 * 1024;
    /**
     * The offset at which the {@link CompositeByteArraySegment} begins, counted from the first block. This is helpful
     * for slicing a {@link CompositeByteArraySegment}. See {@link #slice}.
     */
    private final int startOffset;
    /**
     * Size of each component array.
     */
    private final int arraySize;
    private final Object[] arrays;
    @Getter
    private final int length;
    //endregion

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} class with a default component array size.
     *
     * @param length The length of the {@link CompositeByteArraySegment}. This will determine the number of components
     *               to use, but doesn't allocate any of them yet.
     */
    public CompositeByteArraySegment(int length) {
        this(length, DEFAULT_ARRAY_SIZE);
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} class with the given component array size.
     *
     * @param length    The length of the {@link CompositeByteArraySegment}. This will determine the number of components
     *                  to use, but doesn't allocate any of them yet.
     * @param arraySize The component array size.
     */
    public CompositeByteArraySegment(int length, int arraySize) {
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");
        Preconditions.checkArgument(arraySize > 0, "arraySize must be a positive number.");

        this.length = length;
        this.arraySize = Math.min(length, arraySize); // No point in allocating more memory if total length is smaller than arraySize.
        this.startOffset = 0;
        int count = length / arraySize + (length % arraySize == 0 ? 0 : 1);
        this.arrays = new Object[count];
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} class that wraps the given array. This instance
     * will have a single component array.
     *
     * @param source The byte array to wrap. Any changes made to this array will be reflected in this
     *               {@link CompositeByteArraySegment} instance and vice-versa.
     */
    @VisibleForTesting
    public CompositeByteArraySegment(@NonNull byte[] source) {
        this(new Object[]{source}, source.length, 0, source.length);
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} that uses the given arguments. Useful for slicing.
     *
     * @param arrays      The array components to use.
     * @param arraySize   Size of each individual component.
     * @param startOffset Start offset.
     * @param length      Length of {@link CompositeByteArraySegment}.
     */
    private CompositeByteArraySegment(Object[] arrays, int arraySize, int startOffset, int length) {
        this.arrays = arrays;
        this.arraySize = arraySize;
        this.startOffset = startOffset;
        this.length = length;
    }

    //region CompositeArrayView Implementation

    @Override
    public byte get(int offset) {
        byte[] array = getArray(getArrayId(offset), false); // No need to allocate array if not allocated yet.
        return array == null ? 0 : array[getArrayOffset(offset)];
    }

    @Override
    public void set(int offset, byte value) {
        byte[] array = getArray(getArrayId(offset), true); // Need to allocate array if not allocated yet.
        array[getArrayOffset(offset)] = value;
    }

    @Override
    public InputStream getReader() {
        // Use the collector to create a list of ByteArrayInputStreams and then return them as combined.
        ArrayList<ByteArrayInputStream> streams = new ArrayList<>();
        collect((array, offset, length) -> streams.add(new ByteArrayInputStream(array, offset, length)));
        return new SequenceInputStream(Iterators.asEnumeration(streams.iterator()));
    }

    @Override
    public InputStream getReader(int offset, int length) {
        return slice(offset, length).getReader();
    }

    @Override
    public CompositeArrayView slice(int offset, int length) {
        Exceptions.checkArrayRange(offset, length, this.length, "offset", "length");
        if (offset == 0 && length == this.length) {
            // Nothing to slice.
            return this;
        }

        return new CompositeByteArraySegment(this.arrays, this.arraySize, this.startOffset + offset, length);
    }

    @Override
    public <ExceptionT extends Exception> void collect(Collector<ExceptionT> collectArray) throws ExceptionT {
        collect(collectArray, this.length);
    }

    private <ExceptionT extends Exception> void collect(Collector<ExceptionT> collectArray, int length) throws ExceptionT {
        if (length == 0) {
            // Nothing to collect.
            return;
        }

        // We only need to process a subset of our arrays (since we may be sliced from the original array list).
        int startId = getArrayId(0);
        int endId = getArrayId(length - 1);

        int arrayOffset = getArrayOffset(0); // The first array may need an offset, if this.startOffset > 0.
        for (int arrayId = startId; arrayId <= endId; arrayId++) {
            int arrayLength = Math.min(length, this.arraySize - arrayOffset);
            byte[] array = getArray(arrayId, false); // Don't allocate array if not allocated yet.
            if (array == null) {
                // Providing a dummy, empty array of the correct size is the easiest way to handle unallocated components
                // for all the cases this method is used for.
                collectArray.accept(new byte[arrayLength], 0, arrayLength);
            } else {
                collectArray.accept(array, arrayOffset, arrayLength);
            }

            length -= arrayLength;
            arrayOffset = 0; // After processing the first array (handling this.startOffset), all other array offsets are 0.
        }

        assert length == 0 : "Collection finished but " + length + " bytes remaining";
    }

    @Override
    public byte[] getCopy() {
        byte[] result = new byte[this.length];
        copyTo(ByteBuffer.wrap(result));
        return result;
    }

    @Override
    public void copyFrom(ArrayView source, int targetOffset, int length) {
        Preconditions.checkArgument(length <= source.getLength(), "length exceeds source input's length.");
        Exceptions.checkArrayRange(targetOffset, length, this.length, "offset", "length");

        int sourceOffset = 0;
        int arrayOffset = getArrayOffset(targetOffset);
        int arrayId = getArrayId(targetOffset);
        while (length > 0) {
            byte[] array = getArray(arrayId, true); // Need to allocate if not already allocated.
            int copyLength = Math.min(array.length - arrayOffset, length);
            System.arraycopy(source.array(), source.arrayOffset() + sourceOffset, array, arrayOffset, copyLength);
            sourceOffset += copyLength;
            length -= copyLength;
            arrayId++;
            arrayOffset = 0;
        }

        assert length == 0 : "Copy finished but " + length + " bytes remaining";
    }

    @Override
    public void copyTo(OutputStream target) throws IOException {
        collect(target::write);
    }

    @Override
    public int copyTo(ByteBuffer target) {
        int length = Math.min(this.length, target.remaining());
        collect(target::put, length);
        return length;
    }

    @Override
    public List<ByteBuffer> getContents() {
        ArrayList<ByteBuffer> result = new ArrayList<>();
        for (int i = 0; i < this.arrays.length; i++) {
            byte[] a = getArray(i, false);
            if (a == null) {
                int size = i == this.arrays.length - 1 ? this.length % this.arraySize : this.arraySize;
                result.add(ByteBuffer.allocate(size));
            } else {
                result.add(ByteBuffer.wrap(a));
            }
        }
        return result;
    }

    //endregion

    //region Helpers

    /**
     * Gets the number of arrays allocated.
     *
     * @return The number of arrays allocated.
     */
    @VisibleForTesting
    int getAllocatedArrayCount() {
        return (int) Arrays.stream(this.arrays).filter(Objects::nonNull).count();
    }

    /**
     * Calculates the offset within an array that the given external offset maps to.
     * Use {@link #getArrayId} to identify which array the offset maps to and to validate offset is a valid offset within
     * this {@link #CompositeByteArraySegment}.
     *
     * @param offset The external offset to map.
     * @return The offset within a component array.
     */
    private int getArrayOffset(int offset) {
        return (this.startOffset + offset) % this.arraySize;
    }

    /**
     * Calculates the component array id (index within {@link #arrays} that the given external offset maps to.
     * Use {@link #getArrayOffset} to identify the offset within this array.
     *
     * @param offset The external offset to map.
     * @return The component array id.
     */
    private int getArrayId(int offset) {
        Preconditions.checkElementIndex(offset, this.length, "offset");
        return (this.startOffset + offset) / this.arraySize;
    }

    /**
     * Gets the component array with given id.
     *
     * @param arrayId  The array id (index within {@link #arrays} to return.
     * @param allocate If true, then the component array is allocated (if not already) before returning. If false, the
     *                 component array is not allocated.
     * @return The component array with given id. May return null if allocate == false.
     */
    private byte[] getArray(int arrayId, boolean allocate) {
        Object a = this.arrays[arrayId];
        if (a == null && allocate) {
            a = new byte[this.arraySize];
            this.arrays[arrayId] = a;
        }

        return (byte[]) a;
    }

    //endregion
}
