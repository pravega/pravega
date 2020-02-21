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
import lombok.Getter;

public class CompositeByteArraySegment implements CompositeArrayView {
    private static final int DEFAULT_ARRAY_SIZE = 128 * 1024;
    private final int startOffset;
    private final int arraySize;
    private final Object[] arrays;
    @Getter
    private final int length;

    public CompositeByteArraySegment(int length) {
        this(length, DEFAULT_ARRAY_SIZE);
    }

    public CompositeByteArraySegment(int length, int arraySize) {
        Preconditions.checkArgument(length >= 0);
        Preconditions.checkArgument(arraySize > 0);

        this.length = length;
        this.arraySize = arraySize;
        int count = length / arraySize + (length % arraySize == 0 ? 0 : 1);
        this.startOffset = 0;
        this.arrays = new Object[count];
    }

    public CompositeByteArraySegment(byte[] source) {
        this(source.length);
        copyFrom(new ByteArraySegment(source), 0, getLength());
    }

    private CompositeByteArraySegment(Object[] arrays, int arraySize, int startOffset, int length) {
        this.arrays = arrays;
        this.arraySize = arraySize;
        this.startOffset = startOffset;
        this.length = length;
    }

    @Override
    public byte get(int offset) {
        byte[] array = getArray(getArrayId(offset), false);
        return array == null ? 0 : array[getArrayOffset(offset)];
    }

    @Override
    public void set(int offset, byte value) {
        byte[] array = getArray(getArrayId(offset), true);
        array[getArrayOffset(offset)] = value;
    }

    @Override
    public InputStream getReader() {
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
        if (offset == 0 && length == getLength()) {
            return this;
        }

        return new CompositeByteArraySegment(this.arrays, this.arraySize, this.startOffset + offset, length);
    }

    @Override
    public void collect(Collector collectArray) {
        if (this.length == 0) {
            return;
        }

        int startId = getArrayId(0);
        int endId = getArrayId(this.length - 1);

        int currentOffset = 0;
        int remainingLength = this.length;
        for (int i = startId; i <= endId; i++) {
            byte[] array = getArray(i, false);
            int arrayOffset = getArrayOffset(currentOffset);
            int arrayLength = Math.min(remainingLength, this.arraySize - arrayOffset);
            if (array == null) {
                collectArray.accept(new byte[arrayLength], 0, arrayLength);
            } else {
                collectArray.accept(array, arrayOffset, arrayLength);
            }

            remainingLength -= arrayLength;
            currentOffset += arrayLength;
        }
    }

    @Override
    public byte[] getCopy() {
        byte[] result = new byte[this.length];

        int startId = getArrayId(0);
        int endId = getArrayId(this.length - 1);

        int resultOffset = 0;
        for (int i = startId; i <= endId; i++) {
            byte[] array = getArray(i, false);
            int arrayOffset = getArrayOffset(resultOffset);
            int copyLength = Math.min(result.length - resultOffset, this.arraySize - arrayOffset);
            if (array != null) {
                System.arraycopy(array, arrayOffset, result, resultOffset, copyLength);
            }

            resultOffset += copyLength;
        }

        return result;
    }

    @Override
    public void copyFrom(ArrayView source, int targetOffset, int length) {
        int sourceOffset = 0;
        while (length > 0) {
            byte[] array = getArray(getArrayId(targetOffset), true);
            int arrayOffset = getArrayOffset(targetOffset);
            int copyLength = Math.min(array.length - arrayOffset, length);
            System.arraycopy(source.array(), source.arrayOffset() + sourceOffset, array, arrayOffset, copyLength);
            sourceOffset += copyLength;
            targetOffset += copyLength;
            length -= copyLength;
        }
    }

    @Override
    public void copyTo(OutputStream target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int copyTo(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException();
    }

    private int getArrayOffset(int offset) {
        return (this.startOffset + offset) % this.arraySize;
    }

    private int getArrayId(int offset) {
        Preconditions.checkElementIndex(offset, this.length, "offset");
        return (this.startOffset + offset) / this.arraySize;
    }

    private byte[] getArray(int arrayId, boolean allocate) {
        Object a = this.arrays[arrayId];
        if (a == null && allocate) {
            a = new byte[this.arraySize];
            this.arrays[arrayId] = a;
        }

        return (byte[]) a;
    }
}
