/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CompositeByteArraySegment;
import java.io.OutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * An {@link OutputStream} that grows without making copies of data.
 */
@NotThreadSafe
public class CompositeByteArrayOutputStream extends OutputStream {
    private final CompositeByteArraySegment data;
    @Getter
    private int size;

    /**
     * Creates a new instance of the {@link CompositeByteArrayOutputStream} class.
     *
     * @param maxSize   The maximum size for this instance.
     * @param blockSize The block size. Each time the {@link CompositeByteArrayOutputStream} needs to grow, it will
     *                  allocate a new array of this size and add it to the end.
     */
    public CompositeByteArrayOutputStream(int maxSize, int blockSize) {
        this.data = new CompositeByteArraySegment(maxSize, blockSize);
        this.size = 0;
    }

    @Override
    public void write(int i) {
        this.data.set(this.size, (byte) i);
        this.size++;
    }

    @Override
    public void write(byte[] buffer, int bufferOffset, int length) {
        ByteArraySegment b = new ByteArraySegment(buffer, bufferOffset, length);
        this.data.copyFrom(b.getBufferViewReader(), this.size, b.getLength());
        this.size += b.getLength();
    }

    /**
     * Creates a new {@link OutputStream} that shares the same underlying data as this instance, which begins at the
     * current position in the {@link CompositeByteArrayOutputStream}. Writing to this {@link Slice} will be reflected
     * in the {@link CompositeByteArrayOutputStream} instance that owns it (both data and {@link #size} increase).
     * <p>
     * This method (as well as this class) is not thread safe. Working with multiple {@link Slice} instances at the same
     * time that originate from the same {@link CompositeByteArrayOutputStream} will result in data corruption. It is up
     * to the caller to ensure that a {@link Slice} instance is completely done with before invoking this method again.
     *
     * @return A {@link Slice} beginning at the current position.
     */
    public Slice slice() {
        return new Slice(this.size);
    }

    /**
     * A {@link CompositeByteArrayOutputStream} slice.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public class Slice extends OutputStream {
        /**
         * The beginning position in {@link #data}.
         */
        private final int firstOffset;
        /**
         * Number of bytes in this slice.
         */
        private int size;

        /**
         * Discards all data written via this {@link Slice} and rolls back the {@link CompositeByteArrayOutputStream#size}
         * by the number of bytes written in this {@link Slice}.
         */
        public void reset() {
            CompositeByteArrayOutputStream.this.size -= this.size;
            this.size = 0;
        }

        /**
         * Returns this {@link CompositeByteArrayOutputStream.Slice} as a {@link BufferView}.
         *
         * @return A {@link BufferView} that spans the range of this {@link Slice}.
         */
        public BufferView asBufferView() {
            return CompositeByteArrayOutputStream.this.data.slice(this.firstOffset, this.size);
        }

        @Override
        public void write(int i) {
            CompositeByteArrayOutputStream.this.write(i);
            this.size++;
        }

        @Override
        public void write(byte[] buffer, int bufferOffset, int length) {
            CompositeByteArrayOutputStream.this.write(buffer, bufferOffset, length);
            this.size += length;
        }
    }
}
