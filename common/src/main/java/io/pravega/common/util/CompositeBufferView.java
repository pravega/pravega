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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;

/**
 * Provides a unified view of multiple wrapped {@link BufferView} instances.
 */
class CompositeBufferView implements BufferView {
    //region Members

    private final List<BufferView> components;
    @Getter
    private final int length;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link CompositeBufferView} class. It is recommended to use {@link BufferView#wrap}
     * instead.
     *
     * @param components The components to wrap.
     */
    CompositeBufferView(@NonNull List<BufferView> components) {
        this.components = new ArrayList<>();
        int length = 0;
        for (BufferView c : components) {
            if (c instanceof CompositeBufferView) {
                for (BufferView b : ((CompositeBufferView) c).components) {
                    this.components.add(b.slice());
                }
            } else {
                this.components.add(c.slice());
            }
            length += c.getLength();
        }
        this.length = length;
    }

    //endregion

    //region BufferView implementation

    @Override
    public InputStream getReader() {
        this.components.stream().map(BufferView::getReader).iterator();
        return new SequenceInputStream(Iterators.asEnumeration(this.components.stream().map(BufferView::getReader).iterator()));
    }

    @Override
    public InputStream getReader(int offset, int length) {
        return slice(offset, length).getReader();
    }

    @Override
    public BufferView slice(int offset, int length) {
        Preconditions.checkArgument(offset >= 0 && length >= 0 && offset + length <= this.length,
                "offset and length must be non-negative and less than %s. Given %s-%s", this.length, offset, length);
        if (offset == 0 && length == this.length) {
            return this;
        }
        ArrayList<BufferView> components = new ArrayList<>(this.components.size());
        int index = 0;
        int currentOffset = 0;
        while (length > 0) {
            BufferView c = this.components.get(index);
            int lastComponentOffset = currentOffset + c.getLength();
            if (offset < lastComponentOffset) {
                int sliceStart = Math.max(0, offset - currentOffset);
                int sliceLength = Math.min(length, c.getLength() - sliceStart);
                components.add(c.slice(sliceStart, sliceLength));
                length -= sliceLength;
            }

            index++;
            currentOffset += c.getLength();
        }

        return BufferView.wrap(components);
    }

    @Override
    public byte[] getCopy() {
        byte[] result = new byte[getLength()];
        int offset = 0;
        for (BufferView c : this.components) {
            int copiedBytes = c.copyTo(ByteBuffer.wrap(result, offset, c.getLength()));
            assert copiedBytes == c.getLength();
            offset += copiedBytes;
        }
        return result;
    }

    @Override
    public void copyTo(OutputStream target) throws IOException {
        for (BufferView c : this.components) {
            c.copyTo(target);
        }
    }

    @Override
    public int copyTo(ByteBuffer target) {
        int bytesCopied = 0;
        for (BufferView c : this.components) {
            bytesCopied += c.copyTo(target);
        }

        return bytesCopied;
    }

    @Override
    public List<ByteBuffer> getContents() {
        ArrayList<ByteBuffer> result = new ArrayList<>(this.components.size());
        for (BufferView c : this.components) {
            result.addAll(c.getContents());
        }

        return result;
    }

    @Override
    public void retain() {
        this.components.forEach(BufferView::retain);
    }

    @Override
    public void release() {
        this.components.forEach(BufferView::release);
    }

    //endregion
}
