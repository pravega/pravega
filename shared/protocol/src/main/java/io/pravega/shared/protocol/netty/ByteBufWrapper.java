/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * {@link BufferView} wrapper for {@link ByteBuf} instances.
 */
@NotThreadSafe
public class ByteBufWrapper implements BufferView {
    //region Members

    private final ByteBuf buf;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link ByteBufWrapper} wrapping the given {@link ByteBuf}.
     *
     * This instance will begin at the given buffer's {@link ByteBuf#readerIndex()} and will have a length equal to
     * {@link ByteBuf#readableBytes()}.
     *
     * @param buf The {@link ByteBuf} to wrap. A read-only duplicate will be made of this buffer; any changes made to the
     *            {@link ByteBuf#readerIndex()} or {@link ByteBuf#writerIndex()} to this object will not be reflected
     *            in this {@link ByteBufWrapper} instance. This {@link ByteBuf} reference count will be incremented by 1
     *            to reflect the new reference added by this wrapper. Invoke {@link #release()} to release that reference.
     */
    public ByteBufWrapper(@NonNull ByteBuf buf) {
        this.buf = buf.asReadOnly();
    }

    //endregion

    //region BufferView implementation

    /**
     * Invokes {@link ByteBuf#retain()} on the underlying buffer.
     */
    @Override
    public void retain() {
        this.buf.retain();
    }

    /**
     * Invokes {@link ByteBuf#release()} on the underlying buffer, but only if {@link ByteBuf#refCnt()} is non-zero.
     * As opposed from {@link ByteBuf#release()}, this method will not throw if invoked too many times.
     */
    @Override
    public void release() {
        if (this.buf.refCnt() > 0) {
            this.buf.release();
        }
    }

    @Override
    public List<ByteBuffer> getContents() {
        return Arrays.asList(this.buf.nioBuffers());
    }

    @Override
    public int getLength() {
        return this.buf.readableBytes();
    }

    @Override
    public Reader getBufferViewReader() {
        return new ByteBufReader(this.buf.duplicate());
    }

    @Override
    public InputStream getReader() {
        Exceptions.checkNotClosed(this.buf.refCnt() == 0, this);
        return new ByteBufInputStream(this.buf.duplicate(), false);
    }

    @Override
    public InputStream getReader(int offset, int length) {
        Exceptions.checkNotClosed(this.buf.refCnt() == 0, this);
        return new ByteBufInputStream(this.buf.slice(offset, length), false);
    }

    @Override
    public BufferView slice(int offset, int length) {
        Exceptions.checkNotClosed(this.buf.refCnt() == 0, this);
        return new ByteBufWrapper(this.buf.slice(offset, length));
    }

    @Override
    public byte[] getCopy() {
        Exceptions.checkNotClosed(this.buf.refCnt() == 0, this);
        ByteBuf buf = this.buf.duplicate();
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }

    @Override
    public void copyTo(OutputStream target) throws IOException {
        Exceptions.checkNotClosed(this.buf.refCnt() == 0, this);
        ByteBuf buf = this.buf.duplicate();
        buf.readBytes(target, buf.readableBytes());
    }

    @Override
    public int copyTo(ByteBuffer byteBuffer) {
        Exceptions.checkNotClosed(this.buf.refCnt() == 0, this);
        ByteBuf source = this.buf.duplicate();
        int length = byteBuffer.remaining();
        if (length > getLength()) {
            int origLimit = byteBuffer.limit();
            length = getLength();
            byteBuffer.limit(length);
            source.readBytes(byteBuffer);
            byteBuffer.limit(origLimit);
        } else {
            source.readBytes(byteBuffer);
        }
        return length;
    }

    @Override
    public String toString() {
        return this.buf.toString();
    }

    //endregion

    //region Reader Implementation

    /**
     * {@link BufferView.Reader} implementation.
     */
    @RequiredArgsConstructor
    private static class ByteBufReader implements Reader {
        private final ByteBuf buf;

        @Override
        public int available() {
            return this.buf.readableBytes();
        }

        @Override
        public int readBytes(ByteArraySegment segment) {
            int len = Math.min(segment.getLength(), this.buf.readableBytes());
            if (len > 0) {
                this.buf.readBytes(segment.array(), segment.arrayOffset(), len);
            }
            return len;
        }
    }

    //endregion
}
