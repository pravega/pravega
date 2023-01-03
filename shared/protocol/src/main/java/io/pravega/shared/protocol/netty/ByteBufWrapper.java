/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.protocol.netty;

import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AbstractBufferView;
import io.pravega.common.util.BufferView;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * {@link BufferView} wrapper for {@link ByteBuf} instances.
 */
@NotThreadSafe
public class ByteBufWrapper extends AbstractBufferView implements BufferView {
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
    public <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) throws ExceptionT {
        for (ByteBuffer bb : this.buf.duplicate().nioBuffers()) {
            bufferCollector.accept(bb);
        }
    }

    @Override
    public Iterator<ByteBuffer> iterateBuffers() {
        ByteBuf bb = this.buf.duplicate();
        if (bb instanceof CompositeByteBuf) {
            return Iterators.transform(((CompositeByteBuf) bb).iterator(), ByteBuf::nioBuffer);
        } else if (bb.nioBufferCount() == 1) {
            return Iterators.singletonIterator(bb.nioBuffer());
        }
        return Iterators.forArray(bb.nioBuffers());
    }

    @Override
    public int getLength() {
        return this.buf.readableBytes();
    }

    @Override
    public int getAllocatedLength() {
        // TODO: This won't give us what we want. Unfortunately ByteBuf does not expose this information and there's no way to get it otherwise.
        return this.buf.capacity();
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
            // ByteBuffer has more capacity than we need to write. We need to adjust its limit() to exactly what we need,
            // otherwise ByteBuf.readBytes() won't copy what we need to.

            // Remember the original limit, then adjust it to what we need to copy. Since we copy less than its remaining
            // capacity, we are guaranteed not to overflow it when setting the new limit.
            int origLimit = byteBuffer.limit();
            length = getLength();
            byteBuffer.limit(byteBuffer.position() + length);
            source.readBytes(byteBuffer);
            byteBuffer.limit(origLimit); // Restore original ByteBuffer limit.
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
    private static final class ByteBufReader extends AbstractReader implements Reader {
        private final ByteBuf buf;

        @Override
        public int available() {
            return this.buf.readableBytes();
        }

        @Override
        public int readBytes(ByteBuffer byteBuffer) {
            // We need to adjust its limit() to exactly what we need, otherwise ByteBuf.readBytes() won't copy what we need to.
            int length = Math.min(byteBuffer.remaining(), this.buf.readableBytes());
            int origLimit = byteBuffer.limit();
            byteBuffer.limit(byteBuffer.position() + length);
            this.buf.readBytes(byteBuffer);
            byteBuffer.limit(origLimit); // Restore original ByteBuffer limit.
            return length;
        }

        @Override
        public byte readByte() {
            try {
                return this.buf.readByte();
            } catch (IndexOutOfBoundsException ex) {
                throw new OutOfBoundsException();
            }
        }

        @Override
        public int readInt() {
            try {
                return this.buf.readInt();
            } catch (IndexOutOfBoundsException ex) {
                throw new OutOfBoundsException();
            }
        }

        @Override
        public long readLong() {
            try {
                return this.buf.readLong();
            } catch (IndexOutOfBoundsException ex) {
                throw new OutOfBoundsException();
            }
        }

        @Override
        public BufferView readSlice(int length) {
            try {
                return new ByteBufWrapper(this.buf.readSlice(length));
            } catch (IndexOutOfBoundsException ex) {
                throw new OutOfBoundsException();
            }
        }
    }

    //endregion
}
