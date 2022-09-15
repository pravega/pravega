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
package io.pravega.common.util;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import lombok.Getter;

/**
 * Base implementation of {@link BufferView}, providing common functionality.
 * Derived classes may override these methods with more efficient implementations tailored to their data types.
 */
public abstract class AbstractBufferView implements BufferView {
    static final BufferView EMPTY = new EmptyBufferView();


    @Override
    public int hashCode() {
        return (int) hash();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BufferView) {
            return equals((BufferView) obj);
        }
        return false;
    }

    /**
     * Checks for equality against another {@link BufferView} instance.
     *
     * Note: this method provides a general implementation of equality. Derived classes should override this method with
     * more efficient implementations if available.
     *
     * @param other The other {@link BufferView} instance.
     * @return True if this instance and the other instance have the same contents.
     */
    public boolean equals(BufferView other) {
        int l = getLength();
        if (l != other.getLength()) {
            return false;
        } else if (l == 0) {
            return true;
        }

        Iterator<ByteBuffer> i1 = this.iterateBuffers();
        Iterator<ByteBuffer> i2 = other.iterateBuffers();

        ByteBuffer b1 = i1.next();
        ByteBuffer b2 = i2.next();
        while (b1 != null && b2 != null) {
            int lengthDiff = b1.remaining() - b2.remaining();
            boolean equals;
            if (lengthDiff == 0) {
                equals = b1.equals(b2);
                b1 = null;
                b2 = null;
            } else if (lengthDiff < 0) {
                equals = b1.equals(ByteBufferUtils.slice(b2, b2.position(), b1.remaining()));
                b1 = null;
                b2.position(b2.limit() + lengthDiff);
            } else {
                equals = b2.equals(ByteBufferUtils.slice(b1, b1.position(), b2.remaining()));
                b1.position(b1.limit() - lengthDiff);
                b2 = null;
            }

            if (!equals) {
                return false;
            }

            if (b1 == null && i1.hasNext()) {
                b1 = i1.next();
            }

            if (b2 == null && i2.hasNext()) {
                b2 = i2.next();
            }
        }

        assert b1 == null && b2 == null;
        return true;
    }

    //region AbstractReader

    /**
     * Base implementation of {@link BufferView.Reader}. Subclasses of {@link AbstractBufferView} should implement their
     * own {@link BufferView.Reader} instances based on this class.
     */
    protected static abstract class AbstractReader implements BufferView.Reader {
        /**
         * {@inheritDoc}
         * Default implementation for {@link BufferView.Reader#readInt()}. Derived classes should make every effort to
         * override this implementation with one that is as efficient as possible (if the {@link BufferView}
         * implementation allows it).
         *
         * @return The read int.
         * @throws BufferView.Reader.OutOfBoundsException If {@link #available()} is less than {@link Integer#BYTES}.
         */
        @Override
        public int readInt() {
            return BitConverter.makeInt(readByte(), readByte(), readByte(), readByte());
        }

        /**
         * {@inheritDoc}
         * Default implementation for {@link BufferView.Reader#readLong()}. Derived classes should make every effort to
         * override this implementation with one that is as efficient as possible (if the {@link BufferView}
         * implementation allows it).
         *
         * @return The read int.
         * @throws BufferView.Reader.OutOfBoundsException If {@link #available()} is less than {@link Long#BYTES}.
         */
        @Override
        public long readLong() {
            return BitConverter.makeLong(readByte(), readByte(), readByte(), readByte(), readByte(), readByte(), readByte(), readByte());
        }

        @Override
        public ArrayView readFully(int bufferSize) {
            Preconditions.checkArgument(bufferSize > 0, "bufferSize must be a positive integer.");
            ByteArraySegment readBuffer = new ByteArraySegment(new byte[available()]);
            int readOffset = 0;
            while (readOffset < readBuffer.getLength()) {
                int readLength = Math.min(available(), readBuffer.getLength() - readOffset);
                int readBytes = readBytes(readBuffer.slice(readOffset, Math.min(bufferSize, readLength)).asByteBuffer());
                readOffset += readBytes;
            }
            assert available() == 0;
            return readBuffer;
        }
    }

    //endregion

    //region EmptyBufferView

    /**
     * Simulates an empty {@link BufferView}.
     */
    private static class EmptyBufferView extends AbstractBufferView {
        @Getter
        private final Reader bufferViewReader = new EmptyReader();
        @Getter
        private final InputStream reader = new EmptyInputStream();

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public int getAllocatedLength() {
            return 0;
        }

        @Override
        public InputStream getReader(int offset, int length) {
            return slice(offset, length).getReader();
        }

        @Override
        public BufferView slice(int offset, int length) {
            if (offset != 0 || length != 0) {
                throw new IndexOutOfBoundsException("Cannot slice empty BufferView.");
            }

            return this;
        }

        @Override
        public byte[] getCopy() {
            return new byte[0];
        }

        @Override
        public void copyTo(OutputStream target) {
            // This method intentionally left blank  (nothing to do).
        }

        @Override
        public int copyTo(ByteBuffer byteBuffer) {
            return 0; // Nothing to copy.
        }

        @Override
        public <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) {
            // This method intentionally left blank (nothing to do).
        }

        @Override
        public Iterator<ByteBuffer> iterateBuffers() {
            return Collections.emptyIterator();
        }

        private static class EmptyInputStream extends InputStream {
            @Override
            public int read() throws IOException {
                return -1;
            }
        }

        private static final class EmptyReader extends AbstractReader {
            @Override
            public int available() {
                return 0;
            }

            @Override
            public int readBytes(ByteBuffer byteBuffer) {
                return 0;
            }

            @Override
            public byte readByte() {
                throw new OutOfBoundsException("Cannot read from Empty BufferView.");
            }

            @Override
            public BufferView readSlice(int length) {
                if (length == 0) {
                    return BufferView.empty();
                }

                throw new OutOfBoundsException("Cannot read from Empty BufferView.");
            }
        }
    }

    //endregion
}
