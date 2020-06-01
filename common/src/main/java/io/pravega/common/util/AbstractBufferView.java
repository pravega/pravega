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
import io.pravega.common.hash.HashHelper;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Base implementation of {@link BufferView}, providing common functionality.
 * Derived classes may override these methods with more efficient implementations tailored to their data types.
 */
public abstract class AbstractBufferView implements BufferView {
    static final BufferView EMPTY = new EmptyBufferView();
    private static final HashHelper HASH = HashHelper.seededWith(AbstractBufferView.class.getName());

    @Override
    public int hashCode() {
        HashHelper.HashBuilder builder = HASH.newBuilder();
        collect(builder::put);
        return builder.getAsInt();
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
    @SneakyThrows(IOException.class)
    public boolean equals(BufferView other) {
        int l = getLength();
        if (l != other.getLength()) {
            return false;
        }

        if (l > 0) {
            BufferView.Reader thisReader = getBufferViewReader();
            BufferView.Reader otherReader = other.getBufferViewReader();
            for (int i = 0; i < l; i++) {
                if (thisReader.readByte() != otherReader.readByte()) {
                    return false;
                }
            }
        }

        return true;
    }

    //region AbstractReader

    /**
     * Base implementation of {@link BufferView.Reader}. Subclasses of {@link AbstractBufferView} should implement their
     * own {@link BufferView.Reader} instances based on this class.
     */
    protected static abstract class AbstractReader implements BufferView.Reader {
        @Override
        public int readInt() throws EOFException {
            return BitConverter.makeInt(readByte(), readByte(), readByte(), readByte());
        }

        @Override
        public long readLong() throws EOFException {
            return BitConverter.makeLong(readByte(), readByte(), readByte(), readByte(), readByte(), readByte(), readByte(), readByte());
        }

        @Override
        public ArrayView readFully(int bufferSize) {
            Preconditions.checkArgument(bufferSize > 0, "bufferSize must be a positive integer.");
            ByteArraySegment readBuffer = new ByteArraySegment(new byte[available()]);
            int readOffset = 0;
            while (readOffset < readBuffer.getLength()) {
                int readLength = Math.min(available(), readBuffer.getLength() - readOffset);
                int readBytes = readBytes(readBuffer.slice(readOffset, Math.min(bufferSize, readLength)));
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
        public List<ByteBuffer> getContents() {
            return Collections.emptyList();
        }

        @Override
        public <ExceptionT extends Exception> void collect(Collector<ExceptionT> collectBuffer) {
            // This method intentionally left blank (nothing to do).
        }

        private static class EmptyInputStream extends InputStream {
            @Override
            public int read() throws IOException {
                return -1;
            }
        }

        private static class EmptyReader extends AbstractReader {
            @Override
            public int available() {
                return 0;
            }

            @Override
            public int readBytes(ByteArraySegment segment) {
                return 0;
            }

            @Override
            public int readByte() throws EOFException {
                throw new EOFException("Cannot read from Empty BufferView.");
            }

            @Override
            public BufferView readSlice(int length) throws EOFException {
                if (length == 0) {
                    return BufferView.empty();
                }

                throw new EOFException("Cannot read from Empty BufferView.");
            }
        }
    }

    //endregion
}
