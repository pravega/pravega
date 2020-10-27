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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import lombok.Getter;

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
        public boolean readBoolean() {
            return readByte() != 0;
        }
        
        /**
         * {@inheritDoc}
         * Default implementation for {@link BufferView.Reader#readShort()}. Derived classes should make every effort to
         * override this implementation with one that is as efficient as possible (if the {@link BufferView}
         * implementation allows it).
         *
         * @return The read short.
         * @throws BufferView.Reader.OutOfBoundsException If {@link #available()} is less than {@link Short#BYTES}.
         */
        @Override
        public short readShort() {
            return BitConverter.makeShort(readByte(), readByte());
        }
        
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
        public char readChar() {
            return (char) readShort();
        }
        
        @Override
        public float readFloat() {
            return Float.intBitsToFloat(readInt());
        }
        
        @Override
        public double readDouble() {
            return Double.longBitsToDouble(readLong());
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
        

        /**
         * This is copied (with minor modifications for accessors) from DataInputStream.
         */
        @Override
        public String readUTF() throws UTFDataFormatException {
            int utflen = readShort() & 0x0000FFFF;
            char[] chararr = new char[2 * utflen];

            int c, char2, char3;
            int count = 0;
            int chararr_count=0;

            ArrayView byteview = readFully(utflen);
            count += byteview.arrayOffset();
            utflen += byteview.arrayOffset();
            byte[] bytearr = byteview.array();

            while (count < utflen) {
                c = (int) bytearr[count] & 0xff;
                if (c > 127) break;
                count++;
                chararr[chararr_count++]=(char)c;
            }

            while (count < utflen) {
                c = (int) bytearr[count] & 0xff;
                switch (c >> 4) {
                    case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                        /* 0xxxxxxx*/
                        count++;
                        chararr[chararr_count++]=(char)c;
                        break;
                    case 12: case 13:
                        /* 110x xxxx   10xx xxxx*/
                        count += 2;
                        if (count > utflen)
                            throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                        char2 = (int) bytearr[count-1];
                        if ((char2 & 0xC0) != 0x80)
                            throw new UTFDataFormatException(
                                "malformed input around byte " + count);
                        chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
                                                        (char2 & 0x3F));
                        break;
                    case 14:
                        /* 1110 xxxx  10xx xxxx  10xx xxxx */
                        count += 3;
                        if (count > utflen)
                            throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                        char2 = (int) bytearr[count-2];
                        char3 = (int) bytearr[count-1];
                        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                            throw new UTFDataFormatException(
                                "malformed input around byte " + (count-1));
                        chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                                                        ((char2 & 0x3F) << 6)  |
                                                        ((char3 & 0x3F) << 0));
                        break;
                    default:
                        /* 10xx xxxx,  1111 xxxx */
                        throw new UTFDataFormatException(
                            "malformed input around byte " + count);
                }
            }
            // The number of chars produced may be less than utflen
            return new String(chararr, 0, chararr_count);
        }
    }

    //endregion

    //region EmptyBufferView

    /**
     * Simulates an empty {@link BufferView}.
     */
    static class EmptyBufferView extends AbstractBufferView implements ArrayView {
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
        public EmptyBufferView slice(int offset, int length) {
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
        public <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) {
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

            @Override
            public String readUTF() throws UTFDataFormatException {
                throw new OutOfBoundsException("Cannot read from Empty BufferView.");
            }

            @Override
            public void skipBytes(int num) {
                if (num != 0) {
                    throw new OutOfBoundsException("Cannot skip in an Empty BufferView.");
                }
            }
        }

        @Override
        public byte get(int index) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public byte[] array() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int arrayOffset() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void copyTo(byte[] target, int targetOffset, int length) {
            // TODO Auto-generated method stub
            
        }
    }

    //endregion
}
