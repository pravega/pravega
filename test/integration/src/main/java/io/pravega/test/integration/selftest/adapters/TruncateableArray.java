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
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ArrayView;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents an ArrayView that can append at one end and truncate at the other one.
 */
@NotThreadSafe
public class TruncateableArray implements ArrayView {
    //region Members

    /**
     * We copy the data in fixed sized blocks (of 1MB); this makes lookups a lot faster.
     */
    private static final int BLOCK_SIZE = 1024 * 1024;
    private final ArrayDeque<byte[]> arrays;
    private int firstArrayOffset;
    private int length;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TruncateableArray class.
     */
    TruncateableArray() {
        this.arrays = new ArrayDeque<>();
        this.firstArrayOffset = 0;
        this.length = 0;
    }

    //endregion

    @Override
    public int hashCode() {
        return (int) hash();
    }

    /**
     * Because slicing is not supported equals is based on equality.
     */
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    //region ArrayView Implementation
    
    @Override
    public byte get(int index) {
        Preconditions.checkElementIndex(index, this.length, "index must be non-negative and less than the length of the array.");

        // Adjust the index based on the first entry offset.
        index += this.firstArrayOffset;

        // Find the array which contains the sought index.
        for (byte[] array : this.arrays) {
            if (index < array.length) {
                // This is the array we are looking for; use 'index' to look up into it.
                return array[index];
            }

            index -= array.length;
        }

        throw new AssertionError("unable to locate an inner array based on given input");
    }

    @Override
    public short getShort(int index) {
        throw new UnsupportedOperationException("getShort() not supported.");
    }

    @Override
    public int getInt(int index) {
        throw new UnsupportedOperationException("getInt() not supported.");
    }

    @Override
    public long getLong(int index) {
        throw new UnsupportedOperationException("getLong() not supported.");
    }

    @Override
    public int getLength() {
        return this.length;
    }

    @Override
    public int getAllocatedLength() {
        return this.length;
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException("array() not supported.");
    }

    @Override
    public int arrayOffset() {
        throw new UnsupportedOperationException("arrayOffset() not supported.");
    }

    @Override
    public InputStream getReader() {
        return getReader(0, this.length);
    }

    @Override
    public InputStream getReader(int offset, int length) {
        Preconditions.checkArgument(offset >= 0, "offset must be a non-negative number.");
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");
        Preconditions.checkArgument(offset + length <= this.length, "offset+length must be non-negative and less than or equal to the length of the array.");
        ArrayList<ByteArrayInputStream> streams = new ArrayList<>();

        // Adjust the index based on the first entry offset.
        offset += this.firstArrayOffset;

        // Find the array which contains the starting offset.
        for (byte[] array : this.arrays) {
            if (offset >= array.length) {
                // Not interested in this array
                offset -= array.length;
                continue;
            }

            // Figure out how much of this array we need to extract.
            int arrayLength = Math.min(length, array.length - offset);
            streams.add(new ByteArrayInputStream(array, offset, arrayLength));
            offset = 0;

            // Reduce the requested length by the amount of data we copied.
            length -= arrayLength;
            if (length <= 0) {
                // We've reached the end.
                break;
            }
        }

        return new SequenceInputStream(Iterators.asEnumeration(streams.iterator()));
    }

    @Override
    public Reader getBufferViewReader() {
        throw new UnsupportedOperationException("getBufferViewReader() not supported.");
    }

    @Override
    public ArrayView slice(int offset, int length) {
        throw new UnsupportedOperationException("slice() not supported.");
        // If support is added equals will also need to be implemented.
    }

    @Override
    public ByteBuffer asByteBuffer() {
        throw new UnsupportedOperationException("asByteBuffer() not supported.");
    }

    @Override
    public void copyTo(OutputStream target) {
        throw new UnsupportedOperationException("copyTo() not supported.");
    }

    @Override
    public int copyTo(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException("copyTo() not supported.");
    }

    @Override
    public void copyTo(byte[] target, int targetOffset, int length) {
        throw new UnsupportedOperationException("copyTo() not supported.");
    }

    @Override
    public <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) throws ExceptionT {
        throw new UnsupportedOperationException("collect() not supported.");
    }

    @Override
    public Iterator<ByteBuffer> iterateBuffers() {
        throw new UnsupportedOperationException("iterateBuffers() not supported.");
    }

    @Override
    public byte[] getCopy() {
        throw new UnsupportedOperationException("getCopy() not supported.");
    }

    //endregion

    //region Operations

    /**
     * Appends the given InputStream at the end of the array.
     *
     * @param dataStream The InputStream to append.
     * @param length     The length of the InputStream to append.
     */
    void append(InputStream dataStream, int length) {
        if (length == 0) {
            return;
        }

        int insertOffset = (this.firstArrayOffset + this.length) % BLOCK_SIZE;
        while (length > 0) {
            byte[] insertArray;
            if (insertOffset == 0) {
                // Last Array full
                insertArray = new byte[BLOCK_SIZE];
                this.arrays.add(insertArray);
            } else {
                insertArray = this.arrays.getLast();
            }

            int toCopy = Math.min(length, BLOCK_SIZE - insertOffset);
            try {
                int bytesCopied = StreamHelpers.readAll(dataStream, insertArray, insertOffset, toCopy);
                assert bytesCopied == toCopy : "unable to read the requested number of bytes";
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }

            length -= toCopy;
            this.length += toCopy;
            insertOffset = 0;
        }
    }

    /**
     * Truncates a number of bytes from the beginning of the array.
     *
     * @param truncationLength The number of bytes to truncate.
     */
    void truncate(int truncationLength) {
        Preconditions.checkPositionIndex(truncationLength, this.length, "trimLength must be non-negative and less than the length of the array.");
        this.length -= truncationLength;

        while (this.arrays.size() > 0 && truncationLength > 0) {
            byte[] first = this.arrays.getFirst();
            if (truncationLength >= first.length - this.firstArrayOffset) {
                // We need to truncate more than what is available in the first array; chop it all off.
                this.arrays.removeFirst();
                truncationLength -= first.length - this.firstArrayOffset;
                this.firstArrayOffset = 0;
            } else {
                // We need to truncate less than what is available in the first array; adjust offset.
                this.firstArrayOffset += truncationLength;
                truncationLength = 0;
            }
        }

        assert truncationLength == 0 : "not all requested bytes were truncated";
        if (this.arrays.size() == 0) {
            assert this.firstArrayOffset == 0 : "first entry offset not reset when no entries exist";
            assert this.length == 0 : "non-zero length when no entries exist";
        }
    }

    @Override
    public void set(int index, byte value) {
        throw new UnsupportedOperationException("set() not supported.");
    }

    @Override
    public void setShort(int index, short value) {
        throw new UnsupportedOperationException("setShort() not supported.");
    }

    @Override
    public void setInt(int index, int value) {
        throw new UnsupportedOperationException("setInt() not supported.");
    }

    @Override
    public void setLong(int index, long value) {
        throw new UnsupportedOperationException("setLong() not supported.");
    }

    //endregion
}
