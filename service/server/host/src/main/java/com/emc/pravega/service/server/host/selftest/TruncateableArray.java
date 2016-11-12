/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.common.util.ArrayView;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Represents an ArrayView that can append at one end and truncate at the other one.
 */
class TruncateableArray implements ArrayView {
    //region Members
    private final LinkedList<byte[]> arrays;
    private int offset;
    private int length;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TruncateableArray class.
     */
    TruncateableArray() {
        this.arrays = new LinkedList<>();
        this.offset = 0;
        this.length = 0;
    }

    //endregion

    //region ArrayView Implementation

    @Override
    public byte get(int index) {
        Preconditions.checkPositionIndex(index, this.length, "index must be non-negative and less than the length of " +
                "the array.");

        // Adjust the index based on the first entry offset.
        index += this.offset;

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
    public void set(int index, byte value) {
        throw new IllegalStateException("set() not supported.");
    }

    @Override
    public void setSequence(int index, byte... values) {
        throw new IllegalStateException("setSequence() not supported.");
    }

    @Override
    public int getLength() {
        return this.length;
    }

    @Override
    public InputStream getReader() {
        return getReader(0, this.length);
    }

    @Override
    public InputStream getReader(int offset, int length) {
        ArrayList<ByteArrayInputStream> streams = new ArrayList<>();

        // Adjust the index based on the first entry offset.
        offset += this.offset;

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

    //endregion

    //region Operations

    /**
     * Appends the given InputStream at the end of the array.
     *
     * @param dataStream The InputStream to append.
     * @param length     The length of the InputStream to append.
     */
    void append(InputStream dataStream, int length) {
        byte[] data = new byte[length];
        try {
            int bytesCopied = StreamHelpers.readAll(dataStream, data, 0, length);
            assert bytesCopied == length : "unable to read the requested number of bytes";
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        append(data);
    }

    /**
     * Appends the given byte array at the end of the array.
     *
     * @param data The array to append.
     */
    void append(byte[] data) {
        if (data.length > 0) {
            this.arrays.add(data);
            this.length += data.length;
        }
    }

    /**
     * Truncates a number of bytes from the beginning of the array.
     *
     * @param truncationLength The number of bytes to truncate.
     */
    void truncate(int truncationLength) {
        Preconditions.checkPositionIndex(truncationLength, this.length, "trimLength must be non-negative and less " +
                "than the length of the array.");
        this.length -= truncationLength;

        while (this.arrays.size() > 0 && truncationLength > 0) {
            byte[] first = this.arrays.getFirst();
            if (truncationLength >= first.length - this.offset) {
                // We need to truncate more than what is available in the first array; chop it all off.
                this.arrays.removeFirst();
                truncationLength -= first.length - this.offset;
                this.offset = 0;
            } else {
                // We need to truncate less than what is available in the first array; adjust offset.
                this.offset += truncationLength;
                truncationLength = 0;
            }
        }

        assert truncationLength == 0 : "not all requested bytes were truncated";
        if (this.arrays.size() == 0) {
            assert this.offset == 0 : "first entry offset not reset when no entries exist";
            assert this.length == 0 : "non-zero length when no entries exist";
        }
    }

    //endregion
}
