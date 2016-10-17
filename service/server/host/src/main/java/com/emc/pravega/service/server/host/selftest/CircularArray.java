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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Created by andrei on 10/17/16.
 */
class CircularArray implements ArrayView {
    private final byte[] buffer;
    private int start;
    private int length;

    CircularArray(int capacity) {
        this.buffer = new byte[capacity];
        this.start = 0;
        this.length = 0;
    }

    //region ArrayView Implementation

    @Override
    public byte get(int index) {
        Preconditions.checkPositionIndex(index, this.length, "index must be non-negative and less than the length of the array.");
        return this.buffer[getPosition(this.start + index)];
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

    //endregion

    /**
     * Appends the given data at the end of the array.
     *
     * @param data   The data to append.
     * @param length The length of the data.
     */
    void append(InputStream data, int length) {
        Preconditions.checkPositionIndex(this.length + length, this.buffer.length, "Given input does not fit in the array.");
        try {
            int endIndex = getPosition(this.start + this.length - 1);
            // 1. Write from the first index after EndIndex until the end of the array.
            int bytesToCopy = Math.min(length, this.buffer.length - endIndex - 1);
            if (bytesToCopy > 0) {
                StreamHelpers.readAll(data, this.buffer, endIndex + 1, bytesToCopy);
            }

            // 2. Write from the beginning of the array.
            if (bytesToCopy < length) {
                StreamHelpers.readAll(data, this.buffer, 0, length - bytesToCopy);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        // Update length and end pointer.
        this.length += length;
    }

    /**
     * Truncates the array from the beginning.
     *
     * @param truncationLength The amount of data, in bytes, to truncate.
     */
    void truncate(int truncationLength) {
        Preconditions.checkPositionIndex(truncationLength, this.length, "trimLength must be non-negative and less than the length of the array.");
        this.length -= truncationLength;
        this.start = getPosition(this.start + truncationLength);
    }

    private int getPosition(int index) {
        return index % this.buffer.length;
    }
}
