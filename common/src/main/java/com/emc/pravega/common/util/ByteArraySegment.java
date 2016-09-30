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

package com.emc.pravega.common.util;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.io.FixedByteArrayOutputStream;
import com.emc.pravega.common.io.StreamHelpers;
import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Allows segmenting a byte array and operating only on that segment.
 */
public class ByteArraySegment {
    //region Members

    private final byte[] array;
    private final int startOffset;
    private final int length;
    private final boolean readOnly;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ByteArraySegment class that wraps the entire given array.
     *
     * @param array The array to wrap.
     * @throws NullPointerException If the array is null.
     */
    public ByteArraySegment(byte[] array) {
        this(array, 0, array.length);
    }

    /**
     * Creates a new instance of the ByteArraySegment class that wraps the given array range.
     *
     * @param array       The array to wrap.
     * @param startOffset The offset within the array to start the segment at.
     * @param length      The length of the segment.
     * @throws NullPointerException           If the array is null.
     * @throws ArrayIndexOutOfBoundsException If StartOffset or Length have invalid values.
     */
    public ByteArraySegment(byte[] array, int startOffset, int length) {
        this(array, startOffset, length, false);
    }

    /**
     * Creates a new instance of the ByteArraySegment class that wraps the given array range.
     *
     * @param array       The array to wrap.
     * @param startOffset The offset within the array to start the segment at.
     * @param length      The length of the segment.
     * @param readOnly    If true, no modifications will be allowed on the segment.
     * @throws NullPointerException           If the array is null.
     * @throws ArrayIndexOutOfBoundsException If StartOffset or Length have invalid values.
     */
    public ByteArraySegment(byte[] array, int startOffset, int length, boolean readOnly) {
        Preconditions.checkNotNull(array, "array");
        Exceptions.checkArrayRange(startOffset, length, array.length, "startOffset", "length");

        this.array = array;
        this.startOffset = startOffset;
        this.length = length;
        this.readOnly = readOnly;
    }

    //endregion

    //region Operations

    /**
     * Gets a value indicating whether the ByteArraySegment is read-only.
     *
     * @return The value.
     */
    public boolean isReadOnly() {
        return this.readOnly;
    }

    /**
     * Gets the value of the ByteArraySegment at the specified index.
     *
     * @param index The index to query.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    public byte get(int index) {
        Preconditions.checkElementIndex(index, this.length, "index");
        return this.array[index + this.startOffset];
    }

    /**
     * Sets the value of the ByteArraySegment at the specified index.
     *
     * @param index The index to set the value at.
     * @param value The value to set.
     * @throws IllegalStateException          If the ByteArraySegment is readonly.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    public void set(int index, byte value) {
        Preconditions.checkState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
        Preconditions.checkElementIndex(index, this.length, "index");
        this.array[index + this.startOffset] = value;
    }

    /**
     * Sets the value(s) of the ByteArraySegment starting at the specified index.
     *
     * @param index  The index to start setting the values at.
     * @param values The values to set. Position n inside this array will correspond to position 'index + n' inside the ByteArraySegment.
     * @throws IllegalStateException          If the ByteArraySegment is readonly.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or the items to be added cannot fit.
     */
    public void setSequence(int index, byte... values) {
        Preconditions.checkState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
        Exceptions.checkArrayRange(index, values.length, this.length, "index", "values.length");

        int baseOffset = index + this.startOffset;
        System.arraycopy(values, 0, this.array, baseOffset, values.length);
    }

    /**
     * Copies a specified number of bytes from the given ByteArraySegment into this ByteArraySegment.
     *
     * @param source       The ByteArraySegment to copy bytes from.
     * @param targetOffset The offset within this ByteArraySegment to start copying at.
     * @param length       The number of bytes to copy.
     * @throws IllegalStateException          If the ByteArraySegment is readonly.
     * @throws ArrayIndexOutOfBoundsException If targetOffset or length are invalid.
     */
    public void copyFrom(ByteArraySegment source, int targetOffset, int length) {
        Preconditions.checkState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
        Exceptions.checkArrayRange(targetOffset, length, this.length, "index", "values.length");
        Preconditions.checkElementIndex(length, source.getLength() + 1, "length");

        System.arraycopy(source.array, source.startOffset, this.array, targetOffset + this.startOffset, length);
    }

    /**
     * Copies a specified number of bytes from this ByteArraySegment into the given target array.
     *
     * @param target       The target array.
     * @param targetOffset The offset within the target array to start copying data at.
     * @param length       The number of bytes to copy.
     * @throws ArrayIndexOutOfBoundsException If targetOffset or length are invalid.
     */
    public void copyTo(byte[] target, int targetOffset, int length) {
        Preconditions.checkElementIndex(length, this.length + 1, "length");
        Exceptions.checkArrayRange(targetOffset, length, target.length, "index", "values.length");

        System.arraycopy(this.array, this.startOffset, target, targetOffset, length);
    }

    /**
     * Writes the entire contents of this ByteArraySegment to the given OutputStream. Only copies the contents of the
     * ByteArraySegment, and writes no other data (such as the length of the Segment or any other info).
     *
     * @param stream The OutputStream to write to.
     * @throws IOException If the OutputStream threw one.
     */
    public void writeTo(OutputStream stream) throws IOException {
        stream.write(this.array, this.startOffset, this.length);
    }

    /**
     * Attempts to read the contents of the InputStream and load it into this ByteArraySegment. Up to getLength() bytes
     * will be read from the InputStream, but no guarantees are made that the entire ByteArraySegment will be populated.
     * <p>
     * Only attempts to read the data, and does not expect any other header/footer information in the InputStream. This
     * method is the exact reverse of writeTo().
     *
     * @param stream The InputStream to read from.
     * @return The number of bytes read. This will be less than or equal to getLength().
     * @throws IOException If the InputStream threw one.
     */
    public int readFrom(InputStream stream) throws IOException {
        return StreamHelpers.readAll(stream, this.array, this.startOffset, this.length);
    }

    /**
     * Gets a value representing the length of this ByteArraySegment.
     *
     * @return The length.
     */
    public int getLength() {
        return this.length;
    }

    /**
     * Creates an InputStream that can be used to read the contents of this ByteArraySegment. The InputStream returned
     * is a ByteArrayInputStream that spans the entire ByteArraySegment.
     *
     * @return The InputStream.
     */
    public InputStream getReader() {
        return new ByteArrayInputStream(this.array, this.startOffset, this.length);
    }

    /**
     * Creates an InputStream that can be used to read the contents of this ByteArraySegment. The InputStream returned
     * is a ByteArrayInputStream that spans the given section of the ByteArraySegment.
     *
     * @param offset The starting offset of the section to read.
     * @param length The length of the section to read.
     * @return The InputStream.
     */
    public InputStream getReader(int offset, int length) {
        Exceptions.checkArrayRange(offset, length, this.length, "offset", "length");
        return new ByteArrayInputStream(this.array, this.startOffset + offset, length);
    }

    /**
     * Creates an OutputStream that can be used to write contents to this ByteArraySegment. The OutputStream returned
     * is a FixedByteArrayOutputStream (ByteArrayOutputStream that cannot expand) that spans the entire ByteArraySegment.
     *
     * @return The OutputStream.
     * @throws IllegalStateException If the ByteArraySegment is readonly.
     */
    public OutputStream getWriter() {
        Preconditions.checkState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
        return new FixedByteArrayOutputStream(this.array, this.startOffset, this.length);
    }

    /**
     * Returns a new ByteArraySegment that is a sub-segment of this ByteArraySegment. The new ByteArraySegment wraps
     * the same underlying byte array that this ByteArraySegment does.
     *
     * @param offset The offset within this ByteArraySegment where the new ByteArraySegment starts.
     * @param length The length of the new ByteArraySegment.
     * @return The new ByteArraySegment.
     * @throws ArrayIndexOutOfBoundsException If offset or length are invalid.
     */
    public ByteArraySegment subSegment(int offset, int length) {
        return subSegment(offset, length, this.readOnly);
    }

    /**
     * Returns a new ByteArraySegment that is a sub-segment of this ByteArraySegment. The new ByteArraySegment wraps
     * the same underlying byte array that this ByteArraySegment does.
     *
     * @param offset   The offset within this ByteArraySegment where the new ByteArraySegment starts.
     * @param length   The length of the new ByteArraySegment.
     * @param readOnly Whether the resulting sub-segment should be read-only.
     *                 Note: if this ByteArraySegment is already read-only, this argument is ignored and the resulting
     *                 segment is read-only
     * @return The new ByteArraySegment.
     * @throws ArrayIndexOutOfBoundsException If offset or length are invalid.
     */
    public ByteArraySegment subSegment(int offset, int length, boolean readOnly) {
        Exceptions.checkArrayRange(offset, length, this.length, "offset", "length");
        return new ByteArraySegment(this.array, this.startOffset + offset, length, readOnly || this.readOnly);
    }

    /**
     * Returns a new ByteArraySegment that wraps the same underlying array that this ByteSegmentDoes, except that the
     * new instance is marked as Read-Only.
     * If this instance is already Read-Only, this instance is returned instead.
     */
    public ByteArraySegment asReadOnly() {
        if (isReadOnly()) {
            return this;
        } else {
            return new ByteArraySegment(this.array, this.startOffset, this.length, true);
        }
    }

    //endregion
}
