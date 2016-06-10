package com.emc.logservice.common;

import java.io.*;

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
        Exceptions.throwIfNull(array, "array");
        Exceptions.throwIfIllegalArrayRange(startOffset, length, 0, array.length, "startOffset", "length");

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
     * @return The result.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    public byte get(int index) {
        Exceptions.throwIfIllegalArrayIndex(index, 0, this.length, "index");
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
        Exceptions.throwIfIllegalState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
        Exceptions.throwIfIllegalArrayIndex(index, 0, this.length, "index");
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
        Exceptions.throwIfIllegalState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
        Exceptions.throwIfIllegalArrayRange(index, values.length, 0, this.length, "index", "values.length");

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
        Exceptions.throwIfIllegalState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
        Exceptions.throwIfIllegalArrayRange(targetOffset, length, 0, this.length, "index", "values.length");
        Exceptions.throwIfIllegalArrayIndex(length, 0, source.getLength() + 1, "length");

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
        Exceptions.throwIfIllegalArrayIndex(length, 0, this.length + 1, "length");
        Exceptions.throwIfIllegalArrayRange(targetOffset, length, 0, target.length, "index", "values.length");

        System.arraycopy(this.array, this.startOffset, target, targetOffset, length);
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
        Exceptions.throwIfIllegalArrayRange(offset, length,0, this.length, "offset", "length");
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
        Exceptions.throwIfIllegalState(!this.readOnly, "Cannot modify a read-only ByteArraySegment.");
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
        Exceptions.throwIfIllegalArrayRange(offset, length,0, this.length, "offset", "length");
        return new ByteArraySegment(this.array, this.startOffset + offset, length, readOnly || this.readOnly);
    }

    /**
     * Returns a new ByteArraySegment that wraps the same underlying array that this ByteSegmentDoes, except that the
     * new instance is marked as Read-Only.
     * If this instance is already Read-Only, this instance is returned instead.
     *
     * @return The result.
     */
    public ByteArraySegment asReadOnly() {
        if (isReadOnly()) {
            return this;
        }
        else {
            return new ByteArraySegment(this.array, this.startOffset, this.length, true);
        }
    }

    //endregion
}
