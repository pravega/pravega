/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.util;

import java.io.InputStream;

/**
 * Defines a generic view of an index-based, array-like structure.
 */
public interface ArrayView {
    /**
     * Gets the value at the specified index.
     *
     * @param index The index to query.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    byte get(int index);

    /**
     * Sets the value at the specified index.
     *
     * @param index The index to set the value at.
     * @param value The value to set.
     * @throws IllegalStateException          If the ByteArraySegment is readonly.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    void set(int index, byte value);

    /**
     * Sets the value(s) starting at the specified index.
     *
     * @param index  The index to start setting the values at.
     * @param values The values to set. Position n inside this array will correspond to position 'index + n' inside the ArrayView.
     * @throws IllegalStateException          If the ArrayView is readonly.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or the items to be added cannot fit.
     */
    void setSequence(int index, byte... values);

    /**
     * Gets a value representing the length of this ArrayView.
     *
     * @return The length.
     */
    int getLength();

    /**
     * Creates an InputStream that can be used to read the contents of this ArrayView. The InputStream returned
     * spans the entire ArrayView.
     *
     * @return The InputStream.
     */
    InputStream getReader();

    /**
     * Creates an InputStream that can be used to read the contents of this ArrayView. The InputStream returned
     * spans the given section of the ArrayView.
     *
     * @param offset The starting offset of the section to read.
     * @param length The length of the section to read.
     * @return The InputStream.
     */
    InputStream getReader(int offset, int length);
}
