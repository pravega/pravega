/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.common.util;

import java.io.InputStream;

/**
 * Defines a generic read-only view of an index-based, array-like structure.
 */
public interface ArrayView {
    /**
     * Gets the value at the specified index.
     *
     * @param index The index to query.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     * @return Byte indicating the value at the given index.
     */
    byte get(int index);

    /**
     * Gets a value representing the length of this ArrayView.
     *
     * @return The length.
     */
    int getLength();

    /**
     * Gets a reference to the backing array for this ArrayView. This should be used in conjunction with arrayOffset()
     * in order to determine where in the array this ArrayView starts at.
     * NOTE: Care must be taken when using this array. Just like any other array in Java, it is modifiable and changes to
     * it will be reflected in this ArrayView.
     *
     * @return The backing array.
     */
    byte[] array();

    /**
     * Gets a value indicating the offset in the backing array where this ArrayView starts at.
     *
     * @return The offset in the backing array.
     */
    int arrayOffset();

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
