/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.io.InputStream;

/**
 * Defines a generic read-only view of an index-based, array-like structure.
 */
public interface ArrayView extends BufferView {
    /**
     * Gets the value at the specified index.
     *
     * @param index The index to query.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     * @return Byte indicating the value at the given index.
     */
    byte get(int index);

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
     * spans the given section of the ArrayView.
     *
     * @param offset The starting offset of the section to read.
     * @param length The length of the section to read.
     * @return The InputStream.
     */
    InputStream getReader(int offset, int length);

    /**
     * Copies a specified number of bytes from this ArrayView into the given target array.
     *
     * @param target       The target array.
     * @param targetOffset The offset within the target array to start copying data at.
     * @param length       The number of bytes to copy.
     * @throws ArrayIndexOutOfBoundsException If targetOffset or length are invalid.
     */
    void copyTo(byte[] target, int targetOffset, int length);
}
