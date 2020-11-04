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

/**
 * Modifiable {@link BufferView} whose contents can be interpreted as structured data.
 * See {@link StructuredReadableBuffer} for reading such data.
 */
public interface StructuredWritableBuffer extends BufferView {
    /**
     * Sets the value at the specified index.
     *
     * @param index The index to set the value at.
     * @param value The Byte value to set.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    void set(int index, byte value);

    /**
     * Sets a Short value at the specified index.
     *
     * @param index The index to set the value at.
     * @param value The Short value to set.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    void setShort(int index, short value);

    /**
     * Sets a 32 bit Integer value at the specified index.
     *
     * @param index The index to set the value at.
     * @param value The Integer value to set.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    void setInt(int index, int value);

    /**
     * Sets a 64 bit Long value at the specified index.
     *
     * @param index The index to set the value at.
     * @param value The Long value to set.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    void setLong(int index, long value);
}
