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

    /**
     * Sets a 64 bit Unsigned Long at the specified index. This value can then be deserialized using
     * {@link StructuredReadableBuffer#getUnsignedLong(int)}. This method is not interoperable with
     * {@link StructuredReadableBuffer#getLong}.
     *
     * The advantage of serializing as Unsigned Long (vs. a normal Signed Long) is that the serialization will have the
     * same natural order as the input value type (i.e., if compared using a lexicographic bitwise comparator such as
     * BufferViewComparator, it will have the same ordering as the typical Long type).
     *
     * @param index The index to set the value at.
     * @param value  The (signed) value to write. The value will be converted into the range [0, 2^64-1] before
     *               serialization by flipping the high order bit (so positive values will begin with 1 and negative values
     *               will begin with 0).
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    default void setUnsignedLong(int index, long value) {
        setLong(index, value ^ Long.MIN_VALUE);
    }
}
