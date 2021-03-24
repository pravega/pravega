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
 * {@link BufferView} whose contents can be interpreted as structured data.
 * See {@link StructuredWritableBuffer} for reading such data.
 */
public interface StructuredReadableBuffer extends BufferView {
    /**
     * Gets the value at the specified index.
     *
     * @param index The index to get the value at.
     * @return The byte at the specified index.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    byte get(int index);

    /**
     * Gets the 16 bit Short value at the specified index.
     *
     * @param index The index to get the value at.
     * @return The Short at the specified index.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    short getShort(int index);

    /**
     * Gets the 32 bit Int value at the specified index.
     *
     * @param index The index to get the value at.
     * @return The Integer at the specified index.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    int getInt(int index);

    /**
     * Gets the 64 bit Long value at the specified index.
     *
     * @param index The index to get the value at.
     * @return The Long at the specified index.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    long getLong(int index);

    /**
     * Gets a 64 bit Unsigned Long from the specified index. This value must have been serialized using
     * {@link StructuredWritableBuffer#setUnsignedLong} for proper results. This method is not interoperable with
     * {@link StructuredWritableBuffer#setLong}.
     *
     * @param index The index to get the value at.
     * @return The Long at the specified index.
     * @throws ArrayIndexOutOfBoundsException If index is invalid or if there is insufficient space in the array starting
     *                                        at the specified index to fit the given value.
     */
    default long getUnsignedLong(int index) {
        return getLong(index) ^ Long.MIN_VALUE;
    }
}
