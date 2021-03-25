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

import java.nio.ByteBuffer;

/**
 * Defines a generic read-only view of an index-based, array-like structure.
 */
public interface ArrayView extends BufferView, StructuredWritableBuffer, StructuredReadableBuffer {
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
     * Copies a specified number of bytes from this ArrayView into the given target array.
     *
     * @param target       The target array.
     * @param targetOffset The offset within the target array to start copying data at.
     * @param length       The number of bytes to copy.
     * @throws ArrayIndexOutOfBoundsException If targetOffset or length are invalid.
     */
    void copyTo(byte[] target, int targetOffset, int length);

    /**
     * Creates a new {@link ArrayView} that represents a sub-range of this {@link ArrayView} instance. The new instance
     * will share the same backing array as this one, so a change to one will be reflected in the other.
     *
     * @param offset The starting offset to begin the slice at.
     * @param length The sliced length.
     * @return A new {@link ArrayView}.
     */
    @Override
    ArrayView slice(int offset, int length);

    /**
     * Returns a new {@link ByteBuffer} that wraps the contents of this {@link ArrayView}.
     *
     * @return A {@link ByteBuffer} that shares the same backing array as this {@link ArrayView}. Any changes made to
     * the {@link ByteBuffer} will be reflected in this {@link ArrayView} and viceversa.
     */
    ByteBuffer asByteBuffer();
}
