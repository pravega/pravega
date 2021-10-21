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
package io.pravega.common.io.serialization;

import io.pravega.common.io.DirectDataOutput;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;

/**
 * Defines an extension to OutputStream that allows writing to an arbitrary position.
 */
public interface RandomAccessOutputStream extends DirectDataOutput {
    /**
     * Writes the given Int value at the given position.
     *
     * @param intValue The value to write.
     * @param position The position to write at.
     * @throws IOException               If an IO Exception occurred.
     * @throws IndexOutOfBoundsException If position is outside of the current bounds of this object.
     */
    void writeInt(int intValue, int position) throws IOException;

    /**
     * Writes the given byte value at the given position.
     *
     * @param byteValue The value to write.
     * @param position  The position to write at.
     * @throws IOException               If an IO Exception occurred.
     * @throws IndexOutOfBoundsException If position is outside of the current bounds of this object.
     */
    void write(int byteValue, int position) throws IOException;

    /**
     * Writes a sequence of bytes at the given position.
     *
     * NOTE: depending on the implementation of this interface, this may result in increasing the size of the stream. For
     * example, if position is smaller than size() AND position + length is larger than size() then the extra bytes may
     * be appended at the end, if the underlying OutputStream's structure permits it.
     *
     * @param buffer       The buffer to write from.
     * @param bufferOffset The offset within the buffer to start at.
     * @param length       The number of bytes to write.
     * @param position     The position within the OutputStream to write at.
     * @throws IOException               If an IO Exception occurred.
     * @throws IndexOutOfBoundsException If bufferOffset and length are invalid for the given buffer or if position is
     *                                   invalid for the current OutputStream's state.
     */
    void write(byte[] buffer, int bufferOffset, int length, int position) throws IOException;

    /**
     * Gets a value indicating the size of this OutputStream.
     *
     * @return size of stream
     */
    int size();

    /**
     * Returns a {@link ByteArraySegment} wrapping the current contents of the {@link RandomAccessOutputStream}.
     *
     * Further changes to the {@link RandomAccessOutputStream} may or may not be reflected in the returned object
     * (depending on methods invoked and whether the underlying buffers need to be resized).
     *
     * @return A {@link ByteArraySegment} from the current contents of the {@link RandomAccessOutputStream}.
     */
    ByteArraySegment getData();
}
