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
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.MathHelpers;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This is a utility class for repeatedly reading data from an input stream, that tries to buffer data in a way that minimizes allocations.
 * It is intended that {@link #getBuffOfSize(InputStream, int)} is called in a loop to read chunks of data from the provided input stream.
 * This class does not support reading chunks larger than {@link WireCommands#MAX_WIRECOMMAND_SIZE}.
 */
@NotThreadSafe
class IoBuffer {
    private final int maxBufferSize = WireCommands.MAX_WIRECOMMAND_SIZE;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private ByteBuffer buffer = null;
    
    private ByteBuf sliceOut(int size) {
        ByteBuf result = Unpooled.wrappedBuffer(buffer.array(), buffer.arrayOffset() + buffer.position(), size);
        buffer.position(buffer.position() + size);
        if (!buffer.hasRemaining()) {
            buffer = null;
        }
        return result;
    }
    
    /**
     * Obtain a ByteBuff of size `size` by reading data from the provided input stream or from this buffer if the data is already available.
     */
    public ByteBuf getBuffOfSize(InputStream in, int size) throws IOException {
        checkArgument(size <= maxBufferSize, "Requested buffer size {} is larger than max allowed {}", size, maxBufferSize);
        if (size == 0) {
            // Technically this should not need to be special cased per the Javadoc of InputStrem,
            // but ByteArrayInputStream has a bug that makes this needed.
            return Unpooled.EMPTY_BUFFER;
        }
        if (buffer == null) {
            int bufferSize = MathHelpers.minMax(in.available(), size, maxBufferSize);
            byte[] newBuffer = new byte[bufferSize];
            int read = in.read(newBuffer);
            if (read <= -1) {
                throw new EOFException();
            }
            buffer = ByteBuffer.wrap(newBuffer, 0, read); 
        } 

        if (buffer.remaining() >= size) {
            return sliceOut(size);
        } else {
            int firstSize = buffer.remaining();
            ByteBuf first = sliceOut(firstSize);
            assert buffer == null; //Should have been fully sliced out
            byte[] remaining = new byte[size - firstSize];
            for (int offset = 0; offset < remaining.length;) {
                int read = in.read(remaining, offset, remaining.length - offset);
                if (read <= -1) {
                    throw new EOFException();
                }
                offset += read;
            }
            return Unpooled.wrappedBuffer(first, Unpooled.wrappedBuffer(remaining));
        }
    }
    
}