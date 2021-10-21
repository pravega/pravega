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

import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * Convenience class wrapping byteBuffer to provide a circular buffer. This works by maintaining
 * two byte buffers backed by the same array. The position of the readBuffer corresponds to the
 * point up to which data has been read. The position of the writeBuffer corresponds to the point up
 * to
 * which data has been written. Each buffer's limit is either the end or the buffer or the position
 * of the other, depending on where the data has wrapped.
 */
public class CircularBuffer {

    private final ByteBuffer readBuffer;
    private final ByteBuffer fillBuffer;
    @Getter
    private final int capacity;

    public CircularBuffer(int capacity) {
        this.capacity = capacity;
        byte[] buffer = new byte[capacity];
        readBuffer = ByteBuffer.wrap(buffer);
        fillBuffer = ByteBuffer.wrap(buffer);
        clear();
    }

    public void clear() {
        readBuffer.position(0).limit(0);
        fillBuffer.position(0).limit(fillBuffer.capacity());
    }

    /**
     * Copies into the given ByteBuffer.
     *
     * @param toFill The ByteBuffer to fill into.
     * @return the number of bytes put into toFill
     */
    public int read(ByteBuffer toFill) {
        int originalPos = toFill.position();
        while (dataAvailable() > 0 && toFill.hasRemaining()) {
            readHelper(toFill);
        }
        return toFill.position() - originalPos;
    }

    private void readHelper(ByteBuffer toFill) {
        int readLimit = readBuffer.limit();
        int toRead = Math.min(toFill.remaining(), readBuffer.remaining());
        readBuffer.limit(readBuffer.position() + toRead);
        toFill.put(readBuffer);
        readBuffer.limit(readLimit);
        if (readBuffer.position() == capacity) {
            readBuffer.position(0);
            readBuffer.limit(fillBuffer.position());
            fillBuffer.limit(capacity);
        }
        if (fillBuffer.position() < readBuffer.position()) {
            fillBuffer.limit(readBuffer.position());
        }
    }

    /**
     * Copies from the given ByteBuffer.
     *
     * @param fillFrom The ByteBuffer to read from.
     * @return the number of bytes read from fillFrom
     */
    public int fill(ByteBuffer fillFrom) {
        int origionalPos = fillFrom.position();
        while (capacityAvailable() > 0 && fillFrom.hasRemaining()) {
            fillHelper(fillFrom);
        }
        return fillFrom.position() - origionalPos;
    }

    /**
     * Copies from the given ByteBuffers.
     *
     * @param fillFrom An array of ByteBuffer to read from.
     * @return the number of bytes read from fillFrom
     */
    public int fill(ByteBuffer[] fillFrom) {
        int result = 0;
        for (ByteBuffer bb : fillFrom) {
            int copied = fill(bb);
            result += copied;
            if (copied == 0) {
                break;
            }
        }
        return result;
    }

    private void fillHelper(ByteBuffer fillFrom) {
        int fillLimit = fillBuffer.limit();
        int toAdd = Math.min(fillFrom.remaining(), fillBuffer.remaining());
        fillBuffer.limit(fillBuffer.position() + toAdd);

        int limit = fillFrom.limit();
        fillFrom.limit(fillFrom.position() + toAdd);
        fillBuffer.put(fillFrom);
        fillFrom.limit(limit);
        fillBuffer.limit(fillLimit);

        if (fillBuffer.position() == capacity) {
            fillBuffer.position(0);
            fillBuffer.limit(readBuffer.position());
            readBuffer.limit(capacity);
        }
        if (readBuffer.position() < fillBuffer.position()) {
            readBuffer.limit(fillBuffer.position());
        }
    }

    /**
     * Gets the number of bytes that can be read.
     *
     * @return Integer indicating the number of bytes that can be read.
     */
    public int dataAvailable() {
        if (readBuffer.position() < fillBuffer.position()) {
            return readBuffer.remaining();
        } else if (readBuffer.position() > fillBuffer.position()) {
            return capacity - fillBuffer.remaining();
        } else {
            if (readBuffer.hasRemaining()) {
                return readBuffer.remaining() + fillBuffer.position();
            } else {
                return 0;
            }
        }
    }

    public int capacityAvailable() {
        if (fillBuffer.position() < readBuffer.position()) {
            return fillBuffer.remaining();
        } else if (fillBuffer.position() > readBuffer.position()) {
            return capacity - readBuffer.remaining();
        } else {
            if (fillBuffer.hasRemaining()) {
                return fillBuffer.remaining() + readBuffer.position();
            } else {
                return 0;
            }
        }
    }
    
    @Override
    public String toString() {
        return "Buffer { capacityAvailable:" + capacityAvailable() + " dataAvailable: " + dataAvailable() + "}";
    }
}