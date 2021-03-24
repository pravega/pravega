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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CircularBufferTests {

    @Test
    public void testFillAndEmpty() {
        int times = 10;
        int capacity = 8 * times;
        CircularBuffer buffer = new CircularBuffer(capacity);

        ByteBuffer in = ByteBuffer.allocate(8);
        ByteBuffer out = ByteBuffer.allocate(8);
        for (int i = 0; i < times; i++) {
            in.putLong(i);
            in.rewind();
            assertEquals(capacity - i * 8, buffer.capacityAvailable());
            assertEquals(i * 8, buffer.dataAvailable());
            assertEquals(8, buffer.fill(in));
            in.clear();
        }
        assertEquals(0, buffer.capacityAvailable());
        assertEquals(capacity, buffer.dataAvailable());
        assertEquals(0, buffer.fill(in));

        for (int i = 0; i < times; i++) {
            assertEquals(i * 8, buffer.capacityAvailable());
            assertEquals(capacity - i * 8, buffer.dataAvailable());
            assertEquals(8, buffer.read(out));
            out.rewind();
            assertEquals(i, out.getLong());
            out.clear();
        }
        assertEquals(capacity, buffer.capacityAvailable());
        assertEquals(0, buffer.dataAvailable());
        assertEquals(0, buffer.read(out));
    }

    @Test
    public void testReadFollowWrite() {
        int times = 10;
        int capacity = 8 * times;
        CircularBuffer buffer = new CircularBuffer(capacity);

        ByteBuffer in = ByteBuffer.allocate(8);
        ByteBuffer out = ByteBuffer.allocate(8);
        for (int i = 0; i < times * times; i++) {
            in.putLong(i);
            in.rewind();
            assertEquals(capacity, buffer.capacityAvailable());
            assertEquals(0, buffer.dataAvailable());
            assertEquals(8, buffer.fill(in));
            assertEquals(8, buffer.dataAvailable());
            assertEquals(capacity - 8, buffer.capacityAvailable());
            in.clear();
            assertEquals(8, buffer.read(out));
            out.rewind();
            assertEquals(i, out.getLong());
            out.clear();
        }
    }

    @Test
    public void writeWrapps() {
        int times = 9;
        int capacity = 9;
        CircularBuffer buffer = new CircularBuffer(capacity);

        ByteBuffer in = ByteBuffer.allocate(8);
        ByteBuffer out = ByteBuffer.allocate(8);
        for (int i = 0; i < times * times; i++) {
            in.putLong(i);
            in.rewind();
            assertEquals(capacity, buffer.capacityAvailable());
            assertEquals(0, buffer.dataAvailable());
            assertEquals(8, buffer.fill(in));
            assertEquals(8, buffer.dataAvailable());
            assertEquals(capacity - 8, buffer.capacityAvailable());
            in.clear();
            assertEquals(8, buffer.read(out));
            out.rewind();
            assertEquals(i, out.getLong());
            out.clear();
        }
    }

    @Test
    public void testLargeWrites() {
        int capacity = 10;
        byte[] pattern = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        CircularBuffer buffer = new CircularBuffer(capacity);
        ByteBuffer in = ByteBuffer.allocate(20);
        ByteBuffer out = ByteBuffer.allocate(20);
        in.put(pattern);
        in.put(pattern);
        in.flip();
        assertEquals(0, buffer.dataAvailable());
        assertEquals(10, buffer.fill(in));
        assertEquals(10, buffer.dataAvailable());
        assertEquals(0, buffer.capacityAvailable());
        assertEquals(10, buffer.read(out));
        assertEquals(10, out.position());
        out.flip();
        for (int i = 0; i < 10; i++) {
            assertEquals(pattern[i], out.get(i));
        }
    }

    @Test
    public void testLargeReads() {
        int capacity = 12;
        ByteBuffer in = ByteBuffer.allocate(4);
        in.putInt(1);
        in.flip();
        CircularBuffer buffer = new CircularBuffer(4);
        ByteBuffer out = ByteBuffer.allocate(capacity);
        buffer.fill(in);
        buffer.read(out);
        in.clear();
        in.putInt(2);
        in.flip();
        buffer.fill(in);
        buffer.read(out);
        in.clear();
        in.putInt(3);
        in.flip();
        buffer.fill(in);
        buffer.read(out);
        out.flip();
        assertEquals(1, out.getInt());
        assertEquals(2, out.getInt());
        assertEquals(3, out.getInt());
    }

    @Test
    public void testFillBuffers() {
        int capacity = 10;
        byte[] pattern = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        CircularBuffer buffer = new CircularBuffer(capacity);
        ByteBuffer in = ByteBuffer.allocate(20);
        ByteBuffer out = ByteBuffer.allocate(20);
        in.put(pattern);
        in.put(pattern);
        in.flip();
        ByteBuffer[] buffers = new ByteBuffer[5];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBufferUtils.slice(in, i * 2, 2);
        }

        assertEquals(0, buffer.dataAvailable());
        assertEquals(10, buffer.fill(buffers));
        assertEquals(10, buffer.dataAvailable());
        assertEquals(0, buffer.capacityAvailable());
        assertEquals(10, buffer.read(out));
        assertEquals(10, out.position());
        out.flip();
        for (int i = 0; i < 10; i++) {
            assertEquals(pattern[i], out.get(i));
        }
    }
}
