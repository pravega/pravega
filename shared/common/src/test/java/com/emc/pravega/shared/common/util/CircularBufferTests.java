/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.shared.common.util;

import java.nio.ByteBuffer;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    @Ignore
    public void testLargeReads() {
        fail();
    }

    @Test
    @Ignore
    public void testLargeWrites() {
        fail();
    }
}
