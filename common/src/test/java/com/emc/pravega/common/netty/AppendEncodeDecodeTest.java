/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.common.netty;

import static com.emc.pravega.common.netty.WireCommandType.EVENT;
import static com.emc.pravega.common.netty.WireCommands.APPEND_BLOCK_SIZE;
import static com.emc.pravega.common.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.pravega.common.netty.WireCommands.Append;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import lombok.Cleanup;

public class AppendEncodeDecodeTest {

    private final UUID connectionId = new UUID(1, 2);
    private final String streamName = "Test Stream Name";
    private final CommandEncoder encoder = new CommandEncoder();
    private final FakeLengthDecoder lengthDecoder = new FakeLengthDecoder();
    private final CommandDecoder decoder = new CommandDecoder();
    private Level origionalLogLevel;

    @Before
    public void setup() {
        origionalLogLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
    }

    @After
    public void teardown() {
        ResourceLeakDetector.setLevel(origionalLogLevel);
    }

    private final class FakeLengthDecoder extends LengthFieldBasedFrameDecoder {
        FakeLengthDecoder() {
            super(1024 * 1024, 4, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            return super.decode(ctx, in);
        }
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendWithoutSetup() throws Exception {
        int size = 10;
        @Cleanup("release") ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        append(streamName, connectionId, 0, 1, size, fakeNetwork);
    }

    @Test
    public void testSwitchingStream() throws Exception {
        int size = APPEND_BLOCK_SIZE;
        int numEvents = 2;
        UUID c1 = new UUID(1, 1);
        UUID c2 = new UUID(2, 2);
        String s1 = "Stream 1";
        String s2 = "Stream 2";
        sendAndVerifyEvents(s1, c1, numEvents, size, numEvents);
        sendAndVerifyEvents(s2, c2, numEvents, size, numEvents);
    }

    private void sendAndVerifyEvents(String segment, UUID connectionId, int numEvents, int eventSize, int
            expectedMessages) throws Exception {
        @Cleanup("release") ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(segment, connectionId, fakeNetwork);
        for (int i = 0; i < numEvents; i++) {
            append(segment, connectionId, eventSize * (i + 1), i, eventSize, fakeNetwork);
            read(fakeNetwork, received);
        }
        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(null, keepAlive, fakeNetwork);
        read(fakeNetwork, received);

        assertEquals(expectedMessages + 1, received.size());
        assertEquals(received.get(received.size() - 1), keepAlive);
        received.remove(received.size() - 1);
        verify(received, numEvents, eventSize);
    }

    @Test
    public void testFlushBeforeEndOfBlock() throws Exception {
        testFlush(APPEND_BLOCK_SIZE / 2);
    }

    @Test
    public void testFlushWhenAtBlockBoundry() throws Exception {
        testFlush(APPEND_BLOCK_SIZE);
    }

    private void testFlush(int size) throws Exception {
        @Cleanup("release") ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(streamName, connectionId, fakeNetwork);

        append(streamName, connectionId, size, 0, size, fakeNetwork);
        read(fakeNetwork, received);

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(null, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());

        Append one = (Append) received.get(0);
        assertEquals(size + TYPE_PLUS_LENGTH_SIZE, one.data.readableBytes());
        KeepAlive two = (KeepAlive) received.get(1);
        assertEquals(keepAlive, two);
    }

    @Test
    public void testSmallAppends() throws Exception {
        int eventSize = 10;
        int numEvents = 10000;
        sendAndVerifyEvents(streamName, connectionId, numEvents, eventSize,
                numEvents * (eventSize + TYPE_PLUS_LENGTH_SIZE) / APPEND_BLOCK_SIZE + 1);
    }

    @Test
    public void testAppendSpanningBlockBound() throws Exception {
        int numEvents = 4;
        int size = (APPEND_BLOCK_SIZE * 3) / numEvents;
        sendAndVerifyEvents(streamName, connectionId, numEvents, size, 2);
    }

    @Test
    public void testBlockSizeAppend() throws Exception {
        int numEvents = 4;
        int size = APPEND_BLOCK_SIZE;
        sendAndVerifyEvents(streamName, connectionId, numEvents, size, numEvents);
    }

    @Test
    public void testAlmostBlockSizeAppend1() throws Exception {
        int numEvents = 4;
        int size = APPEND_BLOCK_SIZE - 1;
        sendAndVerifyEvents(streamName, connectionId, numEvents, size, numEvents);
    }

    @Test
    public void testAlmostBlockSizeAppend8() throws Exception {
        int numEvents = 4;
        int size = APPEND_BLOCK_SIZE - 8;
        sendAndVerifyEvents(streamName, connectionId, numEvents, size, numEvents);
    }

    @Test
    public void testAlmostBlockSizeAppend16() throws Exception {
        int numEvents = 4;
        int size = APPEND_BLOCK_SIZE - 16;
        sendAndVerifyEvents(streamName, connectionId, numEvents, size, numEvents);
    }

    @Test
    public void testAppendAtBlockBound() throws Exception {
        int size = APPEND_BLOCK_SIZE;
        @Cleanup("release") ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(streamName, connectionId, fakeNetwork);

        append(streamName, connectionId, size, 1, size, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        append(streamName, connectionId, size + size / 2, 2, size / 2, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(null, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(3, received.size());

        Append one = (Append) received.get(0);
        Append two = (Append) received.get(1);
        assertEquals(size + TYPE_PLUS_LENGTH_SIZE, one.data.readableBytes());
        assertEquals(size / 2 + TYPE_PLUS_LENGTH_SIZE, two.data.readableBytes());
        KeepAlive three = (KeepAlive) received.get(2);
        assertEquals(keepAlive, three);
    }

    @Test
    public void testLargeAppend() throws Exception {
        int size = 10 * APPEND_BLOCK_SIZE;
        sendAndVerifyEvents(streamName, connectionId, 2, size, 2);
    }

    private ArrayList<Object> setupAppend(String testStream, UUID connectionId, ByteBuf fakeNetwork) throws Exception {
        SetupAppend setupAppend = new SetupAppend(connectionId, testStream);
        encoder.encode(null, setupAppend, fakeNetwork);
        ArrayList<Object> received = new ArrayList<>();
        decoder.decode(null, fakeNetwork, received);
        assertEquals(1, received.size());
        assertEquals(setupAppend, received.remove(0));
        return received;
    }

    private long append(String segment, UUID connectionId, long offset, int messageNumber, int length, ByteBuf out)
            throws Exception {
        byte[] content = new byte[length];
        Arrays.fill(content, (byte) messageNumber);
        ByteBuf buffer = Unpooled.wrappedBuffer(content);
        Append msg = new Append(segment, connectionId, messageNumber, buffer);
        encoder.encode(null, msg, out);
        return offset + length;
    }

    private void read(ByteBuf in, List<Object> results) throws Exception {
        ByteBuf segmented = (ByteBuf) lengthDecoder.decode(null, in);
        while (segmented != null) {
            int before = results.size();
            decoder.decode(null, segmented, results);
            assertTrue(results.size() == before || results.size() == before + 1);
            segmented.release();
            segmented = (ByteBuf) lengthDecoder.decode(null, in);
        }
    }

    private void verify(List<Object> results, int numValues, int sizeOfEachValue) {
        int currentValue = -1;
        int currentCount = sizeOfEachValue;
        for (Object r : results) {
            Append append = (Append) r;
            assertEquals("Append split mid event", sizeOfEachValue, currentCount);
            while (append.data.isReadable()) {
                if (currentCount == sizeOfEachValue) {
                    assertEquals(EVENT.getCode(), append.data.readInt());
                    assertEquals(sizeOfEachValue, append.data.readInt());
                    currentCount = 0;
                    currentValue++;
                }
                byte readByte = append.data.readByte();
                assertEquals((byte) currentValue, readByte);
                currentCount++;
            }
            assertEquals(currentValue, append.getEventNumber());
        }
        assertEquals(numValues - 1, currentValue);
        assertEquals(currentCount, sizeOfEachValue);
    }

}
