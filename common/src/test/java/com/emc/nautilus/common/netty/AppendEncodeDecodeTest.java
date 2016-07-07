/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.nautilus.common.netty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;

import static org.junit.Assert.*;

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


    @Test(expected = IllegalStateException.class)
    public void testAppendWithoutSetup() throws Exception {
        int size = 10;
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        append(connectionId, 0, 1, size, fakeNetwork);
    }

    @Test
    public void testSwitchingStream() throws Exception {
        int size = WireCommands.APPEND_BLOCK_SIZE;
        int numEvents = 2;
        UUID c1 = new UUID(1, 1);
        UUID c2 = new UUID(2, 2);
        String s1 = "Stream 1";
        String s2 = "Stream 2";
        sendAndVerifyEvents(c1, s1, numEvents, size, numEvents);
        sendAndVerifyEvents(c2, s2, numEvents, size, numEvents);
    }

    private void sendAndVerifyEvents(UUID connectionId, String segment, int numEvents, int eventSize, int expectedMessages) throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(fakeNetwork,connectionId,segment);
        for (int i = 0; i < numEvents; i++) {
            append(connectionId, eventSize * (i + 1), i, eventSize, fakeNetwork);
            read(fakeNetwork, received);
        }
        assertEquals(expectedMessages, received.size());
        verify(received, numEvents, eventSize);
    }

    @Test
    public void testFlushBeforeEndOfBlock() throws Exception {
        testFlush(WireCommands.APPEND_BLOCK_SIZE / 2);
    }

    @Test
    public void testFlushWhenAtBlockBoundry() throws Exception {
        testFlush(WireCommands.APPEND_BLOCK_SIZE);
    }

    private void testFlush(int size) throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(fakeNetwork,connectionId,streamName);

        append(connectionId, size, 0, size, fakeNetwork);
        read(fakeNetwork, received);

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(null, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());

        AppendData one = (AppendData) received.get(0);
        assertEquals(size, one.data.readableBytes());
        KeepAlive two = (KeepAlive) received.get(1);
        assertEquals(keepAlive, two);
    }

    @Test
    public void testSmallAppends() throws Exception {
        int eventSize = 10;
        int numEvents = WireCommands.APPEND_BLOCK_SIZE;
        sendAndVerifyEvents(connectionId, streamName, numEvents, eventSize, eventSize);
    }

    @Test
    public void testAppendSpanningBlockBound() throws Exception {
        int numEvents = 4;
        int size = (WireCommands.APPEND_BLOCK_SIZE * 3) / numEvents;
        sendAndVerifyEvents(connectionId, streamName, numEvents, size, 3);
    }

    @Test
    public void testBlockSizeAppend() throws Exception {
        int numEvents = 4;
        int size = WireCommands.APPEND_BLOCK_SIZE;
        sendAndVerifyEvents(connectionId, streamName, numEvents, size, numEvents);
    }

    @Test
    public void testAppendAtBlockBound() throws Exception {
        int size = WireCommands.APPEND_BLOCK_SIZE;
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(fakeNetwork,connectionId,streamName);

        append(connectionId, size, 1, size, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        append(connectionId, size + size / 2, 2, size / 2, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(null, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(3, received.size());

        AppendData one = (AppendData) received.get(0);
        AppendData two = (AppendData) received.get(1);
        assertEquals(size, one.data.readableBytes());
        assertEquals(size / 2, two.data.readableBytes());
        KeepAlive three = (KeepAlive) received.get(2);
        assertEquals(keepAlive, three);
    }

    @Test
    public void testLargeAppend() throws Exception {
        int size = 10 * WireCommands.APPEND_BLOCK_SIZE;
        sendAndVerifyEvents(connectionId, streamName, 2, size, 2);
    }

    private ArrayList<Object> setupAppend(ByteBuf fakeNetwork, UUID connectionId, String testStream) throws Exception {
        SetupAppend setupAppend = new SetupAppend(connectionId, testStream);
        encoder.encode(null, setupAppend, fakeNetwork);
        ArrayList<Object> received = new ArrayList<>();
        decoder.decode(null, fakeNetwork, received);
        assertEquals(1, received.size());
        assertEquals(setupAppend, received.remove(0));
        return received;
    }

    private long append(UUID connectionId, long offset, int contentValue, int length, ByteBuf out) throws Exception {
        byte[] content = new byte[length];
        Arrays.fill(content, (byte) contentValue);
        ByteBuf buffer = Unpooled.wrappedBuffer(content);
        AppendData msg = new AppendData(connectionId, offset, buffer);
        encoder.encode(null, msg, out);
        return offset + length;
    }

    private void read(ByteBuf in, List<Object> results) throws Exception {
        ByteBuf segmented = (ByteBuf) lengthDecoder.decode(null, in);
        while (segmented != null) {
            int before = results.size();
            decoder.decode(null, segmented, results);
            assertTrue(results.size() == before || results.size() == before + 1);
            segmented = (ByteBuf) lengthDecoder.decode(null, in);
        }
    }

    private void verify(List<Object> results, int numValues, int sizeOfEachValue) {
        int readSoFar = 0;
        int currentValue = -1;
        int currentCount = sizeOfEachValue;
        for (Object r : results) {
            AppendData append = (AppendData) r;
            assertEquals("Append split mid event", sizeOfEachValue, currentCount);
            readSoFar += append.getData().readableBytes();
            assertEquals(readSoFar, append.getConnectionOffset());
            while (append.data.isReadable()) {
                byte readByte = append.data.readByte();
                if (currentCount == sizeOfEachValue) {
                    assertEquals((byte) (currentValue + 1), readByte);
                    currentValue = currentValue + 1;
                    currentCount = 1;
                } else {
                    assertEquals((byte) currentValue, readByte);
                    currentCount++;
                }
            }
        }
        assertEquals(numValues - 1, currentValue);
        assertEquals(currentCount, sizeOfEachValue);
    }

}
