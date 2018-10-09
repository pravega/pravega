/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.protocol.netty.WireCommandType.EVENT;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AppendEncodeDecodeTest {

    private final int appendBlockSize = 1024;  
    private final UUID writerId = new UUID(1, 2);
    private final String streamName = "Test Stream Name";
    private final CommandEncoder encoder = new CommandEncoder(new FixedBatchSizeTracker(appendBlockSize));
    private final FakeLengthDecoder lengthDecoder = new FakeLengthDecoder();
    private final AppendDecoder appendDecoder = new AppendDecoder();
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

    @RequiredArgsConstructor
    private static final class FixedBatchSizeTracker implements AppendBatchSizeTracker {
        private final int appendBlockSize;  

        @Override
        public int getAppendBlockSize() {
            return appendBlockSize;
        }

        @Override
        public void recordAppend(long eventNumber, int size) {

        }

        @Override
        public void recordAck(long eventNumber) {
        }

        @Override
        public int getBatchTimeout() {
            return 10;
        }
        
    }
    
    private static final class FakeLengthDecoder extends LengthFieldBasedFrameDecoder {
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
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        append(streamName, writerId, 0, 1, size, fakeNetwork);
    }
    
    @Test
    public void testVerySmallBlockSize() throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        Append msg = new Append("segment", writerId, 1, event);
        CommandEncoder commandEncoder = new CommandEncoder(new FixedBatchSizeTracker(3));
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(null, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        
        ArrayList<Object> received = new ArrayList<>();
        commandEncoder.encode(null, msg, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());
        Append readAppend = (Append) received.get(1);
        assertEquals(msg.data.readableBytes(), readAppend.data.readableBytes());
        assertEquals(content.length + TYPE_PLUS_LENGTH_SIZE, readAppend.data.readableBytes());
    }

    @Test
    public void testSwitchingStream() throws Exception {
        int size = appendBlockSize;
        int numEvents = 2;
        UUID c1 = new UUID(1, 1);
        UUID c2 = new UUID(2, 2);
        String s1 = "Stream 1";
        String s2 = "Stream 2";
        sendAndVerifyEvents(s1, c1, numEvents, size, numEvents);
        sendAndVerifyEvents(s2, c2, numEvents, size, numEvents);
    }

    private void sendAndVerifyEvents(String segment, UUID writerId, int numEvents, int eventSize,
            int expectedMessages) throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(segment, writerId, fakeNetwork);
        for (int i = 0; i < numEvents; i++) {
            append(segment, writerId, eventSize * (i + 1L), i, eventSize, fakeNetwork);
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
        testFlush(appendBlockSize / 2);
    }

    @Test
    public void testFlushWhenAtBlockBoundry() throws Exception {
        testFlush(appendBlockSize);
    }

    private void testFlush(int size) throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(streamName, writerId, fakeNetwork);

        append(streamName, writerId, size, 0, size, fakeNetwork);
        read(fakeNetwork, received);

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(null, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());

        Append one = (Append) received.get(0);
        assertEquals(size + TYPE_PLUS_LENGTH_SIZE, one.getData().readableBytes());
        KeepAlive two = (KeepAlive) received.get(1);
        assertEquals(keepAlive, two);
    }

    @Test
    public void testSmallAppends() throws Exception {
        int eventSize = 10;
        int numEvents = 10000;
        sendAndVerifyEvents(streamName,
                            writerId,
                            numEvents,
                            eventSize,
                            numEvents * (eventSize + TYPE_PLUS_LENGTH_SIZE) / appendBlockSize + 1);
    }

    @Test
    public void testAppendSpanningBlockBound() throws Exception {
        int numEvents = 4;
        int size = (appendBlockSize * 3) / numEvents;
        sendAndVerifyEvents(streamName, writerId, numEvents, size, 2);
    }

    @Test
    public void testBlockSizeAppend() throws Exception {
        int numEvents = 4;
        int size = appendBlockSize;
        sendAndVerifyEvents(streamName, writerId, numEvents, size, numEvents);
    }

    @Test
    public void testAlmostBlockSizeAppend1() throws Exception {
        int numEvents = 4;
        int size = appendBlockSize - 1;
        sendAndVerifyEvents(streamName, writerId, numEvents, size, numEvents);
    }

    @Test
    public void testAlmostBlockSizeAppend8() throws Exception {
        int numEvents = 4;
        int size = appendBlockSize - 8;
        sendAndVerifyEvents(streamName, writerId, numEvents, size, numEvents);
    }

    @Test
    public void testAlmostBlockSizeAppend16() throws Exception {
        int numEvents = 4;
        int size = appendBlockSize - 16;
        sendAndVerifyEvents(streamName, writerId, numEvents, size, numEvents);
    }

    @Test
    public void testAppendAtBlockBound() throws Exception {
        int size = appendBlockSize;
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        ArrayList<Object> received = setupAppend(streamName, writerId, fakeNetwork);

        append(streamName, writerId, size, 1, size, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        append(streamName, writerId, size + size / 2, 2, size / 2, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(null, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(3, received.size());

        Append one = (Append) received.get(0);
        Append two = (Append) received.get(1);
        assertEquals(size + TYPE_PLUS_LENGTH_SIZE, one.getData().readableBytes());
        assertEquals(size / 2 + TYPE_PLUS_LENGTH_SIZE, two.getData().readableBytes());
        KeepAlive three = (KeepAlive) received.get(2);
        assertEquals(keepAlive, three);
    }

    @Test
    public void testLargeAppend() throws Exception {
        int size = 10 * appendBlockSize;
        sendAndVerifyEvents(streamName, writerId, 2, size, 2);
    }

    private ArrayList<Object> setupAppend(String testStream, UUID writerId, ByteBuf fakeNetwork) throws Exception {
        SetupAppend setupAppend = new SetupAppend(1, writerId, testStream, "");
        encoder.encode(null, setupAppend, fakeNetwork);
        ArrayList<Object> received = new ArrayList<>();
        WireCommand command = CommandDecoder.parseCommand(fakeNetwork);
        assertTrue(appendDecoder.acceptInboundMessage(command));
        assertEquals(setupAppend, appendDecoder.processCommand(command));
        return received;
    }

    private long append(String segment, UUID writerId, long offset, int messageNumber, int length, ByteBuf out)
            throws Exception {
        byte[] content = new byte[length];
        Arrays.fill(content, (byte) messageNumber);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        Append msg = new Append(segment, writerId, messageNumber, event);
        encoder.encode(null, msg, out);
        return offset + length;
    }

    private void read(ByteBuf in, List<Object> results) throws Exception {
        ByteBuf segmented = (ByteBuf) lengthDecoder.decode(null, in);
        while (segmented != null) {
            int before = results.size();
            WireCommand command = CommandDecoder.parseCommand(segmented);
            if (appendDecoder.acceptInboundMessage(command)) {
                Request request = appendDecoder.processCommand(command);
                if (request != null) {
                    results.add(request);
                }
            } else {
                results.add(command);
            }
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
            while (append.getData().isReadable()) {
                if (currentCount == sizeOfEachValue) {
                    assertEquals(EVENT.getCode(), append.getData().readInt());
                    assertEquals(sizeOfEachValue, append.getData().readInt());
                    currentCount = 0;
                    currentValue++;
                }
                byte readByte = append.getData().readByte();
                assertEquals((byte) currentValue, readByte);
                currentCount++;
            }
            assertEquals(currentValue, append.getEventNumber());
        }
        assertEquals(numValues - 1, currentValue);
        assertEquals(currentCount, sizeOfEachValue);
    }

}
