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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ProgressivePromise;
import io.netty.util.concurrent.ScheduledFuture;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.Flush;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.Iterator;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import static io.pravega.shared.protocol.netty.WireCommandType.EVENT;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class AppendEncodeDecodeTest {

    private final int appendBlockSize = 1024;  
    private final UUID writerId = new UUID(1, 2);
    private final String streamName = "Test Stream Name";
    private final ConcurrentHashMap<Long, AppendBatchSizeTracker> idBatchSizeTrackerMap = new ConcurrentHashMap<>();
    private final CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get);
    private final FakeLengthDecoder lengthDecoder = new FakeLengthDecoder();
    private final AppendDecoder appendDecoder = new AppendDecoder();
    private Level origionalLogLevel;

    private EventExecutor executor =  new EventExecutor() {
        private final ScheduledExecutorService scheduler  = Executors.newSingleThreadScheduledExecutor();
        @Override
        public EventExecutor next() {
            return null;
        }

        @Override
        public EventExecutorGroup parent() {
            return null;
        }

        @Override
        public boolean inEventLoop() {
            return false;
        }

        @Override
        public boolean inEventLoop(Thread thread) {
            return false;
        }

        @Override
        public <V> Promise<V> newPromise() {
            return null;
        }

        @Override
        public <V> ProgressivePromise<V> newProgressivePromise() {
            return null;
        }

        @Override
        public <V> Future<V> newSucceededFuture(V result) {
            return null;
        }

        @Override
        public <V> Future<V> newFailedFuture(Throwable cause) {
            return null;
        }

        @Override
        public boolean isShuttingDown() {
            return false;
        }

        @Override
        public Future<?> shutdownGracefully() {
            return null;
        }

        @Override
        public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public Future<?> terminationFuture() {
            return null;
        }

        @Override
        public void shutdown() {

        }

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public Iterator<EventExecutor> iterator() {
            return null;
        }

        @Override
        public Future<?> submit(Runnable task) {
            return null;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return null;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return null;
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            scheduler.schedule(command, delay, unit);
            return null;
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return null;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
            return false;
        }

        @Override
        public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> collection) throws InterruptedException {
            return null;
        }

        @Override
        public <T> List<java.util.concurrent.Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) throws InterruptedException {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> collection) throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        @Override
        public void execute(Runnable runnable) {

        }
    };

    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private Channel ch;

    @Before
    public void setup() {
        Mockito.when(ctx.channel()).thenReturn(ch);
        Mockito.when(ctx.executor()).thenReturn(executor);
        idBatchSizeTrackerMap.put(0L, new FixedBatchSizeTracker(appendBlockSize));
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
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

    @Test(expected = InvalidMessageException.class)
    public void testAppendDecoderMissingAppendBlockEnd() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);

        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        WireCommands.AppendBlock appendBlock = new WireCommands.AppendBlock(writerId, Unpooled.wrappedBuffer(content));

        // Simulate Setup append
        appendDecoder.processCommand(setupAppend);
        // Simulate receiving an appendBlock
        appendDecoder.processCommand((WireCommand) appendBlock);
        // Simulate a missing appendBlockEnd by sending an appendBlock
        appendDecoder.processCommand((WireCommand) appendBlock);
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendDecoderInvalidAppendBlockEnd() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);

        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        WireCommands.AppendBlockEnd appendBlock = new WireCommands.AppendBlockEnd(writerId, 1024, null,  10, 2, 123L);

        // Simulate Setup append
        appendDecoder.processCommand(setupAppend);
        // Simulate an error by directly sending AppendBlockEnd.
        appendDecoder.processCommand((WireCommand) appendBlock);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIlligalWireCommand() throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        CommandEncoder commandEncoder = new CommandEncoder(null);
        commandEncoder.encode(ctx, null, fakeNetwork);
    }

    @Test
    public void testVerySmallBlockSize() throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(3));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get);
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
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(size));
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

        Flush flush = new Flush(writerId, streamName);
        encoder.encode(null, flush, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        Append one = (Append) received.get(0);
        assertEquals(size + TYPE_PLUS_LENGTH_SIZE, one.getData().readableBytes());
    }


    @Test
    public void testAppendWithoutBatchTracker() throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(null);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        ArrayList<Object> received = new ArrayList<>();
        commandEncoder.encode(ctx, msg, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());
        Append readAppend = (Append) received.get(1);
        assertEquals(msg.data.readableBytes(), readAppend.data.readableBytes());
        assertEquals(content.length + TYPE_PLUS_LENGTH_SIZE, readAppend.data.readableBytes());
    }

    @Test
    public void testblockTimeout() throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        ArrayList<Object> received = new ArrayList<>();
        Mockito.when(ch.writeAndFlush(Mockito.any())).thenAnswer(i -> {
           commandEncoder.encode(ctx, i.getArgument(0), fakeNetwork);
           return null;
        });
        commandEncoder.encode(ctx, msg, fakeNetwork);
        Thread.sleep(idBatchSizeTrackerMap.get(1L).getBatchTimeout() + 100);
        read(fakeNetwork, received);
        assertEquals(2, received.size());
        Append readAppend = (Append) received.get(1);
        assertEquals(msg.data.readableBytes(), readAppend.data.readableBytes());
        assertEquals(content.length + TYPE_PLUS_LENGTH_SIZE, readAppend.data.readableBytes());
    }

    @Test(expected = InvalidMessageException.class)
    public void testInvalidAppendEventNumber() throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(3));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        commandEncoder.encode(ctx, msg, fakeNetwork);
        Append invalidMsg = new Append("segment", writerId, 0, event, 1);
        commandEncoder.encode(ctx, invalidMsg, fakeNetwork);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAppendConditional() throws Exception {
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(3));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        commandEncoder.encode(ctx, msg, fakeNetwork);
        Append invalidMsg = new Append("segment", writerId, 2, event, 0, 1);
        commandEncoder.encode(ctx, invalidMsg, fakeNetwork);
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
    public void testSwitchingWriters() throws Exception {
        int size = 20; //Used to force a minimum size
        UUID writer1 = new UUID(1, 1);
        UUID writer2 = new UUID(2, 2);
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(size));

        CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get);
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        SetupAppend setupAppend = new SetupAppend(1, writer1, streamName, "");
        encoder.encode(null, setupAppend, fakeNetwork);
        setupAppend = new SetupAppend(1, writer2, streamName, "");
        encoder.encode(null, setupAppend, fakeNetwork);
        Append msg1 = new Append(streamName, writer1, 1, new Event(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3, 4 })), 1);
        encoder.encode(null, msg1, fakeNetwork);
        Append msg2 = new Append(streamName, writer2, 1, new Event(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3, 4 })), 1);
        encoder.encode(null, msg2, fakeNetwork);
    }
    
    @Test
    public void testZeroSizeAppend() throws Exception {
        int size = 0; //Used to force a minimum size
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(size));
        CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get);
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        SetupAppend setupAppend = new SetupAppend(1, writerId, streamName, "");
        encoder.encode(null, setupAppend, fakeNetwork);
        Append msg = new Append(streamName, writerId, 1, 1, Unpooled.EMPTY_BUFFER, null, 1);
        assertEquals(0, msg.data.readableBytes());
        encoder.encode(null, msg, fakeNetwork);
        Append msg2 = new Append(streamName, writerId, 2, 1, Unpooled.EMPTY_BUFFER, null, 1);
        Append msg3 = new Append(streamName, writerId, 3, 1, Unpooled.EMPTY_BUFFER, null, 1);
        encoder.encode(null, msg2, fakeNetwork);
        encoder.encode(null, msg3, fakeNetwork);
        ArrayList<Object> received = Lists.newArrayList();
        read(fakeNetwork, received);
        assertEquals(4, received.size());
        assertEquals(setupAppend, received.get(0));
        assertEquals(msg, received.get(1));
        assertEquals(msg2, received.get(2));
        assertEquals(msg3, received.get(3));
    }
    
    @Test
    public void testZeroSizeAppendInBlock() throws Exception {
        int size = 100;
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(size));
        CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get);
        @Cleanup("release")
        ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        SetupAppend setupAppend = new SetupAppend(1, writerId, streamName, "");
        encoder.encode(null, setupAppend, fakeNetwork);
        Append msg = new Append(streamName, writerId, 1, 1, Unpooled.EMPTY_BUFFER, null, 1);
        assertEquals(0, msg.data.readableBytes());
        encoder.encode(null, msg, fakeNetwork);
        Append msg2 = new Append(streamName, writerId, 2, 1, Unpooled.EMPTY_BUFFER, null, 1);
        Append msg3 = new Append(streamName, writerId, 3, 1, Unpooled.EMPTY_BUFFER, null, 1);
        encoder.encode(null, msg2, fakeNetwork);
        encoder.encode(null, msg3, fakeNetwork);
        encoder.encode(null, new KeepAlive(), fakeNetwork);
        ArrayList<Object> received = Lists.newArrayList();
        read(fakeNetwork, received);
        assertEquals(3, received.size());
        assertEquals(setupAppend, received.get(0));
        assertEquals(new Append(streamName, writerId, 3, 3, Unpooled.EMPTY_BUFFER, null, 1), received.get(1));
        assertEquals(new KeepAlive(), received.get(2));
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
        Append msg = new Append(segment, writerId, messageNumber, event, 1);
        assertEquals(length + WireCommands.TYPE_PLUS_LENGTH_SIZE, msg.data.readableBytes());
        encoder.encode(null, msg, out);
        assertEquals(length + WireCommands.TYPE_PLUS_LENGTH_SIZE, msg.data.readableBytes());
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
            assertEquals(1L, append.getRequestId());

        }
        assertEquals(numValues - 1, currentValue);
        assertEquals(currentCount, sizeOfEachValue);
    }

}
