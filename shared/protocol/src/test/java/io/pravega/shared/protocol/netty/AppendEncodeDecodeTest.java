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
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ProgressivePromise;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static io.pravega.shared.metrics.MetricNotifier.NO_OP_METRIC_NOTIFIER;
import static io.pravega.shared.protocol.netty.WireCommandType.EVENT;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class AppendEncodeDecodeTest extends LeakDetectorTestSuite {

    private final int appendBlockSize = 1024;
    private final UUID writerId = new UUID(1, 2);
    private final String streamName = "Test Stream Name";
    private final ConcurrentHashMap<Long, AppendBatchSizeTracker> idBatchSizeTrackerMap = new ConcurrentHashMap<>();
    private final CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
    private final FakeLengthDecoder lengthDecoder = new FakeLengthDecoder();
    private final AppendDecoder appendDecoder = new AppendDecoder();
    private EventExecutor executor = new EventExecutor() {
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
        @SuppressWarnings("deprecation")
        public void shutdown() {

        }

        @Override
        @SuppressWarnings("deprecation")
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
            executorService().schedule(command, delay, unit);
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
    private ByteBuf fakeNetwork;
    private BufferReleaseVerifier releaseVerifier;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setup() {
        Mockito.when(ctx.channel()).thenReturn(ch);
        Mockito.when(ctx.executor()).thenReturn(executor);
        idBatchSizeTrackerMap.put(0L, new FixedBatchSizeTracker(appendBlockSize));
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
        fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
        releaseVerifier = new BufferReleaseVerifier();
    }

    @After
    public void tearDown() {
        releaseVerifier.close(); // This will check that all included buffers have been properly released.
        fakeNetwork.release();
        Assert.assertEquals(0, fakeNetwork.refCnt());
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
        public long recordAck(long eventNumber) {
            return 0;
        }

        @Override
        public int getBatchTimeout() {
            return 100;
        }        
    }
    
    private static final class FakeLengthDecoder extends LengthFieldBasedFrameDecoder {
        FakeLengthDecoder() {
            super(8 * 1024 * 1024, 4, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            return super.decode(ctx, in);
        }
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendWithoutSetup() throws Exception {
        int size = 10;
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
        appendDecoder.processCommand(appendBlock);
        // Simulate a missing appendBlockEnd by sending an appendBlock
        appendDecoder.processCommand(appendBlock);
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
        appendDecoder.processCommand(appendBlock);
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendDecoderInvalidAppendBlockEndEvents() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        ByteBuf data = Unpooled.wrappedBuffer(content);

        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        WireCommands.AppendBlockEnd appendBlock = new WireCommands.AppendBlockEnd(writerId, 1024, data,  0, 2, 123L);

        // Simulate Setup append
        appendDecoder.processCommand(setupAppend);
        // Simulate an error by directly sending AppendBlockEnd.
        appendDecoder.processCommand(appendBlock);
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendDecoderInvalidAppendBlockEndLastEvent() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        ByteBuf data = Unpooled.wrappedBuffer(content);

        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        WireCommands.AppendBlockEnd blockEnd = new WireCommands.AppendBlockEnd(writerId, 1024, data, 1, 2, 123L);
        ByteBuf data2 = Unpooled.wrappedBuffer(content);
        val blockEnd2 = new WireCommands.AppendBlockEnd(writerId, 1024, data2, 1, 1, 123L);
        this.releaseVerifier.include(blockEnd, data).include(blockEnd2, data2);

        // Simulate Setup append
        appendDecoder.processCommand(setupAppend);
        Append append = (Append) appendDecoder.processCommand(blockEnd);
        append.getData().release(); // This append is successful and has been consumed; dispose of it.
        appendDecoder.processCommand(blockEnd2);
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendDecoderInvalidAppendBlockSize() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        ByteBuf data = Unpooled.wrappedBuffer(content);

        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        WireCommands.AppendBlock appendBlock = new WireCommands.AppendBlock(writerId, data);

        ByteBuf data2 = Unpooled.wrappedBuffer(content);
        WireCommands.AppendBlockEnd appendBlockEnd = new WireCommands.AppendBlockEnd(writerId, 1024, data2, 2, 2, 123L);
        this.releaseVerifier.include(appendBlock, data).include(appendBlockEnd, data2);

        // Simulate Setup append
        appendDecoder.processCommand(setupAppend);
        appendDecoder.processCommand(appendBlock);
        appendDecoder.processCommand(appendBlockEnd);
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendDecoderAppendBlockEndInvalidWriterID() throws Exception {
        final UUID writerId2 = new UUID(1, 3);
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);

        SetupAppend setupAppend1 = new SetupAppend(1, writerId, "segment", "");
        SetupAppend setupAppend2 = new SetupAppend(2, writerId2, "segment2", "");
        val data1 = Unpooled.wrappedBuffer(content);
        WireCommands.AppendBlock appendBlock = new WireCommands.AppendBlock(writerId, data1);
        val data2 = Unpooled.wrappedBuffer(content);
        WireCommands.AppendBlockEnd appendBlockEnd = new WireCommands.AppendBlockEnd(writerId2, 1024, data2, 1, 1, 123L);
        this.releaseVerifier.include(appendBlock, data1).include(appendBlockEnd, data2);

        appendDecoder.processCommand(setupAppend1);
        appendDecoder.processCommand(setupAppend2);
        appendDecoder.processCommand(appendBlock);
        appendDecoder.processCommand(appendBlockEnd);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalWireCommand() throws Exception {
        CommandEncoder commandEncoder = new CommandEncoder(null, NO_OP_METRIC_NOTIFIER);
        commandEncoder.encode(ctx, null, fakeNetwork);
    }

    @Test(expected = InvalidMessageException.class)
    public void testAppendInvalidCount() throws Exception {
        byte[] content = new byte[10];
        Arrays.fill(content, (byte) 1);
        ByteBuf data = Unpooled.wrappedBuffer(content);
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        @Cleanup("release")
        val received = new ReceivedCommands();
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        commandEncoder.encode(ctx, new Append("segment", writerId, 1, 0, data, (long) data.readableBytes(), 1), fakeNetwork);
        read(fakeNetwork, received);
    }

    @Test
    public void testVerySmallBlockSize() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(3));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);

        @Cleanup("release")
        val received = new ReceivedCommands();
        commandEncoder.encode(ctx, msg, fakeNetwork);
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
        val received = new ReceivedCommands();
        setupAppend(segment, writerId, fakeNetwork);
        for (int i = 0; i < numEvents; i++) {
            append(segment, writerId, eventSize * (i + 1L), i, eventSize, fakeNetwork);
            read(fakeNetwork, received);
        }
        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(ctx, keepAlive, fakeNetwork);
        read(fakeNetwork, received);

        assertEquals(expectedMessages + 1, received.size());
        assertEquals(received.get(received.size() - 1), keepAlive);
        received.results.remove(received.size() - 1);
        verify(received.results, numEvents, eventSize);
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
        val received = new ReceivedCommands();
        setupAppend(streamName, writerId, fakeNetwork);

        append(streamName, writerId, 0, 0, size, fakeNetwork);
        read(fakeNetwork, received);

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(ctx, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());

        Append one = (Append) received.get(0);
        verifyNoExcessData(one.getData());
        assertEquals(size + TYPE_PLUS_LENGTH_SIZE, one.getData().readableBytes());
        KeepAlive two = (KeepAlive) received.get(1);
        assertEquals(keepAlive, two);
    }

    @Test
    public void testAppendWithoutBatchTracker() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(null, NO_OP_METRIC_NOTIFIER);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        @Cleanup("release")
        val received = new ReceivedCommands();
        commandEncoder.encode(ctx, msg, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());
        Append readAppend = (Append) received.get(1);
        assertEquals(msg.data.readableBytes(), readAppend.data.readableBytes());
        assertEquals(content.length + TYPE_PLUS_LENGTH_SIZE, readAppend.data.readableBytes());
    }

    @Test
    public void testBlockTimeout() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        @Cleanup("release")
        val received = new ReceivedCommands();
        Mockito.when(ch.writeAndFlush(Mockito.any())).thenAnswer(i -> {
            commandEncoder.encode(ctx, i.getArgument(0), fakeNetwork);
            return null;
        });
        commandEncoder.encode(ctx, msg, fakeNetwork);
        Thread.sleep(idBatchSizeTrackerMap.get(1L).getBatchTimeout() + 100);
        read(fakeNetwork, received);
        assertEquals(2, received.size());
        Append readAppend = (Append) received.get(1);
        verifyNoExcessData(readAppend.getData());
        assertEquals(msg.data.readableBytes(), readAppend.data.readableBytes());
        assertEquals(content.length + TYPE_PLUS_LENGTH_SIZE, readAppend.data.readableBytes());
    }

    @Test
    public void testAppendComplete() throws Exception {
        byte[] content = new byte[256];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        @Cleanup("release")
        val received = new ReceivedCommands();
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        commandEncoder.encode(ctx, new Append("segment", writerId, 1, event, 1), fakeNetwork);
        commandEncoder.encode(ctx, new Append("segment", writerId, 2, event, 1), fakeNetwork);
        commandEncoder.encode(ctx, new Append("segment", writerId, 3, event, 1), fakeNetwork);
        commandEncoder.encode(ctx, new Append("segment", writerId, 4, event, 1), fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(2, received.size());
        Append readAppend = (Append) received.get(1);
        assertEquals((256  + TYPE_PLUS_LENGTH_SIZE) * 4L, readAppend.data.readableBytes());
        assertEquals((content.length + TYPE_PLUS_LENGTH_SIZE) * 4L, readAppend.data.readableBytes());
    }

    @Test
    public void testSessionFlush() throws Exception {
        final UUID writerId2 = new UUID(1, 3);
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        @Cleanup("release")
        val received = new ReceivedCommands();
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        setupAppend = new SetupAppend(10, writerId2, "segment2", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        Append msg = new Append("segment", writerId, 1, event, 1);
        commandEncoder.encode(ctx, msg, fakeNetwork);
        content = new byte[8192];
        Arrays.fill(content, (byte) 1);
        event = new Event(Unpooled.wrappedBuffer(content));
        for (int i = 1; i < 129; i++) {
            commandEncoder.encode(ctx, new Append("segment2", writerId2, i, event, 10), fakeNetwork);
        }
        read(fakeNetwork, received);
        assertEquals(4, received.size());
        Append readAppend = (Append) received.get(3);
        assertEquals((8192 + TYPE_PLUS_LENGTH_SIZE) * 128L, readAppend.data.readableBytes());
        assertEquals((content.length + TYPE_PLUS_LENGTH_SIZE) * 128L, readAppend.data.readableBytes());
    }

    @Test
    public void testSessionFlushAll() throws Exception {
        final UUID writerId2 = new UUID(1, 3);
        final UUID writerId3 = new UUID(1, 4);
        final UUID writerId4 = new UUID(1, 5);
        final UUID writerId5 = new UUID(1, 6);
        final UUID writerId6 = new UUID(1, 7);
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(appendBlockSize));
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        @Cleanup("release")
        val received = new ReceivedCommands();
        Mockito.when(ch.writeAndFlush(Mockito.any())).thenAnswer(i -> {
            commandEncoder.encode(ctx, i.getArgument(0), fakeNetwork);
            return null;
        });

        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        setupAppend = new SetupAppend(10, writerId2, "segment2", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        setupAppend = new SetupAppend(11, writerId3, "segment3", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        setupAppend = new SetupAppend(12, writerId4, "segment4", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        setupAppend = new SetupAppend(13, writerId5, "segment5", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        setupAppend = new SetupAppend(14, writerId6, "segment6", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        appendDecoder.processCommand(setupAppend);
        Append msg = new Append("segment", writerId, 1, event, 1);
        commandEncoder.encode(ctx, msg, fakeNetwork);
        content = new byte[1024 * 1023];
        Arrays.fill(content, (byte) 1);
        event = new Event(Unpooled.wrappedBuffer(content));
        commandEncoder.encode(ctx, new Append("segment2", writerId2, 1, event, 10), fakeNetwork);
        commandEncoder.encode(ctx, new Append("segment3", writerId3, 1, event, 11), fakeNetwork);
        commandEncoder.encode(ctx, new Append("segment4", writerId4, 1, event, 12), fakeNetwork);
        commandEncoder.encode(ctx, new Append("segment5", writerId5, 1, event, 13), fakeNetwork);
        commandEncoder.encode(ctx, new Append("segment6", writerId6, 1, event, 14), fakeNetwork);
        Thread.sleep(idBatchSizeTrackerMap.get(1L).getBatchTimeout() + 100);
        read(fakeNetwork, received);
        assertEquals(12, received.size());
        Append readAppend = (Append) received.get(11);
        assertEquals(1024 * 1023 + TYPE_PLUS_LENGTH_SIZE, readAppend.data.readableBytes());
        assertEquals(content.length + TYPE_PLUS_LENGTH_SIZE, readAppend.data.readableBytes());
    }

    @Test(expected = InvalidMessageException.class)
    public void testInvalidAppendEventNumber() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(3));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        SetupAppend setupAppend = new SetupAppend(1, writerId, "segment", "");
        commandEncoder.encode(ctx, setupAppend, fakeNetwork);
        commandEncoder.encode(ctx, msg, fakeNetwork);
        Append invalidMsg = new Append("segment", writerId, 0, event, 1);
        commandEncoder.encode(ctx, invalidMsg, fakeNetwork);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAppendConditional() throws Exception {
        byte[] content = new byte[100];
        Arrays.fill(content, (byte) 1);
        Event event = new Event(Unpooled.wrappedBuffer(content));
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(3));
        Append msg = new Append("segment", writerId, 1, event, 1);
        CommandEncoder commandEncoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
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
        sendAndVerifyEvents(streamName, writerId, numEvents, size, 2);
    }
    
    @Test 
    public void testSwitchingWriters() throws Exception {
        int size = 20; //Used to force a minimum size
        UUID writer1 = new UUID(1, 1);
        UUID writer2 = new UUID(2, 2);
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(size));

        CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        SetupAppend setupAppend = new SetupAppend(1, writer1, streamName, "");
        encoder.encode(ctx, setupAppend, fakeNetwork);
        setupAppend = new SetupAppend(1, writer2, streamName, "");
        encoder.encode(ctx, setupAppend, fakeNetwork);
        Append msg1 = new Append(streamName, writer1, 1, new Event(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3, 4 })), 1);
        encoder.encode(ctx, msg1, fakeNetwork);
        Append msg2 = new Append(streamName, writer2, 1, new Event(Unpooled.wrappedBuffer(new byte[] { 1, 2, 3, 4 })), 1);
        encoder.encode(ctx, msg2, fakeNetwork);
    }
    
    @Test
    public void testZeroSizeAppend() throws Exception {
        int size = 0; //Used to force a minimum size
        idBatchSizeTrackerMap.remove(1L);
        idBatchSizeTrackerMap.put(1L, new FixedBatchSizeTracker(size));
        CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        SetupAppend setupAppend = new SetupAppend(1, writerId, streamName, "");
        encoder.encode(ctx, setupAppend, fakeNetwork);
        Append msg = new Append(streamName, writerId, 1, 1, Unpooled.EMPTY_BUFFER, null, 1);
        assertEquals(0, msg.data.readableBytes());
        encoder.encode(ctx, msg, fakeNetwork);
        Append msg2 = new Append(streamName, writerId, 2, 1, Unpooled.EMPTY_BUFFER, null, 1);
        Append msg3 = new Append(streamName, writerId, 3, 1, Unpooled.EMPTY_BUFFER, null, 1);
        encoder.encode(ctx, msg2, fakeNetwork);
        encoder.encode(ctx, msg3, fakeNetwork);
        @Cleanup("release")
        val received = new ReceivedCommands();
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
        CommandEncoder encoder = new CommandEncoder(idBatchSizeTrackerMap::get, NO_OP_METRIC_NOTIFIER);
        SetupAppend setupAppend = new SetupAppend(1, writerId, streamName, "");
        encoder.encode(ctx, setupAppend, fakeNetwork);
        Append msg = new Append(streamName, writerId, 1, 1, Unpooled.EMPTY_BUFFER, null, 1);
        assertEquals(0, msg.data.readableBytes());
        encoder.encode(ctx, msg, fakeNetwork);
        Append msg2 = new Append(streamName, writerId, 2, 1, Unpooled.EMPTY_BUFFER, null, 1);
        Append msg3 = new Append(streamName, writerId, 3, 1, Unpooled.EMPTY_BUFFER, null, 1);
        encoder.encode(ctx, msg2, fakeNetwork);
        encoder.encode(ctx, msg3, fakeNetwork);
        encoder.encode(ctx, new KeepAlive(), fakeNetwork);
        @Cleanup("release")
        val received = new ReceivedCommands();
        read(fakeNetwork, received);
        assertEquals(3, received.size());
        assertEquals(setupAppend, received.get(0));
        assertEquals(new Append(streamName, writerId, 3, 3, Unpooled.EMPTY_BUFFER, null, 1), received.get(1));
        verifyNoExcessData(((Append) received.get(1)).getData());
        assertEquals(new KeepAlive(), received.get(2));
    }

    @Test
    public void testAppendAtBlockBound() throws Exception {
        int size = appendBlockSize;
        @Cleanup("release")
        val received = new ReceivedCommands();
        setupAppend(streamName, writerId, fakeNetwork);

        append(streamName, writerId, size, 1, size, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        append(streamName, writerId, size + size / 2, 2, size / 2, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());

        KeepAlive keepAlive = new KeepAlive();
        encoder.encode(ctx, keepAlive, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(3, received.size());

        Append one = (Append) received.get(0);
        Append two = (Append) received.get(1);
        verifyNoExcessData(one.getData());
        verifyNoExcessData(two.getData());
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

    @Test
    public void testConditionalAppend() throws Exception {
        @Cleanup("release")
        val received = new ReceivedCommands();
        setupAppend(streamName, writerId, fakeNetwork);

        val event = new Event(Unpooled.wrappedBuffer(new byte[appendBlockSize]));
        val append1 = new WireCommands.ConditionalAppend(writerId, 10, 123, event, 1);
        encoder.encode(ctx, append1, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());
        Append readAppend1 = (Append) received.get(0);
        assertEquals((long) readAppend1.expectedLength, append1.expectedOffset);
        assertEquals(event.getAsByteBuf(), readAppend1.getData());
    }

    @Test
    public void testConditionalBlockEnd() throws Exception {
        @Cleanup("release")
        val received = new ReceivedCommands();
        setupAppend(streamName, writerId, fakeNetwork);

        val data = Unpooled.wrappedBuffer(new byte[appendBlockSize]);
        val append1 = new WireCommands.ConditionalBlockEnd(writerId, 10, 123, data, 1);
        encoder.encode(ctx, append1, fakeNetwork);
        read(fakeNetwork, received);
        assertEquals(1, received.size());
        Append readAppend1 = (Append) received.get(0);
        assertEquals((long) readAppend1.expectedLength, append1.expectedOffset);
        assertEquals(data, readAppend1.getData());
    }

    private void setupAppend(String testStream, UUID writerId, ByteBuf fakeNetwork) throws Exception {
        SetupAppend setupAppend = new SetupAppend(1, writerId, testStream, "");
        encoder.encode(ctx, setupAppend, fakeNetwork);
        WireCommand command = CommandDecoder.parseCommand(fakeNetwork);
        assertTrue(appendDecoder.acceptInboundMessage(command));
        assertEquals(setupAppend, appendDecoder.processCommand(command));
    }

    private long append(String segment, UUID writerId, long offset, int messageNumber, int length, ByteBuf out)
            throws Exception {
        byte[] content = new byte[length];
        Arrays.fill(content, (byte) messageNumber);
        Event event = new Event(Unpooled.wrappedBuffer(content, 0, length));
        Append msg = new Append(segment, writerId, messageNumber, event, 1);
        assertEquals(length + WireCommands.TYPE_PLUS_LENGTH_SIZE, msg.data.readableBytes());
        encoder.encode(ctx, msg, out);
        assertEquals(length + WireCommands.TYPE_PLUS_LENGTH_SIZE, msg.data.readableBytes());
        return offset + length;
    }

    private void read(ByteBuf in, ReceivedCommands received) throws Exception {
        ByteBuf segmented = (ByteBuf) lengthDecoder.decode(null, in);
        while (segmented != null) {
            int before = received.results.size();
            WireCommand command = CommandDecoder.parseCommand(segmented);
            if (appendDecoder.acceptInboundMessage(command)) {
                Request request = appendDecoder.processCommand(command);
                if (request != null) {
                    received.results.add(request);
                }
            } else {
                received.results.add(command);
            }
            assertTrue(received.results.size() == before || received.results.size() == before + 1);
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
            verifyNoExcessData(append.getData());
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

    private void verifyNoExcessData(ByteBuf buf) {
        int allocatedSize = getAllocatedSize(buf);
        assertEquals("Append.getData() has excess memory allocated.", buf.readableBytes(), allocatedSize);
    }

    private int getAllocatedSize(ByteBuf buf) {
        if (buf.hasArray()) {
            // Array-backed buffer. The length of the array is the allocated size.
            return buf.array().length;
        } else if (buf instanceof CompositeByteBuf) {
            // Composite ByteBuf. Sum up component allocated data.
            AtomicInteger allocated = new AtomicInteger();
            ((CompositeByteBuf) buf).iterator().forEachRemaining(b -> allocated.addAndGet(getAllocatedSize(b)));
            return allocated.get();
        } else {
            // Other type of buffer (direct?). Our best guess is invoking capacity() which should return the right value.
            return buf.capacity();
        }
    }

    private static class BufferReleaseVerifier implements AutoCloseable {
        private final ArrayList<Supplier<Integer>> getRefCounts;

        BufferReleaseVerifier() {
            this.getRefCounts = new ArrayList<>();
        }

        BufferReleaseVerifier include(WireCommands.ReleasableCommand cmd, ByteBuf buf) {
            cmd.requireRelease();
            Supplier<Integer> s = buf::refCnt;
            AssertExtensions.assertGreaterThan("Expecting a referenced buffer.", 0, s.get());
            this.getRefCounts.add(s);
            return this;
        }


        @Override
        public void close() {
            for (int i = 0; i < this.getRefCounts.size(); i++) {
                Assert.assertEquals("Referenced buffer found at position " + i, 0, (int) this.getRefCounts.get(i).get());
            }
        }
    }

    private static class ReceivedCommands {
        final List<Object> results = new ArrayList<>();

        int size() {
            return this.results.size();
        }

        Object get(int index) {
            return this.results.get(index);
        }

        /**
         * Releases all appends. This is consistent with the Append Processor behavior in the Segment Store: once it
         * is done processing the append, it will release it. We need to simulate the same behavior so that we may validate
         * the following cases:
         * - No reference undercounts (trying to use a {@link ByteBuf} that has {@link ByteBuf#refCnt()} 0.
         * - No leaked {@link ByteBuf}s. In combination with {@link LeakDetectorTestSuite}, this verifies that at the end
         * the test(s), {@link ByteBuf#refCnt()} is 0 when the {@link ByteBuf}s are garbage collected.
         */
        void release() {
            for (Object o : results) {
                if (o instanceof Append) {
                    ((Append) o).getData().release();
                }
            }
        }
    }
}
