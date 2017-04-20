/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.WireCommandType;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.common.util.ByteBufferUtils;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.Async;
import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.Data;
import org.junit.Ignore;
import org.junit.Test;

import static com.emc.pravega.testcommon.Async.testBlocking;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentInputStreamTest {

    private final Segment segment = new Segment("scope", "foo", 0);
    
    private static class TestAsyncSegmentInputStream extends AsyncSegmentInputStream {
        AtomicBoolean closed = new AtomicBoolean(false);
        private final Vector<CompletableFuture<SegmentRead>> readResults;
        private final AtomicInteger readIndex = new AtomicInteger(-1);

        TestAsyncSegmentInputStream(Segment segment, int expectedReads) {
            super(segment);
            readResults = new Vector<>();
            for (int i = 0; i < expectedReads; i++) {
                readResults.addElement(new CompletableFuture<>());
            }
        }

        @Data
        private class ReadFutureImpl implements ReadFuture {
            final int num;
            int attempt = 0;

            @Override
            public boolean isSuccess() {
                return FutureHelpers.isSuccessful(readResults.get(num + attempt));
            }

            @Override
            public boolean await(long timeout) {
                FutureHelpers.await(readResults.get(num + attempt), timeout);
                return readResults.get(num + attempt).isDone();
            }
        }

        @Override
        public ReadFuture read(long offset, int length) {
            int i = readIndex.incrementAndGet();
            return new ReadFutureImpl(i);
        }

        void complete(int readNumber, SegmentRead readResult) {
            readResults.get(readNumber).complete(readResult);
        }

        void completeExceptionally(int readNumber, Exception e) {
            readResults.get(readNumber).completeExceptionally(e);
        }

        @Override
        public void close() {
            closed.set(true);
        }

        @Override
        public SegmentRead getResult(ReadFuture ongoingRead) {
            ReadFutureImpl read = (ReadFutureImpl) ongoingRead;
            CompletableFuture<SegmentRead> future = readResults.get(read.num + read.attempt);
            if (FutureHelpers.await(future)) {
                return future.getNow(null);
            } else {
                read.attempt++;
                return FutureHelpers.getAndHandleExceptions(future, RuntimeException::new);
            }
        }

        @Override
        public CompletableFuture<StreamSegmentInfo> getSegmentInfo() {
            throw new UnsupportedOperationException();
        }
    }

    private ByteBuffer createEventFromData(byte[] data) {
        ByteBuffer wireData = ByteBuffer.allocate(data.length + WireCommands.TYPE_PLUS_LENGTH_SIZE);
        wireData.putInt(WireCommandType.EVENT.getCode());
        wireData.putInt(data.length);
        wireData.put(data);
        wireData.flip();
        return wireData;
    }

    @Test
    public void testRead() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        ByteBuffer read = testBlocking(() -> stream.read(),
                () -> fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice())));
        assertEquals(ByteBuffer.wrap(data), read);
        read = testBlocking(() -> stream
                .read(), () -> fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), wireData.capacity(), false, false, wireData.slice())));
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testSmallerThanNeededRead() throws EndOfSegmentException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 5);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 7)));
        fakeNetwork.complete(2, new SegmentRead(segment.getScopedName(), 9, false, false, ByteBufferUtils.slice(wireData, 9, 2)));
        fakeNetwork
                .complete(3, new SegmentRead(segment.getScopedName(), 11, false, false, ByteBufferUtils.slice(wireData, 11, wireData.capacity() - 11)));
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testLongerThanRequestedRead() throws EndOfSegmentException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        int numEntries = SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE / data.length;

        ByteBuffer wireData = ByteBuffer.allocate((data.length + WireCommands.TYPE_PLUS_LENGTH_SIZE) * numEntries);
        for (int i = 0; i < numEntries; i++) {
            wireData.putInt(WireCommandType.EVENT.getCode());
            wireData.putInt(data.length);
            wireData.put(data);
        }
        wireData.flip();
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        for (int i = 0; i < numEntries; i++) {
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }
        ByteBuffer read = testBlocking(() -> stream.read(), () -> {
            fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), wireData.capacity(), false, false, createEventFromData(data)));
        });
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testExceptionRecovery() throws EndOfSegmentException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 6);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.completeExceptionally(1, new ConnectionFailedException());
        fakeNetwork.complete(2, new SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 7)));
        fakeNetwork.complete(3, new SegmentRead(segment.getScopedName(), 9, false, false, ByteBufferUtils.slice(wireData, 9, 2)));
        fakeNetwork.complete(4, new SegmentRead(segment.getScopedName(), 11, false, false, ByteBufferUtils.slice(wireData, 11, wireData.capacity() - 11)));
        try {
            stream.read();
            fail();
        } catch (RuntimeException e) {
            //Expected
        }
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read);
    }
    
    @Test
    public void testReadWithoutBlocking() throws EndOfSegmentException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        int numEntries = SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE / data.length;

        ByteBuffer wireData = ByteBuffer.allocate((data.length + WireCommands.TYPE_PLUS_LENGTH_SIZE) * numEntries);
        for (int i = 0; i < numEntries; i++) {
            wireData.putInt(WireCommandType.EVENT.getCode());
            wireData.putInt(data.length);
            wireData.put(data);
        }
        wireData.flip();
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        assertFalse(stream.canReadWithoutBlocking());
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, true, false, wireData.slice()));
        for (int i = 0; i < numEntries; i++) {
            assertTrue(stream.canReadWithoutBlocking());
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }
        assertFalse(stream.canReadWithoutBlocking());
        testBlocking(() -> stream.read(), () -> {
            fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), wireData.capacity(), false, false, createEventFromData(data)));
        });
        assertFalse(stream.canReadWithoutBlocking());
    }
    
    @Test
    public void testEndOfSegment() throws EndOfSegmentException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        SegmentInputStreamImpl stream1 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, true, ByteBufferUtils.slice(wireData, 0, 0)));
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream1.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        SegmentInputStreamImpl stream2 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice()));
        assertEquals(ByteBuffer.wrap(data), stream2.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream2.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        SegmentInputStreamImpl stream3 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), wireData.remaining(), false, true, ByteBufferUtils.slice(wireData, 0, 0)));
        assertEquals(ByteBuffer.wrap(data), stream3.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream3.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        SegmentInputStreamImpl stream4 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 0)));
        fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice()));
        assertEquals(ByteBuffer.wrap(data), stream4.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream4.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        SegmentInputStreamImpl stream5 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 2)));
        fakeNetwork.complete(2, new SegmentRead(segment.getScopedName(), 4, false, true, ByteBufferUtils.slice(wireData, 4,  wireData.capacity() - 4)));
        assertEquals(ByteBuffer.wrap(data), stream5.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream5.read());
    }
    
    @Test
    public void testBlockingEndOfSegment() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        Async.testBlocking(() -> {
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }, () -> {
            fakeNetwork.complete(0, new SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 0)));
            fakeNetwork.complete(1, new SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice()));
        });
    }

    @Test
    @Ignore
    public void testConfigChange() {
        fail();
    }

    @Test
    @Ignore
    public void testSetOffset() {
        fail();
    }

    @Test
    @Ignore
    public void testClose() {
        fail();
    }

    @Test
    @Ignore
    public void testAutoClose() {
        fail();
    }
}
