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
package io.pravega.client.segment.impl;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.stream.impl.Orderer;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.ObjectClosedException;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertBlocks;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentInputStreamTest extends LeakDetectorTestSuite {

    private final Segment segment = new Segment("scope", "foo", 0);
    private final long requestId = 1;
    
    private static class TestAsyncSegmentInputStream extends AsyncSegmentInputStream {
        AtomicBoolean closed = new AtomicBoolean(false);
        private final Vector<CompletableFuture<WireCommands.SegmentRead>> readResults;
        private final AtomicInteger readIndex = new AtomicInteger(-1);

        TestAsyncSegmentInputStream(Segment segment, int expectedReads) {
            super(segment);
            readResults = new Vector<>();
            for (int i = 0; i < expectedReads; i++) {
                readResults.addElement(new CompletableFuture<>());
            }
        }

        @Override
        public CompletableFuture<SegmentRead> read(long offset, int length) {
            int i = readIndex.incrementAndGet();
            return readResults.get(i);
        }

        void complete(int readNumber, WireCommands.SegmentRead readResult) {
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
        public boolean isClosed() {
            return closed.get();
        }
    }

    private ByteBuf createEventFromData(byte[] data) {
        ByteBuffer wireData = ByteBuffer.allocate(data.length + WireCommands.TYPE_PLUS_LENGTH_SIZE);
        wireData.putInt(WireCommandType.EVENT.getCode());
        wireData.putInt(data.length);
        wireData.put(data);
        wireData.flip();
        return Unpooled.wrappedBuffer(wireData);
    }
    
    @Test
    public void testConfigBufferSize() {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController mockController = new MockController("localhost", -1, connectionFactory, false);
        SegmentInputStreamFactoryImpl streamFactory = new SegmentInputStreamFactoryImpl(mockController, connectionFactory);
        @Cleanup
        EventSegmentReader streamSmall = streamFactory.createEventReaderForSegment(segment, 100, null, Long.MAX_VALUE);
        int bufferSize = ((SegmentInputStreamImpl) ((EventSegmentReaderImpl) streamSmall).getIn()).getBufferSize();
        assertEquals(SegmentInputStreamImpl.MIN_BUFFER_SIZE, bufferSize);
        @Cleanup
        EventSegmentReader streamNormal = streamFactory.createEventReaderForSegment(segment, 1024 * 1024, null, Long.MAX_VALUE);
        bufferSize = ((SegmentInputStreamImpl) ((EventSegmentReaderImpl) streamNormal).getIn()).getBufferSize();
        assertEquals(1024 * 1024, bufferSize);
        @Cleanup
        EventSegmentReader streamXL = streamFactory.createEventReaderForSegment(segment, 1000 * 1024 * 1024, null, Long.MAX_VALUE);
        bufferSize = ((SegmentInputStreamImpl) ((EventSegmentReaderImpl) streamXL).getIn()).getBufferSize();
        assertEquals(SegmentInputStreamImpl.MAX_BUFFER_SIZE, bufferSize);
    }

    @Test
    public void testRead() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        ByteBuffer read = assertBlocks(() -> stream.read(),
                () -> fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(), requestId)));
        assertEquals(ByteBuffer.wrap(data), read);
        read = assertBlocks(() -> stream
                .read(), () -> fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.capacity(), false,
                                                                                    false, wireData.slice(), requestId)));
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testSmallerThanNeededRead() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 5);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, 2), requestId));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, wireData.slice(2, 7), requestId));
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 9, false, false, wireData.slice(9, 2), requestId));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), 11, false, false, wireData.slice(11, wireData.capacity() - 11), requestId));
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testLongerThanRequestedRead() throws EndOfSegmentException, SegmentTruncatedException {
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
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, Unpooled.wrappedBuffer(wireData.slice()), requestId));
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        for (int i = 0; i < numEntries; i++) {
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }
        ByteBuffer read = assertBlocks(() -> stream.read(), () -> {
            fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.capacity(), false, false,
                                                                 createEventFromData(data), requestId));
        });
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test(timeout = 10000)
    public void testTimeout() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 7);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);

        assertBlocks(() -> stream.read(),
                     () -> fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false,
                                                                                wireData.slice(), requestId)));
        ByteBuffer read = stream.read(10);
        assertNull(read);
        fakeNetwork.completeExceptionally(1, new ConnectionFailedException());
        AssertExtensions.assertThrows(ConnectionFailedException.class, () -> stream.read());
        stream.read(10);
        assertNull(read);
        read = stream.read(10);
        assertNull(read);
    }
    
    
    @Test
    public void testExceptionRecovery() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 7);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, 2), requestId));
        fakeNetwork.completeExceptionally(1, new ConnectionFailedException());
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, 2), requestId));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, wireData.slice(2, 7), requestId));
        fakeNetwork.complete(4, new WireCommands.SegmentRead(segment.getScopedName(), 9, false, false, wireData.slice(9, 2), requestId));
        fakeNetwork.complete(5, new WireCommands.SegmentRead(segment.getScopedName(), 11, false, false, wireData.slice(11, wireData.capacity() - 11), requestId));
        AssertExtensions.assertThrows(ConnectionFailedException.class, () -> stream.read());
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read);
    }
    
    @Test
    public void testStreamTruncated() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 6);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, 2), requestId));
        fakeNetwork.completeExceptionally(1, new SegmentTruncatedException());
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, wireData.slice(2, 7), requestId));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), 9, false, false, wireData.slice(9, 2), requestId));
        fakeNetwork.complete(4, new WireCommands.SegmentRead(segment.getScopedName(), 11, false, false, wireData.slice(11, wireData.capacity() - 11), requestId));
        AssertExtensions.assertThrows(SegmentTruncatedException.class, () -> stream.read());
        AssertExtensions.assertThrows(SegmentTruncatedException.class, () -> stream.read());
    }
    
    @Test
    public void testStreamTruncatedWithPartialEvent() {
        val trailingData = Unpooled.wrappedBuffer(new byte[]{0, 1});
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, trailingData.slice(), requestId));
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream.read());
    }
    
    @Test
    public void testIsSegmentReady() throws EndOfSegmentException, SegmentTruncatedException {
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
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        assertFalse(stream.isSegmentReady());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, true, false, Unpooled.wrappedBuffer(wireData.slice()), requestId));
        for (int i = 0; i < numEntries; i++) {
            assertTrue(stream.isSegmentReady());
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }
        assertFalse(stream.isSegmentReady());
        assertBlocks(() -> stream.read(), () -> {
            fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.capacity(), false, false, createEventFromData(data), requestId));
        });
        assertFalse(stream.isSegmentReady());
    }
    
    @Test
    public void testEndOfSegment() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        EventSegmentReaderImpl stream1 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        assertFalse(stream1.isSegmentReady());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice(0, 0), requestId));
        assertTrue(stream1.isSegmentReady());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream1.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        EventSegmentReaderImpl stream2 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        assertFalse(stream2.isSegmentReady());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice(), requestId));
        assertTrue(stream2.isSegmentReady());
        assertEquals(ByteBuffer.wrap(data), stream2.read());
        assertTrue(stream2.isSegmentReady());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream2.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        EventSegmentReaderImpl stream3 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        assertFalse(stream3.isSegmentReady());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(), requestId));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.readableBytes(), false, true, wireData.slice(0, 0), requestId));
        assertTrue(stream3.isSegmentReady());
        assertEquals(ByteBuffer.wrap(data), stream3.read());
        assertTrue(stream3.isSegmentReady());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream3.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        EventSegmentReaderImpl stream4 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        assertFalse(stream4.isSegmentReady());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, 0), requestId));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice(), requestId));
        assertEquals(ByteBuffer.wrap(data), stream4.read());
        assertTrue(stream4.isSegmentReady());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream4.read());

        fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        EventSegmentReaderImpl stream5 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        assertFalse(stream5.isSegmentReady());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, 2), requestId));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, wireData.slice(2, 2), requestId));
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 4, false, true, wireData.slice(4, wireData.capacity() - 4), requestId));
        assertEquals(ByteBuffer.wrap(data), stream5.read());
        assertTrue(stream5.isSegmentReady());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream5.read());
    }
    
    @Test
    public void testBlockingEndOfSegment() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        assertBlocks(() -> {
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }, () -> {
            fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, 0), requestId));
            fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice(), requestId));
        });
    }

    @Test
    public void testSetOffset() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data1 = new byte[]{0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[]{6, 7, 8, 9};
        val wireData1 = createEventFromData(data1);
        val wireData2 = createEventFromData(data2);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 5);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData1.slice(0, wireData1.readableBytes()), requestId));
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data1), read);
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData1.slice(0, wireData1.readableBytes()), requestId));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), wireData1.readableBytes(), false, false, wireData2.slice(0, wireData2.readableBytes()), requestId));
        stream.setOffset(0);
        read = stream.read();
        assertEquals(ByteBuffer.wrap(data1), read);
        read = stream.read();
        assertEquals(ByteBuffer.wrap(data2), read);
    }

    @Test(timeout = 5000)
    public void testClose() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(0, wireData.readableBytes()), requestId));
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork, 0);
        stream.close();
        AssertExtensions.assertThrows(ObjectClosedException.class, () -> stream.read());
    }

    @Test(expected = EndOfSegmentException.class)
    public void testReadWithEndOffset() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        int wireDataSize = wireData.readableBytes(); //size of the data with header size.

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, wireDataSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        0, false, false, wireData.slice(), requestId)));
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 0, wireDataSize,
                SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);

        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read); //verify we are reading the data.
        verify(mockAsyncInputStream, times(1)).read(0L, wireDataSize); //ensure there is one invocation.
        stream.read(); // this should throw EndOfSegmentExceptiono as we have reached the endOffset
    }

    @Test
    public void testReadWithEndOffsetWithSmallerReads() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        int wireDataSize = wireData.readableBytes(); //size of the data with header size.

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, wireDataSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        0, false, false, wireData.slice(0, 2), requestId)));
        when(mockAsyncInputStream.read(2, 16))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        2, false, false, wireData.slice(2, wireDataSize - 2), requestId)));
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 0, wireDataSize,
                SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);

        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read); //verify we are reading the data.
        verify(mockAsyncInputStream, times(1)).read(0L, wireDataSize);
        verify(mockAsyncInputStream, times(1)).read(2L, wireDataSize - 2);
    }

    @Test
    public void testReadWithEndOffsetWithDataGreaterThanBuffer() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        int wireDataSize = wireData.readableBytes(); //size of the data with header size.
        int bufferSize = wireDataSize / 2; //buffer is half the data length

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, bufferSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        0, false, false, wireData.slice(0, bufferSize), requestId)));
        when(mockAsyncInputStream.read(bufferSize, wireDataSize - bufferSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        bufferSize, false, false, wireData.slice(bufferSize, wireDataSize - bufferSize), requestId)));

        //Create a SegmentInputStream where the Buffer can hold only part of the data.
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 0, wireDataSize, bufferSize);

        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read); //verify we are reading the data.
        verify(mockAsyncInputStream, times(1)).read(0L, bufferSize);
        verify(mockAsyncInputStream, times(1)).read(bufferSize, wireDataSize - bufferSize);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadWithInvalidEndOffset() {
        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);

        //Set the end offset which is less than startOffset
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 10, 9, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadWithNegativeEndOffset() {
        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        @Cleanup
        EventSegmentReaderImpl stream = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 0,
                -2, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }
    
    @Test
    public void testOrderer() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        
        TestAsyncSegmentInputStream fakeNetwork1 = new TestAsyncSegmentInputStream(segment, 1);
        TestAsyncSegmentInputStream fakeNetwork2 = new TestAsyncSegmentInputStream(segment, 2);
        TestAsyncSegmentInputStream fakeNetwork3 = new TestAsyncSegmentInputStream(segment, 3);
        TestAsyncSegmentInputStream fakeNetwork4 = new TestAsyncSegmentInputStream(segment, 4);
        TestAsyncSegmentInputStream fakeNetwork5 = new TestAsyncSegmentInputStream(segment, 5);
        
        @Cleanup
        EventSegmentReaderImpl stream1 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork1, 0);
        @Cleanup
        EventSegmentReaderImpl stream2 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork2, 0);
        @Cleanup
        EventSegmentReaderImpl stream3 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork3, 0);
        @Cleanup
        EventSegmentReaderImpl stream4 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork4, 0);
        @Cleanup
        EventSegmentReaderImpl stream5 = SegmentInputStreamFactoryImpl.getEventSegmentReader(fakeNetwork5, 0);

        fakeNetwork2.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(), requestId));
        fakeNetwork3.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice(), requestId));
        fakeNetwork4.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, Unpooled.EMPTY_BUFFER, requestId));
        fakeNetwork5.completeExceptionally(0, new SegmentTruncatedException());
        
        Orderer o = new Orderer();
        List<EventSegmentReaderImpl> segments = ImmutableList.of(stream1, stream2, stream3, stream4, stream5);
        assertEquals(stream2, o.nextSegment(segments));
        assertEquals(stream3, o.nextSegment(segments));
        assertEquals(stream4, o.nextSegment(segments));
        assertEquals(stream5, o.nextSegment(segments));
        assertNotNull(stream2.read());
        assertEquals(stream3, o.nextSegment(segments));
        assertEquals(stream4, o.nextSegment(segments));
        assertEquals(stream5, o.nextSegment(segments));
        assertNotNull(stream3.read());
        assertEquals(stream3, o.nextSegment(segments));
        assertEquals(stream4, o.nextSegment(segments));
        assertEquals(stream5, o.nextSegment(segments));
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream3.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream4.read());
        AssertExtensions.assertThrows(SegmentTruncatedException.class, () -> stream5.read());
        assertEquals(stream3, o.nextSegment(segments));
        assertEquals(stream4, o.nextSegment(segments));
        assertEquals(stream5, o.nextSegment(segments));
    }

    @Test
    public void testRefillSize() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        val wireData = createEventFromData(data);
        int wireDataSize = wireData.readableBytes(); // size of the data with header size.
        int bufferSize = SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE;

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, bufferSize)).thenReturn(
                completedFuture(new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(), requestId)));

        int expectedReadSize = bufferSize - wireDataSize;

        when(mockAsyncInputStream.read(wireDataSize, expectedReadSize)).thenReturn(
                completedFuture(new SegmentRead(segment.getScopedName(), wireDataSize, false, false, wireData.slice(), requestId)));

        // Verify that it requests enough data to fill the buffer.
        @Cleanup
        EventSegmentReaderImpl stream1 = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 0);

        ByteBuffer read = stream1.read();
        assertEquals(ByteBuffer.wrap(data), read); 
        verify(mockAsyncInputStream, times(1)).read(0L, bufferSize);
        verify(mockAsyncInputStream, times(1)).read(wireDataSize, expectedReadSize);

        when(mockAsyncInputStream.read(0, wireDataSize)).thenReturn(
            completedFuture(new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(), requestId)));

        // Verify it won't read beyond it's limit.
        @Cleanup
        EventSegmentReaderImpl stream2 = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 0, wireDataSize, bufferSize);

        read = stream2.read();
        assertEquals(ByteBuffer.wrap(data), read);
        verify(mockAsyncInputStream, times(1)).read(0L, wireDataSize);

        // Verify it works with a small buffer.
        when(mockAsyncInputStream.read(0, 100)).thenReturn(
                                                           completedFuture(new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice(), requestId)));
        @Cleanup
        EventSegmentReaderImpl stream3 = SegmentInputStreamFactoryImpl.getEventSegmentReader(mockAsyncInputStream, 0, Long.MAX_VALUE, 100);

        read = stream3.read();
        assertEquals(ByteBuffer.wrap(data), read); 
        verify(mockAsyncInputStream, times(1)).read(0L, 100);
    }

}
