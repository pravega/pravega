/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertBlocks;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentInputStreamTest {

    private final Segment segment = new Segment("scope", "foo", 0);
    
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
        ByteBuffer read = assertBlocks(() -> stream.read(),
                () -> fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice())));
        assertEquals(ByteBuffer.wrap(data), read);
        read = assertBlocks(() -> stream
                .read(), () -> fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.capacity(), false, false, wireData.slice())));
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testSmallerThanNeededRead() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 5);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 7)));
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 9, false, false, ByteBufferUtils.slice(wireData, 9, 2)));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), 11, false, false, ByteBufferUtils.slice(wireData, 11, wireData.capacity() - 11)));
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
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        for (int i = 0; i < numEntries; i++) {
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }
        ByteBuffer read = assertBlocks(() -> stream.read(), () -> {
            fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.capacity(), false, false, createEventFromData(data)));
        });
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test(timeout = 10000)
    public void testTimeout() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 7);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);

        assertBlocks(() -> stream.read(),
                     () -> fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice())));
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
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 7);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.completeExceptionally(1, new ConnectionFailedException());
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 7)));
        fakeNetwork.complete(4, new WireCommands.SegmentRead(segment.getScopedName(), 9, false, false, ByteBufferUtils.slice(wireData, 9, 2)));
        fakeNetwork.complete(5, new WireCommands.SegmentRead(segment.getScopedName(), 11, false, false, ByteBufferUtils.slice(wireData, 11, wireData.capacity() - 11)));
        AssertExtensions.assertThrows(ConnectionFailedException.class, () -> stream.read());
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read);
    }
    
    @Test
    public void testStreamTruncated() throws EndOfSegmentException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 6);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.completeExceptionally(1, new SegmentTruncatedException());
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 7)));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), 9, false, false, ByteBufferUtils.slice(wireData, 9, 2)));
        fakeNetwork.complete(4, new WireCommands.SegmentRead(segment.getScopedName(), 11, false, false, ByteBufferUtils.slice(wireData, 11, wireData.capacity() - 11)));
        AssertExtensions.assertThrows(SegmentTruncatedException.class, () -> stream.read());
        AssertExtensions.assertThrows(SegmentTruncatedException.class, () -> stream.read());
    }
    
    @Test
    public void testReadWithoutBlocking() throws EndOfSegmentException, SegmentTruncatedException {
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
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, true, false, wireData.slice()));
        for (int i = 0; i < numEntries; i++) {
            assertTrue(stream.canReadWithoutBlocking());
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }
        assertFalse(stream.canReadWithoutBlocking());
        assertBlocks(() -> stream.read(), () -> {
            fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.capacity(), false, false, createEventFromData(data)));
        });
        assertFalse(stream.canReadWithoutBlocking());
    }
    
    @Test
    public void testEndOfSegment() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        SegmentInputStreamImpl stream1 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, ByteBufferUtils.slice(wireData, 0, 0)));
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream1.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        SegmentInputStreamImpl stream2 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice()));
        assertEquals(ByteBuffer.wrap(data), stream2.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream2.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        SegmentInputStreamImpl stream3 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), wireData.remaining(), false, true, ByteBufferUtils.slice(wireData, 0, 0)));
        assertEquals(ByteBuffer.wrap(data), stream3.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream3.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        SegmentInputStreamImpl stream4 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 0)));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice()));
        assertEquals(ByteBuffer.wrap(data), stream4.read());
        AssertExtensions.assertThrows(EndOfSegmentException.class, () -> stream4.read());
        
        fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        SegmentInputStreamImpl stream5 = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 2)));
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 4, false, true, ByteBufferUtils.slice(wireData, 4,  wireData.capacity() - 4)));
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
        assertBlocks(() -> {
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }, () -> {
            fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 0)));
            fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice()));
        });
    }

    @Test
    public void testSetOffset() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data1 = new byte[]{0, 1, 2, 3, 4, 5};
        byte[] data2 = new byte[]{6, 7, 8, 9};
        ByteBuffer wireData1 = createEventFromData(data1);
        ByteBuffer wireData2 = createEventFromData(data2);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 5);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData1, 0, wireData1.remaining())));
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data1), read);
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData1, 0, wireData1.remaining())));
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), wireData1.remaining(), false, false, ByteBufferUtils.slice(wireData2, 0, wireData2.remaining())));
        stream.setOffset(0);
        read = stream.read();
        assertEquals(ByteBuffer.wrap(data1), read);
        read = stream.read();
        assertEquals(ByteBuffer.wrap(data2), read);
    }

    @Test(timeout = 5000)
    public void testClose() {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, wireData.remaining())));
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        stream.close();
        AssertExtensions.assertThrows(ObjectClosedException.class, () -> stream.read());
    }

    @Test(expected = EndOfSegmentException.class)
    public void testReadWithEndOffset() throws Exception {
        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        int wireDataSize = wireData.remaining(); //size of the data with header size.

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, wireDataSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        0, false, false, wireData.slice())));
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(mockAsyncInputStream, 0, wireDataSize,
                SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);

        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read); //verify we are reading the data.
        verify(mockAsyncInputStream, times(1)).read(0L, wireDataSize); //ensure there is one invocation.
        stream.read(); // this should throw EndOfSegmentExceptiono as we have reached the endOffset
    }

    @Test
    public void testReadWithEndOffsetWithSmallerReads() throws Exception {
        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        int wireDataSize = wireData.remaining(); //size of the data with header size.

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, wireDataSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        0, false, false, ByteBufferUtils.slice(wireData, 0, 2))));
        when(mockAsyncInputStream.read(2, 16))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        2, false, false, ByteBufferUtils.slice(wireData, 2, wireDataSize - 2))));
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(mockAsyncInputStream, 0, wireDataSize,
                SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);

        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read); //verify we are reading the data.
        verify(mockAsyncInputStream, times(1)).read(0L, wireDataSize);
        verify(mockAsyncInputStream, times(1)).read(2L, wireDataSize - 2);
    }

    @Test
    public void testReadWithEndOffsetWithDataGreaterThanBuffer() throws Exception {
        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data);
        int wireDataSize = wireData.remaining(); //size of the data with header size.
        int bufferSize = wireDataSize / 2; //buffer is half the data length

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, bufferSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        0, false, false, ByteBufferUtils.slice(wireData, 0, bufferSize))));
        when(mockAsyncInputStream.read(bufferSize, wireDataSize - bufferSize))
                .thenReturn(CompletableFuture.completedFuture(new WireCommands.SegmentRead(segment.getScopedName(),
                        bufferSize, false, false, ByteBufferUtils.slice(wireData, bufferSize, wireDataSize - bufferSize))));

        //Create a SegmentInputStream where the Buffer can hold only part of the data.
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(mockAsyncInputStream, 0, wireDataSize, bufferSize);

        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read); //verify we are reading the data.
        verify(mockAsyncInputStream, times(1)).read(0L, bufferSize);
        verify(mockAsyncInputStream, times(1)).read(bufferSize, wireDataSize - bufferSize);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadWithInvalidEndOffset() {
        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);

        //Set the end offset which is less than startOffset+WireCommands.TYPE_PLUS_LENGTH_SIZE
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(mockAsyncInputStream, 0,
                WireCommands.TYPE_PLUS_LENGTH_SIZE - 1, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadWithNegativeEndOffset() {
        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(mockAsyncInputStream, 0,
                -2, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }
    
    
    
    @Test
    public void testRefillSize() throws EndOfSegmentException, SegmentTruncatedException {
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        ByteBuffer wireData = createEventFromData(data);
        int wireDataSize = wireData.remaining(); // size of the data with header size.
        int bufferSize = SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE;

        AsyncSegmentInputStream mockAsyncInputStream = mock(AsyncSegmentInputStream.class);
        when(mockAsyncInputStream.read(0, bufferSize)).thenReturn(
            completedFuture(new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice())));

        int expectedReadSize = bufferSize - wireDataSize;

        when(mockAsyncInputStream.read(wireDataSize, expectedReadSize)).thenReturn(
            completedFuture(new SegmentRead(segment.getScopedName(), wireDataSize, false, false, wireData.slice())));

        // Verify that it requests enough data to fill the buffer.
        @Cleanup
        SegmentInputStreamImpl stream1 = new SegmentInputStreamImpl(mockAsyncInputStream, 0);

        ByteBuffer read = stream1.read();
        assertEquals(ByteBuffer.wrap(data), read); 
        verify(mockAsyncInputStream, times(1)).read(0L, bufferSize);
        verify(mockAsyncInputStream, times(1)).read(wireDataSize, expectedReadSize);

        when(mockAsyncInputStream.read(0, wireDataSize)).thenReturn(
            completedFuture(new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice())));

        // Verify it won't read beyond it's limit.
        @Cleanup
        SegmentInputStreamImpl stream2 = new SegmentInputStreamImpl(mockAsyncInputStream, 0, wireDataSize, bufferSize);

        read = stream2.read();
        assertEquals(ByteBuffer.wrap(data), read);
        verify(mockAsyncInputStream, times(1)).read(0L, wireDataSize);

        // Verify it works with a small buffer.
        when(mockAsyncInputStream.read(0, 100)).thenReturn(
                                                           completedFuture(new SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice())));
        @Cleanup
        SegmentInputStreamImpl stream3 = new SegmentInputStreamImpl(mockAsyncInputStream, 0, Long.MAX_VALUE, 100);

        read = stream3.read();
        assertEquals(ByteBuffer.wrap(data), read); 
        verify(mockAsyncInputStream, times(1)).read(0L, 100);
    }
}
