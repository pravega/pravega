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

import io.pravega.common.util.ByteBufferUtils;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.pravega.client.segment.impl.AsyncSegmentEventReaderImpl.DEFAULT_READ_LENGTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsyncSegmentEventReaderImplTest {

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
        public CompletableFuture<WireCommands.SegmentRead> read(long offset, int length) {
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

    private ByteBuffer createEventFromData(byte[] data, int numEntries) {
        ByteBuffer wireData = ByteBuffer.allocate((data.length + WireCommands.TYPE_PLUS_LENGTH_SIZE) * numEntries);
        for (int i = 0; i < numEntries; i++) {
            wireData.putInt(WireCommandType.EVENT.getCode());
            wireData.putInt(data.length);
            wireData.put(data);
        }
        wireData.flip();
        return wireData;
    }

    private long eventOffset(byte[] data, long entryIndex) {
        return (data.length + WireCommands.TYPE_PLUS_LENGTH_SIZE) * entryIndex;
    }

    @Test
    public void testClose() {
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 0);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork, 0);
        reader.close();
        assertTrue(fakeNetwork.isClosed());
        assertTrue(reader.isClosed());
    }

    @Test
    public void testRead() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork);

        CompletableFuture<ByteBuffer> readFuture = reader.readAsync(0L);
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        assertTrue(readFuture.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture.join());
    }

    @Test
    public void testSmallerThanNeededRead() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);

        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 4);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork);

        // progressively read a sequence of buffers that add up to a single event
        CompletableFuture<ByteBuffer> readFuture = reader.readAsync(0L);
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 2, false, false, ByteBufferUtils.slice(wireData, 2, 7)));
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 9, false, false, ByteBufferUtils.slice(wireData, 9, 2)));
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(3, new WireCommands.SegmentRead(segment.getScopedName(), 11, false, false, ByteBufferUtils.slice(wireData, 11, wireData.capacity() - 11)));
        assertTrue(readFuture.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture.join());
    }

    @Test
    public void testLongerThanRequestedRead() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        int numEntries = DEFAULT_READ_LENGTH / data.length;

        ByteBuffer wireData = createEventFromData(data, numEntries);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork, 0);

        // prepare a buffer with numerous events
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, true, true, wireData.slice()));

        // read the events and verify that the network is read once
        for (int i = 0; i < numEntries; i++) {
            CompletableFuture<ByteBuffer> readFuture = reader.readAsync(eventOffset(data, i));
            assertTrue(readFuture.isDone());
            assertEquals(ByteBuffer.wrap(data), readFuture.join());
            assertEquals(0, fakeNetwork.readIndex.get());
        }
        final CompletableFuture<ByteBuffer> readFuture = reader.readAsync(eventOffset(data, numEntries));
        assertTrue(readFuture.isDone());
        AssertExtensions.assertThrows(EndOfSegmentException.class, readFuture::join);
        assertEquals(0, fakeNetwork.readIndex.get());
    }

    @Test
    public void testExceptionRecovery() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork);

        // verify that the reader resets its internal state following an exception

        // step 1: mutate the state with partial reads followed by an exception
        final CompletableFuture<ByteBuffer> readFuture1 = reader.readAsync(0L);
        assertEquals(0L, reader.getReadState().getOffset());
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        assertEquals(2L, reader.getReadState().getOffset());
        fakeNetwork.completeExceptionally(1, new ConnectionFailedException());
        AssertExtensions.assertThrows(ConnectionFailedException.class, readFuture1::join);

        // step 2: re-read and verify that the internal state was correctly reset
        final CompletableFuture<ByteBuffer> readFuture2 = reader.readAsync(0L);
        assertEquals(0L, reader.getReadState().getOffset());
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        assertTrue(readFuture2.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture2.join());
    }

    @Test
    public void testStreamTruncated() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 2);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork, 0);
        CompletableFuture<ByteBuffer> readFuture;

        // read at truncated offset
        readFuture = reader.readAsync(0L);
        fakeNetwork.completeExceptionally(0, new SegmentTruncatedException());
        AssertExtensions.assertThrows(SegmentTruncatedException.class, readFuture::join);

        // read at available offset
        readFuture = reader.readAsync(eventOffset(data, 1));
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), eventOffset(data, 1), false, false, wireData.slice()));
        assertTrue(readFuture.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture.join());
    }

    @Test
    public void testEndOfSegment() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork, 0);
        CompletableFuture<ByteBuffer> readFuture;

        // prepare a buffer containing one event followed by end-of-segment
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, true, wireData.slice()));

        // read the event
        readFuture = reader.readAsync(0L);
        assertTrue(readFuture.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture.join());

        // read again, expecting end-of-segment
        readFuture = reader.readAsync(eventOffset(data, 1));
        assertTrue(readFuture.isDone());
        AssertExtensions.assertThrows(EndOfSegmentException.class, readFuture::join);
    }

    @Test
    public void testReadCancellation() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 3);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork);

        // verify user cancellation
        CompletableFuture<ByteBuffer> readFuture = reader.readAsync(0L);
        assertFalse(readFuture.isDone());
        readFuture.cancel(true);
        assertTrue(fakeNetwork.readResults.get(0).isCancelled());

        // verify cancellation of outstanding read (if any) when readAsync is called
        CompletableFuture<ByteBuffer> otherReadFuture = reader.readAsync(0L);
        assertFalse(otherReadFuture.isDone());
        readFuture = reader.readAsync(0L);
        assertTrue(otherReadFuture.isCancelled());
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(2, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        assertTrue(readFuture.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture.join());
    }

    @Test
    public void testCorruptData() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData;
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        AsyncSegmentEventReaderImpl reader = new AsyncSegmentEventReaderImpl(fakeNetwork, 0);
        CompletableFuture<ByteBuffer> readFuture;

        // invalid type
        wireData = createEventFromData(data, 1);
        wireData.putInt(0, WireCommandType.EVENT.getCode() + 1);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        readFuture = reader.readAsync(0L);
        AssertExtensions.assertThrows(InvalidMessageException.class, readFuture::join);

        // invalid length
        wireData = createEventFromData(data, 1);
        wireData.putInt(Integer.BYTES, WireCommands.MAX_WIRECOMMAND_SIZE + 1);
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        readFuture = reader.readAsync(0L);
        AssertExtensions.assertThrows(InvalidMessageException.class, readFuture::join);
    }
}