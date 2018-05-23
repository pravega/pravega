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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsyncSegmentFrameDecoderImplTest {

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

    @Test
    public void testClose() {

    }

    @Test
    public void testRead() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        @Cleanup
        AsyncSegmentFrameDecoderImpl decoder = new AsyncSegmentFrameDecoderImpl(fakeNetwork, 0);

        CompletableFuture<ByteBuffer> readFuture = decoder.read();
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
        AsyncSegmentFrameDecoderImpl decoder = new AsyncSegmentFrameDecoderImpl(fakeNetwork, 0);

        CompletableFuture<ByteBuffer> readFuture = decoder.read();
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
        int numEntries = AsyncSegmentFrameDecoderImpl.DEFAULT_READ_LENGTH / data.length;

        ByteBuffer wireData = createEventFromData(data, numEntries);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 1);
        fakeNetwork.complete(0, new WireCommands.SegmentRead(segment.getScopedName(), 0, true, true, wireData.slice()));
        @Cleanup
        AsyncSegmentFrameDecoderImpl decoder = new AsyncSegmentFrameDecoderImpl(fakeNetwork, 0);

        for (int i = 0; i < numEntries; i++) {
            CompletableFuture<ByteBuffer> readFuture = decoder.read();
            assertTrue(readFuture.isDone());
            assertEquals(ByteBuffer.wrap(data), readFuture.join());
        }
        final CompletableFuture<ByteBuffer> readFuture = decoder.read();
        assertTrue(readFuture.isDone());
        AssertExtensions.assertThrows(EndOfSegmentException.class, readFuture::join);
    }

    @Test
    public void testExceptionRecovery() {

    }

    @Test
    public void testStreamTruncated() {

    }

    @Test
    public void testEndOfSegment() {

    }

    @Test
    public void testSetOffset() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        AsyncSegmentFrameDecoderImpl decoder = new AsyncSegmentFrameDecoderImpl(fakeNetwork, 0);
        CompletableFuture<ByteBuffer> readFuture = decoder.read();
        assertFalse(readFuture.isDone());
        long offset = wireData.remaining() / 2;
        decoder.setOffset(offset);
        assertTrue(readFuture.isCancelled());
        assertTrue(fakeNetwork.readResults.get(0).isCancelled());

        readFuture = decoder.read();
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), offset, false, false, wireData.slice()));
        assertTrue(readFuture.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture.join());
    }

    @Test
    public void testReadCancellation() throws Exception {
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteBuffer wireData = createEventFromData(data, 1);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(segment, 2);
        @Cleanup
        AsyncSegmentFrameDecoderImpl decoder = new AsyncSegmentFrameDecoderImpl(fakeNetwork, 0);
        CompletableFuture<ByteBuffer> readFuture = decoder.read();
        assertFalse(readFuture.isDone());
        readFuture.cancel(true);
        assertTrue(fakeNetwork.readResults.get(0).isCancelled());

        readFuture = decoder.read();
        assertFalse(readFuture.isDone());
        fakeNetwork.complete(1, new WireCommands.SegmentRead(segment.getScopedName(), 0, false, false, wireData.slice()));
        assertTrue(readFuture.isDone());
        assertEquals(ByteBuffer.wrap(data), readFuture.join());
    }

    @Test
    public void testReadWithEndOffset() {

    }

    @Test
    public void testCorruptData() {

    }
}