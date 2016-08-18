/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import static com.emc.pravega.testcommon.Async.testBlocking;

import java.nio.ByteBuffer;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;

import com.emc.pravega.common.netty.WireCommandType;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.util.ByteBufferUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import lombok.Cleanup;

public class SegmentInputStreamTest {

    private static class TestAsyncSegmentInputStream extends AsyncSegmentInputStream {
        AtomicBoolean closed = new AtomicBoolean(false);
        private final Vector<CompletableFuture<SegmentRead>> readResults;
        private final AtomicInteger readIndex = new AtomicInteger(-1);

        TestAsyncSegmentInputStream(int expectedReads) {
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

        void complete(int readNumber, SegmentRead readResult) {
            readResults.get(readNumber).complete(readResult);
        }

        @Override
        public void close() {
            closed.set(true);
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
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(3);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        ByteBuffer read = testBlocking(() -> stream.read(),
                                       () -> fakeNetwork.complete(0, new SegmentRead("Foo", 0, false, false, wireData.slice())));
        assertEquals(ByteBuffer.wrap(data), read);
        read = testBlocking(() -> stream
            .read(), () -> fakeNetwork.complete(1, new SegmentRead("Foo", wireData.capacity(), false, false, wireData.slice())));
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testSmallerThanNeededRead() throws EndOfSegmentException {
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        ByteBuffer wireData = createEventFromData(data);
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(5);
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        fakeNetwork.complete(0, new SegmentRead("Foo", 0, false, false, ByteBufferUtils.slice(wireData, 0, 2)));
        fakeNetwork.complete(1, new SegmentRead("Foo", 2, false, false, ByteBufferUtils.slice(wireData, 2, 7)));
        fakeNetwork.complete(2, new SegmentRead("Foo", 9, false, false, ByteBufferUtils.slice(wireData, 9, 2)));
        fakeNetwork
            .complete(3, new SegmentRead("Foo", 11, false, false, ByteBufferUtils.slice(wireData, 11, wireData.capacity() - 11)));
        ByteBuffer read = stream.read();
        assertEquals(ByteBuffer.wrap(data), read);
    }

    @Test
    public void testLongerThanRequestedRead() throws EndOfSegmentException {
        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int numEntries = SegmentInputStreamImpl.BUFFER_SIZE / data.length;

        ByteBuffer wireData = ByteBuffer.allocate((data.length + WireCommands.TYPE_PLUS_LENGTH_SIZE) * numEntries);
        for (int i = 0; i < numEntries; i++) {
            wireData.putInt(WireCommandType.EVENT.getCode());
            wireData.putInt(data.length);
            wireData.put(data);
        }
        wireData.flip();
        TestAsyncSegmentInputStream fakeNetwork = new TestAsyncSegmentInputStream(3);
        fakeNetwork.complete(0, new SegmentRead("Foo", 0, false, false, wireData.slice()));
        @Cleanup
        SegmentInputStreamImpl stream = new SegmentInputStreamImpl(fakeNetwork, 0);
        for (int i = 0; i < numEntries; i++) {
            assertEquals(ByteBuffer.wrap(data), stream.read());
        }
        ByteBuffer read = testBlocking(() -> stream.read(), () -> {
            fakeNetwork.complete(1, new SegmentRead("Foo", wireData.capacity(), false, false, createEventFromData(data)));
        });
        assertEquals(ByteBuffer.wrap(data), read);
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
