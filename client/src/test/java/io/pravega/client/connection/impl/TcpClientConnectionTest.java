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
package io.pravega.client.connection.impl;

import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.protocol.netty.EnhancedByteBufInputStream;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.SneakyThrows;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static io.pravega.test.common.AssertExtensions.assertBlocks;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TcpClientConnectionTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);

    /*
        A test Socket InputStream to simulate network related race conditions.
     */
    private static class TestInputStream extends InputStream {
        int numAvailable = 0;
        ReusableLatch readInvokedLatch = new ReusableLatch(false);
        ReusableLatch lastByteLatch = new ReusableLatch(false);
        private final InputStream out;

        @SneakyThrows
        public TestInputStream(WireCommand msg) {
            this.out = new EnhancedByteBufInputStream(Unpooled.wrappedBuffer(getCommandAsBytes(msg)));
        }

        @Override
        public int available() throws IOException {
            return numAvailable;
        }

        @Override
        public int read() throws IOException {
            // Read from the inputstream.
            int data = out.read();
            // Indicate that the read has been invoked.
            readInvokedLatch.release();
            if (out.available() == 0) {
                // Block on the latch before returning the last byte.
                Exceptions.handleInterrupted(() -> lastByteLatch.await());
            }
            return data;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }

        private byte[] getCommandAsBytes(WireCommand msg) throws IOException {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            msg.writeFields(new DataOutputStream(bout));
            byte[] msgBytes = bout.toByteArray();
            // Insert type and length to the wireCommand msg.
            byte[] networkBytes = new byte[Integer.BYTES + Integer.BYTES + msgBytes.length];
            ByteBuffer buf = ByteBuffer.wrap(networkBytes);
            buf.putInt(msg.getType().getCode());
            buf.putInt(msgBytes.length);
            buf.put(msgBytes);
            return networkBytes;
        }
    }

    @Test
    public void testTcpClientConnectionStop() throws InterruptedException {
        ReplyProcessor rp = mock(ReplyProcessor.class);
        TestInputStream tcpStream = new TestInputStream(new WireCommands.SegmentIsSealed(1L, "seg", "", 1234L));
        TcpClientConnection.ConnectionReader reader = new TcpClientConnection.ConnectionReader("reader", tcpStream, rp, mock(FlowToBatchSizeTracker.class));
        // Trigger a read.
        reader.start();
        // Await until the read from the Socket Input stream has started.
        tcpStream.readInvokedLatch.await();
        // Invoke stop and verify it is blocked.
        assertBlocks(reader::stop, () -> tcpStream.lastByteLatch.release());
        // Verify that segment sealed callback is not invoked since stop() has already been invoked.
        verify(rp, times(0)).process(any(WireCommands.SegmentIsSealed.class));
        // Ensure a connection dropped is invoked as part of the stop().
        verify(rp, times(1)).connectionDropped();
        // Ensure no other callbacks are invoked post this.
        verifyNoMoreInteractions(rp);
    }

    @Test
    public void testAwaitCallbackCompletionBeforeConnectionDrop() throws InterruptedException {
        ReusableLatch blockCallbackLatch = new ReusableLatch(false);
        ReusableLatch isCallbackInvokedLatch = new ReusableLatch(false);
        ReplyProcessor rp = spy(new FailingReplyProcessor() {
            @SneakyThrows
            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                isCallbackInvokedLatch.release();
                blockCallbackLatch.await();
            }

            @Override
            public void connectionDropped() {
            }

            @Override
            public void processingFailure(Exception error) {
                fail("This call back is not expected");
            }
        });
        TestInputStream tcpStream = new TestInputStream(new WireCommands.SegmentIsSealed(1L, "seg", "", 1234L));
        TcpClientConnection.ConnectionReader reader = new TcpClientConnection.ConnectionReader("reader", tcpStream, rp, mock(FlowToBatchSizeTracker.class));
        // Trigger a read.
        reader.start();
        tcpStream.lastByteLatch.release();
        // Wait until the ReplyProcessor callback is invoked.
        isCallbackInvokedLatch.await();

        // Verify if ConnectionReader.stop() is blocked until the ReplyProcessor callback finish its execution.
        assertBlocks(reader::stop, blockCallbackLatch::release);
        verify(rp, times(1)).process(any(WireCommands.SegmentIsSealed.class));
        verify(rp, times(1)).segmentIsSealed(any(WireCommands.SegmentIsSealed.class));
        // Verify that connectionDropped callback is invoked at the end.
        verify(rp, times(1)).connectionDropped();
        // Ensure no other callbacks are invoked.
        verifyNoMoreInteractions(rp);
    }
}
