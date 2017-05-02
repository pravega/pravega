/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.stream.impl.segment;

import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.stream.Segment;
import io.pravega.stream.impl.netty.ClientConnection;
import io.pravega.stream.mock.MockConnectionFactoryImpl;
import io.pravega.stream.mock.MockController;
import io.pravega.test.common.Async;

import java.nio.ByteBuffer;

import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AsyncSegmentInputStreamTest {
    private static final int SERVICE_PORT = 12345;

    @Test(timeout = 20000)
    public void testRetry() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        AsyncSegmentInputStream.ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678));
        processor.connectionDropped();
        verify(c).close();
        assertFalse(readFuture.isSuccess());
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.allocate(0));
        WireCommands.SegmentRead result = Async.testBlocking(() -> in.getResult(readFuture), () -> {
            processor.segmentRead(segmentRead);
        });
        verify(c).send(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678));
        assertTrue(readFuture.isSuccess());
        assertEquals(segmentRead, result);
        verifyNoMoreInteractions(c);
    }

    @Test(timeout = 10000)
    public void testRead() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRead", 1);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);

        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        AsyncSegmentInputStream.ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678));
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.allocate(0));
        processor.segmentRead(segmentRead);
        assertTrue(readFuture.isSuccess());
        assertEquals(segmentRead, in.getResult(readFuture));
        verifyNoMoreInteractions(c);
    }

    @Test(timeout = 10000)
    public void testWrongOffsetReturned() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testWrongOffsetReturned", 0);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        AsyncSegmentInputStream.ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678));
        processor.segmentRead(new WireCommands.SegmentRead(segment.getScopedName(), 1235, false, false, ByteBuffer.allocate(0)));
        assertFalse(readFuture.isSuccess());
        verifyNoMoreInteractions(c);
    }

}
