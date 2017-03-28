/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.segment.AsyncSegmentInputStream.ReadFuture;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.testcommon.Async;

import java.nio.ByteBuffer;

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
        ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new ReadSegment(segment.getScopedName(), 1234, 5678));
        processor.connectionDropped();
        verify(c).close();
        assertFalse(readFuture.isSuccess());
        SegmentRead segmentRead = new SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.allocate(0));
        SegmentRead result = Async.testBlocking(() -> in.getResult(readFuture), () -> {
            processor.segmentRead(segmentRead);
        });
        verify(c).send(new ReadSegment(segment.getScopedName(), 1234, 5678));
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
        ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new ReadSegment(segment.getScopedName(), 1234, 5678));
        SegmentRead segmentRead = new SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.allocate(0));
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
        ReadFuture readFuture = in.read(1234, 5678);
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        verify(c).sendAsync(new ReadSegment(segment.getScopedName(), 1234, 5678));
        processor.segmentRead(new SegmentRead(segment.getScopedName(), 1235, false, false, ByteBuffer.allocate(0)));
        assertFalse(readFuture.isSuccess());
        verifyNoMoreInteractions(c);
    }

}
