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

import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AsyncSegmentInputStreamTest {
    private static final int SERVICE_PORT = 12345;

    @Test(timeout = 10000)
    public void testRetry() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        InOrder inOrder = Mockito.inOrder(c);
        connectionFactory.provideConnection(endpoint, c);
        
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.allocate(0));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                connectionFactory.getProcessor(endpoint).connectionDropped();
                return null;            
            }
        }).doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
            connectionFactory.getProcessor(endpoint).authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(100, "SomeException"));
            return null;
        }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                connectionFactory.getProcessor(endpoint).segmentRead(segmentRead);
                return null;
            }
        }).when(c).sendAsync(any(ReadSegment.class), any(ClientConnection.CompletedCallback.class));
        
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        assertEquals(segmentRead, readFuture.join());
        assertTrue(Futures.isSuccessful(readFuture));
        inOrder.verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234,  5678, ""),
                                    any(ClientConnection.CompletedCallback.class));
        inOrder.verify(c).close();
        inOrder.verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234,  5678, ""),
                                    any(ClientConnection.CompletedCallback.class));
        inOrder.verify(c).close();
        inOrder.verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234,   5678, ""),
                                    any(ClientConnection.CompletedCallback.class));
        verifyNoMoreInteractions(c);
    }
    
    @Test(timeout = 10000)
    public void testCloseAbortsRead() throws InterruptedException, ExecutionException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        in.getConnection().get(); // Make sure connection is established.
        CompletableFuture<SegmentRead> read = in.read(1234, 5678);
        assertFalse(read.isDone());
        in.close();
        assertThrows(ConnectionClosedException.class, () -> Futures.getThrowingException(read));
        verify(c).close();
    }

    @Test(timeout = 10000)
    public void testRead() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRead", 1);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);

        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.allocate(0));
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        AssertExtensions.assertBlocks(() -> readFuture.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentRead(segmentRead);            
        });
        verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234,  5678, ""),
                                    any(ClientConnection.CompletedCallback.class));
        assertTrue(Futures.isSuccessful(readFuture));
        assertEquals(segmentRead, readFuture.join());
        verifyNoMoreInteractions(c);
    }

    @Test(timeout = 10000)
    public void testSegmentTruncated() throws ConnectionFailedException {
        String mockClientReplyStackTrace = "SomeException";
        Segment segment = new Segment("scope", "testRead", 1);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);

        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);

        //segment truncated response from Segment store.
        WireCommands.SegmentIsTruncated segmentIsTruncated = new WireCommands.SegmentIsTruncated(1234L, segment.getScopedName(), 1234, mockClientReplyStackTrace);
        //Trigger read.
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);

        //verify that a response from Segment store completes the readFuture and the future completes with SegmentTruncatedException.
        AssertExtensions.assertBlocks(() -> assertThrows(SegmentTruncatedException.class, () -> readFuture.get()), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentIsTruncated(segmentIsTruncated);
        });
        verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234,  5678, ""),
                                    any(ClientConnection.CompletedCallback.class));
        assertTrue(!Futures.isSuccessful(readFuture)); // verify read future completedExceptionally
        assertThrows(SegmentTruncatedException.class, () -> readFuture.get());
        verifyNoMoreInteractions(c);

        //Ensure that reads at a different offset can still happen on the same instance.
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 5656, false, false, ByteBuffer.allocate(0));
        CompletableFuture<SegmentRead> readFuture2 = in.read(5656, 5678);
        AssertExtensions.assertBlocks(() -> readFuture2.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentRead(segmentRead);
        });
        verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 5656,  5678, ""),
                                    any(ClientConnection.CompletedCallback.class));
        assertTrue(Futures.isSuccessful(readFuture2));
        assertEquals(segmentRead, readFuture2.join());
        verifyNoMoreInteractions(c);
    }

    @Test(timeout = 10000)
    public void testWrongOffsetReturned() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testWrongOffsetReturned", 0);
        byte[] good = new byte[] { 0, 1, 2, 3, 4 };
        byte[] bad = new byte[] { 9, 8, 7, 6 };
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        AssertExtensions.assertBlocks(() -> readFuture.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentRead(new WireCommands.SegmentRead(segment.getScopedName(), 1235, false, false, ByteBuffer.wrap(bad)));            
            processor.segmentRead(new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.wrap(good)));         
        });
        verify(c).sendAsync(new WireCommands.ReadSegment(segment.getScopedName(), 1234,  5678, ""),
                                    any(ClientConnection.CompletedCallback.class));
        assertTrue(Futures.isSuccessful(readFuture));
        assertEquals(ByteBuffer.wrap(good), readFuture.join().getData());
        verifyNoMoreInteractions(c);
    }

}
