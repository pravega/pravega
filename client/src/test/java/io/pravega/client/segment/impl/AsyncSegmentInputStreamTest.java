/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.Flow;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteBufferUtils;
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
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AsyncSegmentInputStreamTest {
    private static final int SERVICE_PORT = 12345;

    @Test(timeout = 10000)
    public void testRetry() {

        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        InOrder inOrder = Mockito.inOrder(c);
        connectionFactory.provideConnection(endpoint, c);
        
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false,
                                                                            ByteBufferUtils.EMPTY, in.getRequestId());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                connectionFactory.getProcessor(endpoint).connectionDropped();
                return null;            
            }
        }).doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
            connectionFactory.getProcessor(endpoint).authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(in.getRequestId(), "SomeException"));
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
        inOrder.verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())),
                                    Mockito.any(ClientConnection.CompletedCallback.class));
        inOrder.verify(c).close();
        inOrder.verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())),
                                    Mockito.any(ClientConnection.CompletedCallback.class));
        inOrder.verify(c).close();
        inOrder.verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())),
                                    Mockito.any(ClientConnection.CompletedCallback.class));
        verifyNoMoreInteractions(c);
    }

    @Test(timeout = 10000)
    public void testRetryWithConnectionFailures() {

        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);

        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        MockConnectionFactoryImpl mockedCF = spy(connectionFactory);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, mockedCF, segment, "");
        InOrder inOrder = Mockito.inOrder(c);

        // Failed Connection
        CompletableFuture<ClientConnection> failedConnection = new CompletableFuture<>();
        failedConnection.completeExceptionally(new ConnectionFailedException("Netty error while establishing connection"));

        // Successful Connection
        CompletableFuture<ClientConnection> successfulConnection = new CompletableFuture<>();
        successfulConnection.complete(c);

        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false,
                                                                            ByteBufferUtils.EMPTY, in.getRequestId());
        // simulate a establishConnection failure to segment store.
        Mockito.doReturn(failedConnection)
               .doCallRealMethod()
               .when(mockedCF).establishConnection(any(Flow.class), eq(endpoint), any(ReplyProcessor.class));

        ArgumentCaptor<ClientConnection.CompletedCallback> callBackCaptor =
                ArgumentCaptor.forClass(ClientConnection.CompletedCallback.class);
        Mockito.doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                // Simulate a connection failure post establishing connection to SegmentStore.
                callBackCaptor.getValue().complete(new ConnectionFailedException("SendAsync exception since netty channel is null"));
                return null;
            }
        }).doAnswer(new Answer<Void>() {
                   public Void answer(InvocationOnMock invocation) {
                       mockedCF.getProcessor(endpoint).segmentRead(segmentRead);
                       return null;
                   }
               }).when(c).sendAsync(any(ReadSegment.class), callBackCaptor.capture());

        // Read invocation.
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);

        // Verification.
        assertEquals(segmentRead, readFuture.join());
        assertTrue(Futures.isSuccessful(readFuture)); // read completes after 3 retries.
        // Verify that the reader attempts to establish connection 3 times ( 2 failures followed by a successful attempt).
        verify(mockedCF, times(3)).establishConnection(any(Flow.class), eq(endpoint), any(ReplyProcessor.class));
        // The second time sendAsync is invoked but it fail due to the exception.
        inOrder.verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())),
                                    Mockito.any(ClientConnection.CompletedCallback.class));
        // Validate that the connection is closed in case of an error.
        inOrder.verify(c).close();
        // Validate that the read command is send again over the new connection.
        inOrder.verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())),
                                    Mockito.any(ClientConnection.CompletedCallback.class));
        // No more interactions since the SSS responds and the ReplyProcessor.segmentRead is invoked by netty.
        verifyNoMoreInteractions(c);
    }
    
    @Test(timeout = 10000)
    public void testCloseAbortsRead() throws InterruptedException, ExecutionException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
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
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);

        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false,
                                                                            ByteBufferUtils.EMPTY, in.getRequestId());
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        AssertExtensions.assertBlocks(() -> readFuture.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentRead(segmentRead);            
        });
        verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())),
                            Mockito.any(ClientConnection.CompletedCallback.class));
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
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);

        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);

        //segment truncated response from Segment store.
        WireCommands.SegmentIsTruncated segmentIsTruncated = new WireCommands.SegmentIsTruncated(in.getRequestId(), segment.getScopedName(), 1234,
                                                                                                 mockClientReplyStackTrace, 1234L);
        //Trigger read.
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);

        //verify that a response from Segment store completes the readFuture and the future completes with SegmentTruncatedException.
        AssertExtensions.assertBlocks(() -> assertThrows(SegmentTruncatedException.class, () -> readFuture.get()), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentIsTruncated(segmentIsTruncated);
        });
        verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())),
                            Mockito.any(ClientConnection.CompletedCallback.class));
        assertTrue(!Futures.isSuccessful(readFuture)); // verify read future completedExceptionally
        assertThrows(SegmentTruncatedException.class, () -> readFuture.get());
        verifyNoMoreInteractions(c);

        //Ensure that reads at a different offset can still happen on the same instance.
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 5656, false, false,
                                                                            ByteBufferUtils.EMPTY, in.getRequestId());
        CompletableFuture<SegmentRead> readFuture2 = in.read(5656, 5678);
        AssertExtensions.assertBlocks(() -> readFuture2.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentRead(segmentRead);
        });
        verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 5656, 5678, "", in.getRequestId())),
                            Mockito.any(ClientConnection.CompletedCallback.class));
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
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, "");
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        AssertExtensions.assertBlocks(() -> readFuture.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentRead(new WireCommands.SegmentRead(segment.getScopedName(), 1235, false, false, ByteBuffer.wrap(bad), in.getRequestId()));
            processor.segmentRead(new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false, ByteBuffer.wrap(good), in.getRequestId()));
        });
        verify(c).sendAsync(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId() )),
                            Mockito.any(ClientConnection.CompletedCallback.class));
        assertTrue(Futures.isSuccessful(readFuture));
        assertEquals(ByteBuffer.wrap(good), readFuture.join().getData());
        verifyNoMoreInteractions(c);
    }

}
