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

import io.netty.buffer.Unpooled;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import lombok.Cleanup;
import lombok.SneakyThrows;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AsyncSegmentInputStreamTest extends LeakDetectorTestSuite {
    private static final int SERVICE_PORT = 12345;

    @Test(timeout = 10000)
    public void testRetry() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        DelegationTokenProvider tokenProvider = mock(DelegationTokenProvider.class);
        when(tokenProvider.retrieveToken()).thenReturn(CompletableFuture.completedFuture("")); // return empty token
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, tokenProvider, dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        InOrder inOrder = Mockito.inOrder(c);
        connectionFactory.provideConnection(endpoint, c);

        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false,
                Unpooled.EMPTY_BUFFER, in.getRequestId());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                connectionFactory.getProcessor(endpoint).connectionDropped();
                return null;            
            }
        }).doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
            connectionFactory.getProcessor(endpoint).authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(
                    in.getRequestId(), "SomeException", WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
            return null;
        }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                connectionFactory.getProcessor(endpoint).process(segmentRead);
                return null;
            }
        }).when(c).send(any(ReadSegment.class));
        assertEquals(0, dataAvailable.availablePermits());
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        assertEquals(segmentRead, readFuture.join());
        assertTrue(Futures.isSuccessful(readFuture));
        assertEquals(1, dataAvailable.availablePermits());
        inOrder.verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        inOrder.verify(c).close();
        inOrder.verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        inOrder.verify(c).close();
        inOrder.verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        verifyNoMoreInteractions(c);
        // ensure retrieve Token is invoked for every retry.
        verify(tokenProvider, times(3)).retrieveToken();
    }

    @Test
    public void testProcessingFailure() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        DelegationTokenProvider tokenProvider = mock(DelegationTokenProvider.class);
        when(tokenProvider.retrieveToken()).thenReturn(CompletableFuture.completedFuture("")); // return empty token
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment, tokenProvider, dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        InOrder inOrder = Mockito.inOrder(c);
        connectionFactory.provideConnection(endpoint, c);

        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false,
                Unpooled.EMPTY_BUFFER, in.getRequestId());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                connectionFactory.getProcessor(endpoint).processingFailure(new ConnectionFailedException("Custom error"));
                return null;
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                connectionFactory.getProcessor(endpoint).process(segmentRead);
                return null;
            }
        }).when(c).send(any(ReadSegment.class));
        assertEquals(0, dataAvailable.availablePermits());
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        assertEquals(segmentRead, readFuture.join());
        assertTrue(Futures.isSuccessful(readFuture));
        assertEquals(1, dataAvailable.availablePermits());
        inOrder.verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        inOrder.verify(c).close();
        inOrder.verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        verifyNoMoreInteractions(c);
        // ensure retrieve Token is invoked for every retry.
        verify(tokenProvider, times(2)).retrieveToken();
    }

    @SneakyThrows
    @Test(timeout = 10000)
    public void testAuthenticationFailure() {
        Segment segment = new Segment("scope", "testRead", 1);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        Semaphore dataAvailable = new Semaphore(0);

        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);

        //Non-token expiry auth token check failure response from Segment store.
        WireCommands.AuthTokenCheckFailed authTokenCheckFailed = new WireCommands.AuthTokenCheckFailed( in.getRequestId(), "SomeException",
                WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED);

        //Trigger read.
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        assertEquals(0, dataAvailable.availablePermits());

        //verify that a response from Segment store completes the readFuture and the future completes with the specified exception.
        AssertExtensions.assertBlocks(() -> assertThrows(AuthenticationException.class, () -> readFuture.get()), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.process(authTokenCheckFailed);
        });
        verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        assertTrue(!Futures.isSuccessful(readFuture)); // verify read future completedExceptionally
    }

    @Test(timeout = 10000)
    public void testRetryWithConnectionFailures() throws ConnectionFailedException {

        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);

        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        MockConnectionFactoryImpl mockedCF = spy(connectionFactory);
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, mockedCF, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        InOrder inOrder = Mockito.inOrder(c);

        // Failed Connection
        CompletableFuture<ClientConnection> failedConnection = new CompletableFuture<>();
        failedConnection.completeExceptionally(new ConnectionFailedException("Netty error while establishing connection"));

        // Successful Connection
        CompletableFuture<ClientConnection> successfulConnection = new CompletableFuture<>();
        successfulConnection.complete(c);

        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false,
                Unpooled.EMPTY_BUFFER, in.getRequestId());
        // simulate a establishConnection failure to segment store.
        Mockito.doReturn(failedConnection)
               .doCallRealMethod()
               .when(mockedCF).establishConnection(eq(endpoint), any(ReplyProcessor.class));

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                // Simulate a connection failure post establishing connection to SegmentStore.
                throw Exceptions.sneakyThrow(new ConnectionFailedException("SendAsync exception since netty channel is null"));
            }
        }).doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                mockedCF.getProcessor(endpoint).process(segmentRead);
                return null;
            }
        }).when(c).send(any(ReadSegment.class));
        assertEquals(0, dataAvailable.availablePermits());
        // Read invocation.
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);

        // Verification.
        assertEquals(segmentRead, readFuture.join());
        assertTrue(Futures.isSuccessful(readFuture)); // read completes after 3 retries.
        assertEquals(1, dataAvailable.availablePermits());
        // Verify that the reader attempts to establish connection 3 times ( 2 failures followed by a successful attempt).
        verify(mockedCF, times(3)).establishConnection(eq(endpoint), any(ReplyProcessor.class));
        // The second time sendAsync is invoked but it fail due to the exception.
        inOrder.verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        // Validate that the connection is closed in case of an error.
        inOrder.verify(c).close();
        // Validate that the read command is send again over the new connection.
        inOrder.verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        // No more interactions since the SSS responds and the ReplyProcessor.segmentRead is invoked by netty.
        verifyNoMoreInteractions(c);
    }
    
    @Test(timeout = 10000)
    public void testCloseAbortsRead() throws InterruptedException, ExecutionException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        in.getConnection().get(); // Make sure connection is established.
        CompletableFuture<SegmentRead> read = in.read(1234, 5678);
        assertFalse(read.isDone());
        in.close();
        assertThrows(ConnectionClosedException.class, () -> Futures.getThrowingException(read));
        verify(c).close();
        assertEquals(0, dataAvailable.availablePermits());
    }

    @Test(timeout = 10000)
    public void testRead() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRead", 1);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);

        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false,
                Unpooled.EMPTY_BUFFER, in.getRequestId());
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        assertEquals(0, dataAvailable.availablePermits());
        AssertExtensions.assertBlocks(() -> readFuture.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.process(segmentRead);            
        });
        assertEquals(1, dataAvailable.availablePermits());
        verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        assertTrue(Futures.isSuccessful(readFuture));
        assertEquals(segmentRead, readFuture.join());
        verifyNoMoreInteractions(c);
    }

    @Test(timeout = 10000)
    public void testSegmentTruncated() throws ConnectionFailedException {
        String mockClientReplyStackTrace = "SomeException";
        Segment segment = new Segment("scope", "testRead", 1);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);

        //segment truncated response from Segment store.
        WireCommands.SegmentIsTruncated segmentIsTruncated = new WireCommands.SegmentIsTruncated(in.getRequestId(), segment.getScopedName(), 1234,
                                                                                                 mockClientReplyStackTrace, 1234L);
        //Trigger read.
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        assertEquals(0, dataAvailable.availablePermits());
        //verify that a response from Segment store completes the readFuture and the future completes with SegmentTruncatedException.
        AssertExtensions.assertBlocks(() -> assertThrows(SegmentTruncatedException.class, () -> readFuture.get()), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.process(segmentIsTruncated);
        });
        verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId())));
        assertTrue(!Futures.isSuccessful(readFuture)); // verify read future completedExceptionally
        assertThrows(SegmentTruncatedException.class, () -> readFuture.get());
        verifyNoMoreInteractions(c);
        //Ensure that reads at a different offset can still happen on the same instance.
        WireCommands.SegmentRead segmentRead = new WireCommands.SegmentRead(segment.getScopedName(), 5656, false, false,
                Unpooled.EMPTY_BUFFER, in.getRequestId());
        CompletableFuture<SegmentRead> readFuture2 = in.read(5656, 5678);
        AssertExtensions.assertBlocks(() -> readFuture2.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.segmentRead(segmentRead);
        });
        verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 5656, 5678, "", in.getRequestId())));
        assertTrue(Futures.isSuccessful(readFuture2));
        assertEquals(segmentRead, readFuture2.join());
        verifyNoMoreInteractions(c);
        assertEquals(1, dataAvailable.availablePermits());
    }

    @Test(timeout = 10000)
    public void testWrongOffsetReturned() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testWrongOffsetReturned", 0);
        byte[] good = new byte[] { 0, 1, 2, 3, 4 };
        byte[] bad = new byte[] { 9, 8, 7, 6 };
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        CompletableFuture<SegmentRead> readFuture = in.read(1234, 5678);
        assertEquals(0, dataAvailable.availablePermits());
        AssertExtensions.assertBlocks(() -> readFuture.get(), () -> {
            ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
            processor.process(new WireCommands.SegmentRead(segment.getScopedName(), 1235, false, false, Unpooled.wrappedBuffer(bad), in.getRequestId()));
            processor.process(new WireCommands.SegmentRead(segment.getScopedName(), 1234, false, false, Unpooled.wrappedBuffer(good), in.getRequestId()));
        });
        assertEquals(2, dataAvailable.availablePermits());
        verify(c).send(eq(new WireCommands.ReadSegment(segment.getScopedName(), 1234, 5678, "", in.getRequestId() )));
        assertTrue(Futures.isSuccessful(readFuture));
        assertEquals(Unpooled.wrappedBuffer(good), readFuture.join().getData());
        verifyNoMoreInteractions(c);
    }

    @Test
    public void testRecvErrorMessage() throws ExecutionException, InterruptedException {
        int requestId = 0;
        Segment segment = new Segment("scope", "testWrongOffsetReturned", requestId);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        Semaphore dataAvailable = new Semaphore(requestId);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);

        in.getConnection().get();

        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        WireCommands.ErrorMessage reply = new WireCommands.ErrorMessage(requestId, segment.getScopedName(), "error.", WireCommands.ErrorMessage.ErrorCode.ILLEGAL_ARGUMENT_EXCEPTION);
        processor.process(reply);
        verify(c).close();
    }

    @Test(timeout = 10000)
    public void testWrongHost() throws ExecutionException, InterruptedException {
        Segment segment = new Segment("scope", "testRead", 1);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        Semaphore dataAvailable = new Semaphore(0);
        @Cleanup
        AsyncSegmentInputStreamImpl in = new AsyncSegmentInputStreamImpl(controller, connectionFactory, segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), dataAvailable);
        ClientConnection c = mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, c);
        in.getConnection().get();
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        CompletableFuture<PravegaNodeUri> endpointForSegment;
        endpointForSegment = controller.getEndpointForSegment("scope1/stream1/0");
        assertEquals(new PravegaNodeUri("localhost", SERVICE_PORT), endpointForSegment.get());
        WireCommands.WrongHost wrongHost = new WireCommands.WrongHost(in.getRequestId(), segment.getScopedName(), "newHost", "SomeException");
        processor.process(wrongHost);
        verify(c).close();
    }

}
