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

import io.pravega.auth.InvalidTokenException;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsTruncated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentTruncated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

public class SegmentMetadataClientTest {
    
    @Test(timeout = 10000)
    public void testCurrentStreamLength() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo getStreamInfo = invocation.getArgument(0);
                processor.process(new StreamSegmentInfo(getStreamInfo.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                                  123, 121));
                return null;
            }
        }).when(connection).send(any(WireCommands.GetStreamSegmentInfo.class));
        long head = client.fetchCurrentSegmentHeadOffset().join();
        long length = client.fetchCurrentSegmentLength().join();
        assertEquals(121, head);
        assertEquals(123, length);
    }
    
    @Test(timeout = 10000)
    public void testTruncate() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testTruncate", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        AtomicLong requestId = new AtomicLong();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.TruncateSegment truncateSegment = invocation.getArgument(0);
                processor.process(new SegmentTruncated(truncateSegment.getRequestId(), segment.getScopedName()));
                requestId.set(truncateSegment.getRequestId());
                return null;
            }
        }).when(connection).send(any(WireCommands.TruncateSegment.class));
        client.truncateSegment(123L).join();
        Mockito.verify(connection).send(Mockito.eq(new WireCommands.TruncateSegment(requestId.get(), segment.getScopedName(), 123L, "")));
    }

    @Test(timeout = 10000)
    public void testTruncateWithSegmentTruncationException() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testTruncate", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        AtomicLong requestId = new AtomicLong();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.TruncateSegment truncateSegment = invocation.getArgument(0);
                processor.process(new SegmentIsTruncated(truncateSegment.getRequestId(), segment.getScopedName(), 124L, "", 124L));
                requestId.set(truncateSegment.getRequestId());
                return null;
            }
        }).when(connection).send(any(WireCommands.TruncateSegment.class));
        client.truncateSegment(123L).join();
        Mockito.verify(connection).send(Mockito.eq(new WireCommands.TruncateSegment(requestId.get(), segment.getScopedName(), 123L, "")));
    }

    @Test(timeout = 10000)
    public void testTruncateNoSuchSegmentError() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testTruncate", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        AtomicLong requestId = new AtomicLong();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.TruncateSegment truncateSegment = invocation.getArgument(0);
                processor.process(new WireCommands.NoSuchSegment(truncateSegment.getRequestId(), segment.getScopedName(), "", 123L));
                requestId.set(truncateSegment.getRequestId());
                return null;
            }
        }).when(connection).send(any(WireCommands.TruncateSegment.class));
        AssertExtensions.assertThrows(NoSuchSegmentException.class, () -> client.truncateSegment(123L).join());
        Mockito.verify(connection).send(Mockito.eq(new WireCommands.TruncateSegment(requestId.get(), segment.getScopedName(), 123L, "")));
    }
    
    @Test(timeout = 10000)
    public void testSeal() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testSeal", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        AtomicLong requestId = new AtomicLong();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.SealSegment sealSegment = invocation.getArgument(0);
                processor.process(new WireCommands.SegmentSealed(sealSegment.getRequestId(), segment.getScopedName()));
                requestId.set(sealSegment.getRequestId());
                return null;
            }
        }).when(connection).send(any(WireCommands.SealSegment.class));
        client.sealSegment().join();
        Mockito.verify(connection).send(Mockito.eq(new WireCommands.SealSegment(requestId.get(), segment.getScopedName(), "")));
    }  

    @Test(timeout = 10000)
    public void testGetProperty() throws Exception {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetSegmentAttribute request = invocation.getArgument(0);
                processor.process(new WireCommands.SegmentAttribute(request.getRequestId(), 123));
                return null;
            }
        }).when(connection).send(any(WireCommands.GetSegmentAttribute.class));
        long value = client.fetchProperty(SegmentAttribute.RevisionStreamClientMark).join();
        assertEquals(123, value);
    }

    @Test(timeout = 10000)
    public void compareAndSetAttribute() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.UpdateSegmentAttribute request = invocation.getArgument(0);
                processor.process(new SegmentAttributeUpdated(request.getRequestId(), true));
                return null;
            }
        }).when(connection).send(any(WireCommands.UpdateSegmentAttribute.class));
        assertTrue(client.compareAndSetAttribute(SegmentAttribute.RevisionStreamClientMark, -1234, 1234).join());
    }

    @Test(timeout = 10000)
    public void testReconnects() throws Exception {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = Mockito.spy(new MockConnectionFactoryImpl());
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        final List<Long> requestIds = new ArrayList<>();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = invocation.getArgument(0);
                requestIds.add(request.getRequestId());
                if (requestIds.size() == 1) {
                    ReplyProcessor processor = cf.getProcessor(endpoint);
                    processor.connectionDropped();
                } else {
                    ReplyProcessor processor = cf.getProcessor(endpoint);
                    processor.process(new StreamSegmentInfo(request.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                            123, 121));
                }
                return null;
            }
        }).when(connection).send(any(WireCommands.GetStreamSegmentInfo.class));
        long length = client.fetchCurrentSegmentLength().join();
        InOrder order = Mockito.inOrder(connection, cf);
        order.verify(cf).establishConnection(eq(endpoint), any(ReplyProcessor.class));
        order.verify(connection).send(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(0), segment.getScopedName(), "")));
        order.verify(cf).establishConnection(eq(endpoint), any(ReplyProcessor.class));
        order.verify(connection).send(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(1), segment.getScopedName(), "")));
        order.verify(cf).getProcessor(eq(endpoint));
        order.verifyNoMoreInteractions();
        assertEquals(123, length);
    }
    
    @Test(timeout = 10000)
    public void testExceptionOnSend() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        ConnectionPool cf = Mockito.mock(ConnectionPool.class);
        Mockito.when(cf.getInternalExecutor()).thenReturn(executor);
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        ClientConnection connection1 = mock(ClientConnection.class);
        ClientConnection connection2 = mock(ClientConnection.class);
        AtomicReference<ReplyProcessor> processor = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            final CompletableFuture<ClientConnection> future = invocation.getArgument(3);
            future.completeExceptionally(new ConnectionFailedException(new RuntimeException("Mock error")));
            return null;
        }).doAnswer(invocation -> {
            final CompletableFuture<ClientConnection> future = invocation.getArgument(3);
            future.complete(connection1);
            return null;
        }).doAnswer(invocation -> {
            final CompletableFuture<ClientConnection> future = invocation.getArgument(3);
            processor.set(invocation.getArgument(2));
            future.complete(connection2);
            return null;
        }).when(cf).getClientConnection(Mockito.any(Flow.class), Mockito.eq(endpoint), Mockito.any(ReplyProcessor.class), Mockito.<CompletableFuture<ClientConnection>>any());

        final List<Long> requestIds = new ArrayList<>();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = invocation.getArgument(0);
                requestIds.add(request.getRequestId());
                if (requestIds.size() == 1) {
                    throw new ConnectionFailedException();
                } else {
                    processor.get().process(new StreamSegmentInfo(request.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                                  123, 121));
                }
                return null;
            }
        }).when(connection1).send(any(WireCommands.GetStreamSegmentInfo.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                WireCommands.GetStreamSegmentInfo request = invocation.getArgument(0);
                requestIds.add(request.getRequestId());
                processor.get().process(new StreamSegmentInfo(request.getRequestId(), segment.getScopedName(), true, false, false, 0,
                                                              123, 121));
                return null;
            }
        }).when(connection2).send(any(WireCommands.GetStreamSegmentInfo.class));
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        InOrder order = Mockito.inOrder(connection1, connection2, cf);
        long length = client.fetchCurrentSegmentLength().join();
        order.verify(cf, Mockito.times(2)).getClientConnection(Mockito.any(Flow.class), Mockito.eq(endpoint), Mockito.any(), Mockito.<CompletableFuture<ClientConnection>>any());
        order.verify(connection1).send(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(0), segment.getScopedName(), "")));
        order.verify(connection1).close();
        order.verify(cf).getClientConnection(Mockito.any(Flow.class), Mockito.eq(endpoint), Mockito.any(), Mockito.<CompletableFuture<ClientConnection>>any());
        order.verify(connection2).send(Mockito.eq(new WireCommands.GetStreamSegmentInfo(requestIds.get(1), segment.getScopedName(), "")));
        order.verifyNoMoreInteractions();
        assertEquals(123, length);
    }

    @Test(timeout = 10000)
    public void testTokenExpiry() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {

                WireCommands.GetStreamSegmentInfo getStreamInfo = invocation.getArgument(0);
                processor.process(new WireCommands.AuthTokenCheckFailed(getStreamInfo.getRequestId(), "server-stacktrace",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED));
                return null;
            }
        }).when(connection).send(any(WireCommands.GetStreamSegmentInfo.class));

        AssertExtensions.assertThrows("ConnectionFailedException was not thrown or server stacktrace contained unexpected content.",
                () -> client.getStreamSegmentInfo().join(),
                e -> e instanceof ConnectionFailedException && e.getMessage().contains("serverStackTrace=server-stacktrace"));
    }

    @Test(timeout = 10000)
    public void testTokenCheckFailed() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {

                WireCommands.GetStreamSegmentInfo getStreamInfo = invocation.getArgument(0);
                processor.process(new WireCommands.AuthTokenCheckFailed(getStreamInfo.getRequestId(), "server-stacktrace",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
                return null;
            }
        }).when(connection).send(any(WireCommands.GetStreamSegmentInfo.class));

        AssertExtensions.assertThrows("TokenException was not thrown or server stacktrace contained unexpected content.",
                () -> client.fetchCurrentSegmentLength().join(),
                e -> e instanceof InvalidTokenException && e.getMessage().contains("serverStackTrace=server-stacktrace"));
    }

    @Test(timeout = 10000)
    public void testTokenCheckFailure() throws ConnectionFailedException {
        Segment segment = new Segment("scope", "testRetry", 4);
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), cf, true);
        @Cleanup
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(endpoint, connection);
        @Cleanup
        SegmentMetadataClientImpl client = new SegmentMetadataClientImpl(segment, controller, cf, "");
        client.getConnection();
        ReplyProcessor processor = cf.getProcessor(endpoint);

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {

                WireCommands.GetStreamSegmentInfo getStreamInfo = invocation.getArgument(0);
                processor.process(new WireCommands.AuthTokenCheckFailed(getStreamInfo.getRequestId(), "server-stacktrace",
                        WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
                return null;
            }
        }).when(connection).send(any(WireCommands.GetStreamSegmentInfo.class));

        AssertExtensions.assertThrows("TokenException was not thrown or server stacktrace contained unexpected content.",
                () -> client.fetchCurrentSegmentLength().join(),
                e -> e instanceof InvalidTokenException && e.getMessage().contains("serverStackTrace=server-stacktrace"));
    }
}
