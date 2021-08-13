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
import io.pravega.auth.AuthenticationException;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalAppend;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.ErrorMessage;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RawClientTest {
    
    private final long requestId = 1L;

    @Test
    public void testHello() throws ConnectionFailedException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        RawClient rawClient = new RawClient(controller, connectionFactory, new Segment("scope", "testHello", 0));

        rawClient.sendRequest(1, new WireCommands.Hello(0, 0));
        Mockito.verify(connection).send(Mockito.eq(new WireCommands.Hello(0, 0)));
        rawClient.close();
        Mockito.verify(connection).close();
    }

    @Test
    public void testRequestReply() throws InterruptedException, ExecutionException, ConnectionFailedException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        @Cleanup
        RawClient rawClient = new RawClient(controller, connectionFactory, new Segment("scope", "testHello", 0));

        UUID id = UUID.randomUUID();
        ConditionalAppend request = new ConditionalAppend(id, 1, 0, new Event(Unpooled.EMPTY_BUFFER), requestId);
        CompletableFuture<Reply> future = rawClient.sendRequest(1, request);
        Mockito.verify(connection).send(Mockito.eq(request));
        assertFalse(future.isDone());
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        DataAppended reply = new DataAppended(requestId, id, 1, 0, -1);
        processor.process(reply);
        assertTrue(future.isDone());
        assertEquals(reply, future.get());
    }
    
    @Test
    public void testReplyWithoutRequest() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        @Cleanup
        RawClient rawClient = new RawClient(controller, connectionFactory, new Segment("scope", "testHello", 0));

        UUID id = UUID.randomUUID();
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        DataAppended reply = new DataAppended(requestId, id, 1, 0, -1);
        processor.process(reply);
        assertFalse(rawClient.isClosed());
    }

    @Test
    public void testRecvErrorMessage() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        Segment segment = new Segment("scope", "testHello", 0);
        @Cleanup
        RawClient rawClient = new RawClient(controller, connectionFactory, segment);

        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        WireCommands.ErrorMessage reply = new ErrorMessage(requestId, segment.getScopedName(), "error.", ErrorMessage.ErrorCode.ILLEGAL_ARGUMENT_EXCEPTION);
        processor.process(reply);
        Mockito.verify(connection).close();
    }

    @Test
    public void testExceptionHandling() throws ConnectionFailedException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        Segment segment = new Segment("scope", "test", 0);
        RawClient rawClient = new RawClient(controller, connectionFactory, segment);

        WireCommands.ReadSegment request1 = new WireCommands.ReadSegment(segment.getScopedName(), 0, 10, "",
               requestId);

        CompletableFuture<Reply> future = rawClient.sendRequest(requestId, request1);
        // Verify if the request was sent over the connection.
        Mockito.verify(connection).send(Mockito.eq(request1));
        assertFalse("Since there is no response the future should not be completed", future.isDone());
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        processor.processingFailure(new ConnectionFailedException("Custom error"));

        assertTrue(future.isCompletedExceptionally());
        assertFutureThrows("The future should be completed exceptionally", future,
                t -> t instanceof ConnectionFailedException);
        rawClient.close();
        rawClient = new RawClient(controller, connectionFactory, segment);
        WireCommands.ReadSegment request2 = new WireCommands.ReadSegment(segment.getScopedName(), 0, 10, "", 2L);
        future = rawClient.sendRequest(2L, request2);
        // Verify if the request was sent over the connection.
        Mockito.verify(connection).send(Mockito.eq(request2));
        assertFalse("Since there is no response the future should not be completed", future.isDone());
        processor = connectionFactory.getProcessor(endpoint);
        processor.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(2L, "", WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));

        assertTrue(future.isCompletedExceptionally());
        assertFutureThrows("The future should be completed exceptionally", future,
                t -> t instanceof AuthenticationException);
        rawClient.close();
    }

    @Test
    public void testOverloadConstructor() throws ConnectionFailedException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();

        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);

        RawClient rawClient = new RawClient(endpoint, connectionFactory);

        rawClient.sendRequest(1, new WireCommands.Hello(0, 0));
        Mockito.verify(connection).send(Mockito.eq(new WireCommands.Hello(0, 0)));
        rawClient.close();
        Mockito.verify(connection).close();
    }

    @Test
    public void testExceptionWhileObtainingConnection() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);

        // setup mock
        CompletableFuture<PravegaNodeUri> endpointFuture = new CompletableFuture<>();
        endpointFuture.complete(endpoint);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getEndpointForSegment(Mockito.any(String.class))).thenReturn(endpointFuture);

        ConnectionPool connectionPool = Mockito.mock(ConnectionPool.class);
        // simulate error when obtaining a client connection.
        Mockito.doAnswer(invocation -> {
            final CompletableFuture<ClientConnection> future = invocation.getArgument(3);
            future.completeExceptionally(new ConnectionFailedException(new RuntimeException("Mock error")));
            return null;
        }).when(connectionPool).getClientConnection(Mockito.any(Flow.class), Mockito.eq(endpoint), Mockito.any(ReplyProcessor.class), Mockito.<CompletableFuture<ClientConnection>>any());

        // Test exception paths.
        @Cleanup
        RawClient rawClient = new RawClient(endpoint, connectionPool);
        CompletableFuture<Reply> reply = rawClient.sendRequest(100L, new WireCommands.Hello(0, 0));
        assertFutureThrows("RawClient did not wrap the exception into ConnectionFailedException", reply, t -> t instanceof ConnectionFailedException);

        @Cleanup
        RawClient rawClient1 = new RawClient(controller, connectionPool, new Segment("scope", "stream", 1));
        CompletableFuture<Reply> reply1 = rawClient1.sendRequest(101L, new WireCommands.Hello(0, 0));
        assertFutureThrows("RawClient did not wrap the exception into ConnectionFailedException", reply1, t -> t instanceof ConnectionFailedException);
    }
}
