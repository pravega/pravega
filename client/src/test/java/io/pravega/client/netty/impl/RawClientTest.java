/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.netty.buffer.Unpooled;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalAppend;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RawClientTest {
    private final long requestId = 1L;

    @Test
    public void testHello() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, true);
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        RawClient rawClient = new RawClient(controller, connectionFactory, new Segment("scope", "testHello", 0));

        rawClient.sendRequest(1, new WireCommands.Hello(0, 0));
        Mockito.verify(connection).sendAsync(Mockito.eq(new WireCommands.Hello(0, 0)),
                                             Mockito.any(ClientConnection.CompletedCallback.class));
        rawClient.close();
        Mockito.verify(connection).close();
    }

    @Test
    public void testRequestReply() throws InterruptedException, ExecutionException {
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
        Mockito.verify(connection).sendAsync(Mockito.eq(request),
                                             Mockito.any(ClientConnection.CompletedCallback.class));
        assertFalse(future.isDone());
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        DataAppended reply = new DataAppended(requestId, id, 1, 0);
        processor.process(reply);
        assertTrue(future.isDone());
        assertEquals(reply, future.get());
    }

    @Test
    public void testOverloadConstructor() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();

        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);

        RawClient rawClient = new RawClient(endpoint, connectionFactory);

        rawClient.sendRequest(1, new WireCommands.Hello(0, 0));
        Mockito.verify(connection).sendAsync(Mockito.eq(new WireCommands.Hello(0, 0)),
                Mockito.any(ClientConnection.CompletedCallback.class));
        rawClient.close();
        Mockito.verify(connection).close();
    }

    @Test
    public void testRequestReplyTimeout() {
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
        CompletableFuture<Reply> future = rawClient.sendRequest(1, request, Duration.ofSeconds(0));
        assertThrows(TimeoutException.class, () -> future.join());
    }
}
