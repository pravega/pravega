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
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalAppend;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RawClientTest {

    @Test
    public void testHello() throws ConnectionFailedException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        RawClient rawClient = new RawClient(connectionFactory, endpoint);

        rawClient.sendRequest(1, new WireCommands.Hello(0, 0));
        Mockito.verify(connection).send(new WireCommands.Hello(0, 0));
        rawClient.close();
        Mockito.verify(connection).close();
    }
    
    @Test
    public void testRequestReply() throws ConnectionFailedException, InterruptedException, ExecutionException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        connectionFactory.provideConnection(endpoint, connection);
        @Cleanup
        RawClient rawClient = new RawClient(connectionFactory, endpoint);

        UUID id = UUID.randomUUID();
        ConditionalAppend request = new ConditionalAppend(id, 1, 0, Unpooled.EMPTY_BUFFER);
        CompletableFuture<Reply> future = rawClient.sendRequest(1, request);
        Mockito.verify(connection).send(request);
        assertFalse(future.isDone());
        ReplyProcessor processor = connectionFactory.getProcessor(endpoint);
        DataAppended reply = new DataAppended(id, 1, 0);
        processor.process(reply);
        assertTrue(future.isDone());
        assertEquals(reply, future.get());
    }

}
