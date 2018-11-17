/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.byteStream;

import io.pravega.client.ClientFactory;
import io.pravega.client.byteStream.impl.BufferedByteStreamWriterImpl;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;

public class ByteStreamWriterTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private MockConnectionFactoryImpl connectionFactory;
    private MockController controller;
    private ClientFactory clientFactory;

    @Before
    public void setup() throws ConnectionFailedException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        connectionFactory = new MockConnectionFactoryImpl();
        ClientConnection connection = Mockito.mock(ClientConnection.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CreateSegment request = (CreateSegment) invocation.getArgument(0);
                connectionFactory.getProcessor(endpoint)
                                 .process(new SegmentCreated(request.getRequestId(), request.getSegment()));
                return null;
            }
        }).when(connection).send(Mockito.any(CreateSegment.class));
        connectionFactory.provideConnection(endpoint, connection);
        controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        controller.createScope(SCOPE);
        controller.createStream(SCOPE, STREAM, StreamConfiguration.builder()
                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                   .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory, streamFactory,
                                              streamFactory, streamFactory);
    }

    @After
    public void teardown() {
        clientFactory.close();
        controller.close();
        connectionFactory.close();
    }

    @Test(timeout = 5000)
    public void testWrite() throws Exception {
        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        byte[] value = new byte[] { 1, 2, 3, 4, 5 };
        writer.write(value);
        writer.flush();
        assertEquals(value.length, writer.fetchTailOffset());
        writer.write(value);
        writer.write(value);
        writer.flush();
        assertEquals(value.length * 3, writer.fetchTailOffset());
    }

    @Test(timeout = 5000)
    public void testSingleByteWrite() throws Exception {
        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        int numBytes = BufferedByteStreamWriterImpl.BUFFER_SIZE * 2 + 1;
        for (int i = 0; i < numBytes; i++) {
            writer.write(i);
        }
        writer.flush();
        assertEquals(numBytes, writer.fetchTailOffset());
    }

    @Test(timeout = 5000)
    public void testLargeWrite() throws Exception {
        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        byte[] value = new byte[2 * PendingEvent.MAX_WRITE_SIZE + 10];
        Arrays.fill(value, (byte) 1);
        writer.write(value);
        writer.flush();
        assertEquals(value.length, writer.fetchTailOffset());
        Arrays.fill(value, (byte) 2);
        writer.write(value);
        Arrays.fill(value, (byte) 3);
        writer.write(value);
        writer.flush();
        assertEquals(value.length * 3L, writer.fetchTailOffset());
    }

    @Test
    public void testCloseAndSeal() throws IOException {
        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        ByteBuffer toWrite = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4 });
        writer.write(toWrite);
        writer.closeAndSeal();
        assertEquals(5, writer.fetchTailOffset());
    }
}
