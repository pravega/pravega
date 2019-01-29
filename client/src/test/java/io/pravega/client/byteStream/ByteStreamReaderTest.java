/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.byteStream;

import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.shared.segment.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ByteStreamReaderTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private MockConnectionFactoryImpl connectionFactory;
    private MockController controller;
    private ByteStreamClientFactory clientFactory;

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
        }).when(connection).sendAsync(Mockito.any(CreateSegment.class),
                                      Mockito.any(ClientConnection.CompletedCallback.class));
        connectionFactory.provideConnection(endpoint, connection);
        controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        controller.createScope(SCOPE);
        controller.createStream(SCOPE, STREAM, StreamConfiguration.builder()
                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                   .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        clientFactory = new ByteStreamClientImpl(SCOPE, controller, connectionFactory, streamFactory, streamFactory,
                                              streamFactory);
    }

    @After
    public void teardown() {
        clientFactory.close();
        controller.close();
        connectionFactory.close();
    }
    
    @Test(timeout = 5000)
    public void testReadWritten() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        writer.write(value);
        writer.flush();
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(STREAM);
        for (int i = 0; i < 10; i++) {
            assertEquals(i, reader.read());
        }
        writer.write(value);
        writer.flush();
        byte[] read = new byte[5];
        assertEquals(5, reader.read(read));
        assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 }, read);
        assertEquals(2, reader.read(read, 2, 2));
        assertArrayEquals(new byte[] { 0, 1, 5, 6, 4 }, read);
        assertEquals(3, reader.read(read, 2, 3));
        assertArrayEquals(new byte[] { 0, 1, 7, 8, 9 }, read);
        assertEquals(-1, reader.read(read, 2, 3));
        assertArrayEquals(new byte[] { 0, 1, 7, 8, 9 }, read);
    }

    @Test(timeout = 5000)
    public void testAvailable() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(STREAM);
        assertEquals(0, reader.available());
        byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        writer.write(value);
        writer.flush();
        assertEquals(10, reader.available());
    }

    @Test(timeout = 5000)
    public void testSkip() throws Exception {
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(STREAM);
        for (int i = 0; i < 5; i++) {
            writer.write(new byte[] { (byte) i });
        }
        assertEquals(1, reader.skip(1));
        assertEquals(1, reader.getOffset());
        byte[] read = new byte[2];
        assertEquals(2, reader.read(read));
        assertEquals(3, reader.getOffset());
        assertArrayEquals(new byte[] { 1, 2 }, read);
        long skipped = reader.skip(10);
        assertEquals(2, skipped);
        assertEquals(-1, reader.read());
        reader.seekToOffset(0);
        assertEquals(0, reader.getOffset());
        assertEquals(5, reader.available());
    }

}