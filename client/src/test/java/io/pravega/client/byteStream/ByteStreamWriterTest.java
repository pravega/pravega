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

import io.pravega.client.ClientFactory;
import io.pravega.client.byteStream.impl.BufferedByteStreamWriterImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteStreamWriterTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    @Test(timeout = 5000)
    public void testWrite() throws Exception {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        controller.createScope(SCOPE);
        controller.createStream(StreamConfiguration.builder()
                                .scope(SCOPE)
                                .streamName(STREAM)
                                .scalingPolicy(ScalingPolicy.fixed(1))
                                .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        byte[] value = new byte[] { 1, 2, 3, 4, 5 };
        writer.write(value);
        writer.flush();
        assertEquals(value.length, writer.fetchOffset());
        writer.write(value);
        writer.write(value);
        writer.flush();
        assertEquals(value.length * 3, writer.fetchOffset());
    }
    
    @Test(timeout = 5000)
    public void testSingleByteWrite() throws Exception {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        controller.createScope(SCOPE);
        controller.createStream(StreamConfiguration.builder()
                                .scope(SCOPE)
                                .streamName(STREAM)
                                .scalingPolicy(ScalingPolicy.fixed(1))
                                .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        int numBytes = BufferedByteStreamWriterImpl.BUFFER_SIZE * 2 + 1;
        for (int i = 0; i < numBytes; i++) {
            writer.write(i);
        }
        writer.flush();
        assertEquals(numBytes, writer.fetchOffset());
    }
    
    @Test(timeout = 5000)
    public void testLargeWrite() throws Exception {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        controller.createScope(SCOPE);
        controller.createStream(StreamConfiguration.builder()
                                .scope(SCOPE)
                                .streamName(STREAM)
                                .scalingPolicy(ScalingPolicy.fixed(1))
                                .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        byte[] value = new byte[2 * PendingEvent.MAX_WRITE_SIZE + 10];
        Arrays.fill(value, (byte) 1);
        writer.write(value);
        writer.flush();
        assertEquals(value.length, writer.fetchOffset());
        Arrays.fill(value, (byte) 2);
        writer.write(value);
        Arrays.fill(value, (byte) 3);
        writer.write(value);
        writer.flush();
        assertEquals(value.length * 3, writer.fetchOffset());
    }
    
    
    @Test
    public void testCloseAndSeal() throws IOException {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        controller.createScope(SCOPE);
        controller.createStream(StreamConfiguration.builder()
                                .scope(SCOPE)
                                .streamName(STREAM)
                                .scalingPolicy(ScalingPolicy.fixed(1))
                                .build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        ByteBuffer toWrite = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4 });
        writer.write(toWrite);
        writer.closeAndSeal();
        assertEquals(5, writer.fetchOffset());
    }
}
