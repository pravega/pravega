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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ByteStreamReaderTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    @Test(timeout = 5000)
    public void testReadWritten() throws Exception {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        writer.write(value);
        writer.flush();
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(STREAM);
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
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(STREAM);
        assertEquals(0, reader.available());
        byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        writer.write(value);
        writer.flush();
        assertEquals(10, reader.available());
    }

    @Test(timeout = 5000)
    public void testSkip() throws Exception {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 0);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
        @Cleanup
        ByteStreamWriter writer = client.createByteStreamWriter(STREAM);
        @Cleanup
        ByteStreamReader reader = client.createByteStreamReader(STREAM);
        for (int i = 0; i < 5; i++) {
            writer.write(new byte[] { (byte) i });
        }
        reader.skip(1);
        assertEquals(1, reader.getOffset());
        byte[] read = new byte[2];
        reader.read(read);
        assertEquals(3, reader.getOffset());
        assertArrayEquals(new byte[] { 1, 2 }, read);
        long skipped = reader.skip(10);
        assertEquals(2, skipped);
        assertEquals(-1, reader.read());
        reader.jumpToOffset(0);
        assertEquals(0, reader.getOffset());
        assertEquals(5, reader.available());
    }

}