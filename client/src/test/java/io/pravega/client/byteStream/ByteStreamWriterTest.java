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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
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

        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        ByteStreamClient client = clientFactory.createByteStreamClient();
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
}
