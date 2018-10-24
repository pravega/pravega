/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.byteStream.ByteStreamClient;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.io.IOException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Slf4j
public class ByteStreamTest {

    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    
    @Before
    public void setup() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        this.server = new PravegaConnectionListener(false, servicePort, store);
        this.server.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
    }

    @After
    public void tearDown() throws Exception {
        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }
        if (this.server != null) {
            this.server.close();
            this.server = null;
        }
        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }
    }
    
    
    @Test(timeout = 30000)
    public void readWriteTest() throws IOException {
        String scope = "ByteStreamTest";
        String stream = "ReadWriteTest";
        
        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).build();
        try (StreamManager streamManager = new StreamManagerImpl(controller)) {
            //create a scope
            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Create scope status {}", createScopeStatus);
            //create a stream
            Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
            log.info("Create stream status {}", createStreamStatus);
        }

        try (ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory)) {
            ByteStreamClient client = clientFactory.createByteStreamClient();
            
            byte[] payload = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            byte[] readBuffer = new byte[10];
            
            ByteStreamWriter writer = client.createByteStreamWriter(stream);
            ByteStreamReader reader = client.createByteStreamReader(stream);
            
            AssertExtensions.assertBlocks(() -> reader.read(readBuffer), () -> writer.write(payload));
            assertArrayEquals(payload, readBuffer);
            Arrays.fill(readBuffer, (byte) 0);
            writer.write(payload);
            writer.write(payload);
            writer.write(payload);
            writer.closeAndSeal();
            assertEquals(10, reader.read(readBuffer));
            assertArrayEquals(payload, readBuffer);
            for (int i=0;i<10;i++) {
                assertEquals(i, reader.read());
            }
            Arrays.fill(readBuffer, (byte) -1);
            assertEquals(5, reader.read(readBuffer, 0, 5));
            assertEquals(5, reader.read(readBuffer, 5, 5));
            assertArrayEquals(payload, readBuffer);
            assertEquals(-1, reader.read());
            assertEquals(-1, reader.read(readBuffer));
        }
    }
    
}
