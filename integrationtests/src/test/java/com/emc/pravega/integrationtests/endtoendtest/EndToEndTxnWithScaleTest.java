/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.integrationtests.endtoendtest;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.mock.MockClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class EndToEndTxnWithScaleTest {
    static final StreamConfiguration CONFIG =
            StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(
                    ScalingPolicy.byEventRate(10, 2, 1)).build();
    private ClientFactory clientFactory;
    private Controller controller;
    ControllerWrapper controllerWrapper;
    PravegaConnectionListener server;
    TestingServer zkTestServer;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServer();
        int port = Config.SERVICE_PORT;
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();

        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, 12345, store, null);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
        controllerWrapper.awaitRunning();
        controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("pravega").get();

        controllerWrapper.getControllerService().createScope("test").get();

        controller.createStream(CONFIG).get();
        clientFactory = new MockClientFactory("test", controller);
    }

    @After
    public void teardown() {
        clientFactory.close();
        server.close();
    }

    @Test
    public void testTxnWithScale() throws Exception {
        // Mocking pravega service by putting scale up and scale down requests for the stream
        EventStreamWriter<String> test = clientFactory.createEventWriter(
                "test", new JavaSerializer<>(), EventWriterConfig.builder().build());
        Transaction<String> transaction = test.beginTxn(5000, 3600000, 29000);
        transaction.writeEvent("0", "txntest1");
        transaction.commit();

        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        controller.scaleStream(stream, Collections.singletonList(0), map).get();

        transaction = test.beginTxn(5000, 3600000, 29000);
        transaction.writeEvent("0", "txntest2");
        transaction.commit();
    }
}
