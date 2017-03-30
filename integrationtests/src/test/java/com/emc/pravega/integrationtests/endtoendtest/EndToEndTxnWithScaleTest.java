/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.integrationtests.endtoendtest;

import com.emc.pravega.ClientFactory;
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
import com.emc.pravega.testcommon.TestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class EndToEndTxnWithScaleTest {

    private final int controllerPort = TestUtils.randomPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.randomPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    
    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServer();
        
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        
        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();
        
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, true,
                                                                    controllerPort, serviceHost, servicePort, containerCount);
        controllerWrapper.awaitRunning();
    }
    
    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 10000)
    public void testTxnWithScale() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope("test")
                                                        .streamName("test")
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("pravega").get();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ClientFactory clientFactory = new MockClientFactory("test", controller);
        @Cleanup
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
