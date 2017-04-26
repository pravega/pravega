/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.ClientFactory;
import io.pravega.ReaderGroupManager;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.stream.EventRead;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.Stream;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.Transaction;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.impl.ReaderGroupManagerImpl;
import io.pravega.stream.impl.StreamImpl;
import io.pravega.test.common.TestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndTxnWithScaleTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                true,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
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
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope("test", controller);
        @Cleanup
        EventStreamWriter<String> test = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        Transaction<String> transaction = test.beginTxn(5000, 3600000, 29000);
        transaction.writeEvent("0", "txntest1");
        transaction.commit();

        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0), map).get();

        assertTrue(result);

        transaction = test.beginTxn(5000, 3600000, 29000);
        transaction.writeEvent("0", "txntest2");
        transaction.commit();
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().build(), Collections.singleton("test"));
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("txntest1", event.getEvent());
        event = reader.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("txntest2", event.getEvent());
    }
}