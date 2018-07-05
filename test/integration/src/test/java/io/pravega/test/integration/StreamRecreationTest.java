package io.pravega.test.integration;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class StreamRecreationTest {
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 40000)
    public void testStreamRecreation() throws Exception {
        final String myScope = "myScope";
        final String myStream = "myString";

        // Create the scope and the stream.
        @Cleanup
        StreamManager streamManager = StreamManager.create(URI.create("tcp://" + serviceHost + ":" + controllerPort));
        streamManager.createScope(myScope);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder().scope(myScope).streamName(myStream).build();
        streamManager.createStream(myScope, myStream, streamConfiguration);

        // Write a single event.
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(myScope, URI.create("tcp://" + serviceHost + ":" + controllerPort));
        EventStreamWriter<String> writer = clientFactory.createEventWriter(myStream, new JavaSerializer<>(), EventWriterConfig.builder().build());
        writer.writeEvent("Test Event 1").join();
        writer.close();

        // Delete the stream.
        streamManager.sealStream(myScope, myStream);
        streamManager.deleteStream(myScope, myStream);

        // Wait for a while and then create it again.
        Thread.sleep(10000);
        streamManager.createStream(myScope, myStream, streamConfiguration);
        streamManager.sealStream(myScope, myStream);
        streamManager.deleteStream(myScope, myStream);
        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(myStream, new JavaSerializer<>(), EventWriterConfig.builder().build());
        writer2.writeEvent("Test Event 2").join();
    }
}
