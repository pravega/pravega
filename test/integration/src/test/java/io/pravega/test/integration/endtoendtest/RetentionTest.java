/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@Slf4j
public class RetentionTest {

    private final String serviceHost = "localhost";
    private final int containerCount = 4;
    private int controllerPort;
    private URI controllerURI;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        controllerPort = TestUtils.getAvailableListenPort();
        controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
        int servicePort = TestUtils.getAvailableListenPort();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, this.serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
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

    @Test(timeout = 30000)
    public void testRetentionTime() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(2))
                                                        .retentionPolicy(RetentionPolicy.byTime(Duration.ofSeconds(1)))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        String name = "testtime";
        Stream stream = new StreamImpl(name, name);
        controllerWrapper.getControllerService().createScope(name, 0L).get();
        controller.createStream(name, name, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                          .controllerURI(controllerURI)
                                                                                          .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(name, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(name, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        Map<Segment, Long> x = controller.getSegmentsAtTime(stream, 0L).join();
        assertTrue(x.values().stream().allMatch(a -> a == 0));
        AtomicBoolean continueLoop = new AtomicBoolean(true);
        Futures.loop(continueLoop::get, () -> writer.writeEvent("a"), executor);

        AssertExtensions.assertEventuallyEquals(true, () -> controller
                .getSegmentsAtTime(stream, 0L).join().values().stream().anyMatch(a -> a > 0), 30 * 1000L);
        continueLoop.set(false);
    }

    @Test(timeout = 30000)
    public void testRetentionSize() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(2))
                                                        .retentionPolicy(RetentionPolicy.bySizeBytes(10))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        String name = "testsize";
        Stream stream = new StreamImpl(name, name);
        controllerWrapper.getControllerService().createScope(name, 0L).get();
        controller.createStream(name, name, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                          .controllerURI(controllerURI)
                                                                                          .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(name, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(name, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        Map<Segment, Long> x = controller.getSegmentsAtTime(stream, 0L).join();
        assertTrue(x.values().stream().allMatch(a -> a == 0));
        AtomicBoolean continueLoop = new AtomicBoolean(true);
        Futures.loop(continueLoop::get, () -> writer.writeEvent("a"), executor);

        AssertExtensions.assertEventuallyEquals(true, () -> controller
                .getSegmentsAtTime(stream, 0L).join().values().stream().anyMatch(a -> a > 0), 30 * 1000L);
        continueLoop.set(false);
    }
}
