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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.net.URI;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndWithScaleTest extends ThreadPooledTestSuite {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
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
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 90000)
    public void testScale() throws Exception {
        final String scope = "test";
        final String streamName = "test";
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();

        // Test scale both in a new stream and in a re-created one.
        for (int i = 0; i < 2; i++) {
            @Cleanup
            Controller controller = controllerWrapper.getController();
            controllerWrapper.getControllerService().createScope(scope, 0L).get();
            controller.createStream(scope, streamName, config).get();
            @Cleanup
            ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                        .controllerURI(URI.create("tcp://localhost"))
                                                                                        .build());
            @Cleanup
            ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
            @Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                    EventWriterConfig.builder().build());
            writer.writeEvent("0", "txntest1" + i).get();

            // scale
            Stream stream = new StreamImpl(scope, streamName);
            Map<Double, Double> map = new HashMap<>();
            map.put(0.0, 0.33);
            map.put(0.33, 0.66);
            map.put(0.66, 1.0);
            Boolean result = controller.scaleStream(stream, Collections.singletonList(i * 4L), map, executorService()).getFuture().get();
            assertTrue(result);
            writer.writeEvent("0", "txntest2" + i).get();
            @Cleanup
            ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
            groupManager.createReaderGroup("reader" + i, ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0).
                    stream(Stream.of(scope, streamName).getScopedName()).build());
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader("readerId" + i, "reader" + i, new JavaSerializer<>(),
                    ReaderConfig.builder().build());
            EventRead<String> event = reader.readNextEvent(10000);
            assertNotNull(event.getEvent());
            assertEquals("txntest1" + i, event.getEvent());
            event = reader.readNextEvent(100);
            assertNull(event.getEvent());
            groupManager.getReaderGroup("reader" + i).initiateCheckpoint("cp" + i, executorService());
            event = reader.readNextEvent(10000);
            assertEquals("cp" + i, event.getCheckpointName());
            event = reader.readNextEvent(10000);
            assertEquals("txntest2" + i, event.getEvent());
            assertTrue(controller.sealStream(scope, streamName).join());
            assertTrue(controller.deleteStream(scope, streamName).join());
        }
    }
}