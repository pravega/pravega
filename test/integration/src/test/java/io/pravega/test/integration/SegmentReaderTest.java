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

package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.SegmentReaderManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.hash.RandomFactory;
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
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for Segment Reader.
 */
@Slf4j
public class SegmentReaderTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "testSegmentReaderScope";
    private static final String STREAM = "testSegmentReaderStream";

    protected final int controllerPort = TestUtils.getAvailableListenPort();
    protected final String serviceHost = "localhost";
    protected final int servicePort = TestUtils.getAvailableListenPort();
    protected final int containerCount = 4;

    protected TestingServer zkTestServer;

    private final Random random = RandomFactory.create();

    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private JavaSerializer<Integer> serializer;
    private ClientConfig clientConfig;
    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        // Create and start segment store service
        serviceBuilder = createServiceBuilder();
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        // Create and start controller service
        controllerWrapper = createControllerWrapper();
        controllerWrapper.awaitRunning();
        serializer = new JavaSerializer<>();

        clientConfig = createClientConfig();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    //region Factory methods that may be overridden by subclasses.

    protected ServiceBuilder createServiceBuilder() {
        return ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
    }

    protected ControllerWrapper createControllerWrapper() {
        return new ControllerWrapper(zkTestServer.getConnectString(),
                false, true,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount, -1);
    }

    protected ClientConfig createClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(controllerUri()))
                .build();
    }

    protected String controllerUri() {
        return "tcp://localhost:" + controllerPort;
    }

    //endregion

    @Test(timeout = 50000)
    public void testSegmentReadOnSealedStream() throws ExecutionException, InterruptedException {
        int noOfEvents = 50;
        long timeout = 1000;
        int readEventCount = 0;
        createStream(1);
        writeEventsIntoStream(noOfEvents);

        @Cleanup
        SegmentReaderManager<Integer> segmentReaderManager = SegmentReaderManager.create(clientConfig, serializer);
        List<SegmentReader<Integer>> segmentReaderList = segmentReaderManager.getSegmentReaders(Stream.of(SCOPE, STREAM), null).get();
        assertEquals(1, segmentReaderList.size());
        controllerWrapper.getControllerService().sealStream(SCOPE, STREAM, 0L).join();
        SegmentReader<Integer> segmentReader = segmentReaderList.get(0);
        boolean exitNow = false;
        while (!exitNow) {
            try {
                segmentReader.read(timeout);
                readEventCount ++;
            } catch (EndOfSegmentException e) {
                exitNow = true;
            }
        }

        assertEquals(noOfEvents, readEventCount);
    }

    @Test(timeout = 50000)
    public void testSegmentRead() throws ExecutionException, InterruptedException {
        int noOfEvents = 50;
        long timeout = 1000;
        int readEventCount = 0;
        createStream(3);
        writeEventsIntoStream(noOfEvents);

        @Cleanup
        SegmentReaderManager<Integer> segmentReaderManager = SegmentReaderManager.create(clientConfig, serializer);
        List<SegmentReader<Integer>> segmentReaderList = segmentReaderManager.getSegmentReaders(Stream.of(SCOPE, STREAM), null).get();
        assertEquals(1, segmentReaderList.size());
        controllerWrapper.getControllerService().sealStream(SCOPE, STREAM, 0L).join();
        SegmentReader<Integer> segmentReader = segmentReaderList.get(0);
        boolean exitNow = false;
        while (!exitNow) {
            try {
                segmentReader.read(timeout);
                readEventCount ++;
            } catch (EndOfSegmentException e) {
                exitNow = true;
            }
        }

        assertEquals(noOfEvents, readEventCount);
    }

    private void createStream(int numOfSegments) {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numOfSegments))
                .build();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).join();
        assertTrue("Create Stream operation", controllerWrapper.getController().createStream(SCOPE, STREAM, config).join());
    }

    private void writeEventsIntoStream(int numberOfEvents) {
        Controller controller = controllerWrapper.getController();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<Integer> writer = clientFactory.createEventWriter(STREAM, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        Supplier<Integer> intEvent = random::nextInt;
        IntStream.range(0, numberOfEvents).forEach(v -> writer.writeEvent(intEvent.get()).join());
    }


}
