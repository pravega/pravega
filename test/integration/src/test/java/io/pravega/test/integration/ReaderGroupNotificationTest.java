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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.notifications.EndOfDataNotification;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.SegmentNotification;
import io.pravega.client.stream.notifications.notifier.EndOfDataNotifier;
import io.pravega.client.stream.notifications.notifier.SegmentNotifier;
import io.pravega.common.util.ReusableLatch;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ReaderGroupNotificationTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "test";
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private AtomicBoolean listenerInvoked = new AtomicBoolean();
    private ReusableLatch listenerLatch = new ReusableLatch();
    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

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
        listenerLatch.reset();
        listenerInvoked.set(false);
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 40000)
    public void testSegmentNotifications() throws Exception {
        final String streamName = "stream1";
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).get();
        controller.createStream(SCOPE, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://localhost"))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();

        // scale
        Stream stream = new StreamImpl(SCOPE, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();
        assertTrue(result);
        writer.writeEvent("0", "data2").get();

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).groupRefreshTimeMillis(0).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("reader");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().initialAllocationDelay(0).build());

        val notificationResults = new ArrayBlockingQueue<SegmentNotification>(2);

        //Add segment event listener
        Listener<SegmentNotification> l1 = notification -> {
            log.info("Number of Segments: {}, Number of Readers: {}", notification.getNumOfSegments(), notification.getNumOfReaders());
            notificationResults.add(notification);
        };
        SegmentNotifier segmentNotifier = (SegmentNotifier) readerGroup.getSegmentNotifier(executorService());
        segmentNotifier.registerListener(l1);

        // Read first event and validate notification.
        EventRead<String> event1 = reader1.readNextEvent(5000);
        assertEquals("data1", event1.getEvent());

        segmentNotifier.pollNow();
        SegmentNotification initialSegmentNotification = notificationResults.take();
        assertNotNull(initialSegmentNotification);
        assertEquals(1, initialSegmentNotification.getNumOfReaders());
        assertEquals(1, initialSegmentNotification.getNumOfSegments());
        
        EventRead<String> emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        readerGroup.initiateCheckpoint("cp", executorService());
        EventRead<String> cpEvent = reader1.readNextEvent(1000);
        assertTrue(cpEvent.isCheckpoint());
        
        // Read second event and validate notification.
        EventRead<String> event2 = reader1.readNextEvent(10000);
        assertEquals("data2", event2.getEvent());
        segmentNotifier.pollNow();
        SegmentNotification segmentNotificationPostScale = notificationResults.take();
        assertEquals(1, segmentNotificationPostScale.getNumOfReaders());
        assertEquals(2, segmentNotificationPostScale.getNumOfSegments());
    }

    @Test(timeout = 40000)
    public void testEndOfStreamNotifications() throws Exception {
        final String streamName = "stream2";
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).get();
        controller.createStream(SCOPE, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://localhost"))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();

        // scale
        Stream stream = new StreamImpl(SCOPE, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();
        assertTrue(result);
        writer.writeEvent("0", "data2").get();
        assertTrue(controller.sealStream(SCOPE, streamName).get()); // seal stream

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).groupRefreshTimeMillis(0).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("reader");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().initialAllocationDelay(0).build());

        //Add segment event listener
        Listener<EndOfDataNotification> l1 = notification -> {
            listenerInvoked.set(true);
            listenerLatch.release();
        };
        EndOfDataNotifier endOfDataNotifier = (EndOfDataNotifier) readerGroup.getEndOfDataNotifier(executorService());
        endOfDataNotifier.registerListener(l1);

        EventRead<String> event1 = reader1.readNextEvent(10000);
        assertEquals("data1", event1.getEvent());
        EventRead<String> emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        readerGroup.initiateCheckpoint("cp", executorService());
        EventRead<String> cpEvent = reader1.readNextEvent(10000);
        assertTrue(cpEvent.isCheckpoint());
        EventRead<String> event2 = reader1.readNextEvent(10000);
        assertEquals("data2", event2.getEvent());
        emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        readerGroup.initiateCheckpoint("cp2", executorService());
        cpEvent = reader1.readNextEvent(10000);
        assertTrue(cpEvent.isCheckpoint());
        emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());

        endOfDataNotifier.pollNow();
        listenerLatch.await();
        assertTrue("Listener invoked", listenerInvoked.get());
    }

}
