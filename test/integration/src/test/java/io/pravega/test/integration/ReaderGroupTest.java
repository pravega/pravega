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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.TestUtils;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.Data;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

public class ReaderGroupTest {

    private static final String SCOPE = "scopeName";
    private static final String STREAM_NAME = "streamName";
    private static final String READER_GROUP = "ExampleReaderGroup";
    private static final String ENDPOINT = "localhost";

    @Data
    private static class ReaderThread implements Runnable {
        private static final int READ_TIMEOUT = 60000;
        private final int eventsToRead;
        private final String readerId;
        private final EventStreamClientFactory clientFactory;
        private final AtomicReference<Exception> exception = new AtomicReference<>(null);

        @Override
        public void run() {
            try {
                @Cleanup
                EventStreamReader<String> reader = clientFactory.createReader(readerId,
                                                                              READER_GROUP,
                                                                              new JavaSerializer<>(),
                                                                              ReaderConfig.builder().build());
                String event = null;
                for (int i = 0; i < eventsToRead; i++) {
                    event = reader.readNextEvent(READ_TIMEOUT).getEvent();
                    if (event == null) {
                        exception.set(new IllegalStateException("Read timedOut unexpectedly"));
                    }
                }
            } catch (Exception e) {
                exception.set(e);
            }
        }
    }

    @Test(timeout = 20000)
    public void testEventHandoff() throws Exception {
        int servicePort = TestUtils.getAvailableListenPort();
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store, tableStore,
                serviceBuilder.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, ENDPOINT, servicePort);
        streamManager.createScope(SCOPE);
        streamManager.createStream(SCOPE, STREAM_NAME, StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(2))
                                                                   .build());
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();

        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .automaticCheckpointIntervalMillis(-1)
                                                         .stream(Stream.of(SCOPE, STREAM_NAME))
                                                         .build();
        streamManager.createReaderGroup(READER_GROUP, groupConfig);

        writeEvents(100, clientFactory);
        ReaderThread r1 = new ReaderThread(20, "Reader1", clientFactory);
        ReaderThread r2 = new ReaderThread(80, "Reader2", clientFactory);
        Thread reader1Thread = new Thread(r1);
        Thread reader2Thread = new Thread(r2);
        reader1Thread.start();
        reader2Thread.start();
        reader1Thread.join();
        reader2Thread.join();
        if (r1.exception.get() != null) {
            throw r1.exception.get();
        }
        if (r2.exception.get() != null) {
            throw r2.exception.get();
        }
        streamManager.deleteReaderGroup(READER_GROUP);
    }
    
    @Test(timeout = 10000)
    public void testMultiSegmentsPerReader() throws Exception {
        int servicePort = TestUtils.getAvailableListenPort();
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store, tableStore,
                serviceBuilder.getLowPriorityExecutor());
        server.startListening();

        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, ENDPOINT, servicePort);
        streamManager.createScope(SCOPE);
        streamManager.createStream(SCOPE, STREAM_NAME, StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(2))
                                                                   .build());
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();

        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .automaticCheckpointIntervalMillis(-1)
                                                         .stream(Stream.of(SCOPE, STREAM_NAME))
                                                         .build();
        streamManager.createReaderGroup(READER_GROUP, groupConfig);

        writeEvents(100, clientFactory);
        new ReaderThread(100, "Reader", clientFactory).run();
        streamManager.deleteReaderGroup(READER_GROUP);
    }

    @Test
    public void testReaderGroupConfig() throws Exception {
        final long rolloverSizeBytes = 1024 * 1024;
        @Cleanup
        TestingServer zkTestServer = new TestingServerStarter().start();

        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int servicePort = TestUtils.getAvailableListenPort();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store, tableStore,
                serviceBuilder.getLowPriorityExecutor());
        server.startListening();
        int controllerPort = TestUtils.getAvailableListenPort();
        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(
                zkTestServer.getConnectString(),
                false,
                controllerPort,
                ENDPOINT,
                servicePort,
                4);
        controllerWrapper.awaitRunning();

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(new URI(String.format("tcp://%s:%d", ENDPOINT, controllerPort)))
                .build();

        // create connection pool
        @Cleanup
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ConnectionPool connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);

        // create segment helper
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostMonitorMinRebalanceInterval(10)
                .containerCount(4)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(ENDPOINT, servicePort, 4))
                .build();
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(hostMonitorConfig);
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "collector");
        @Cleanup
        SegmentHelper segmentHelper = new SegmentHelper(connectionPool, hostStore, executor);

        // create stream
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        streamManager.createScope(SCOPE);
        streamManager.createStream(SCOPE, STREAM_NAME, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(2))
                .build());

        // create reader group
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                .automaticCheckpointIntervalMillis(-1)
                .stream(Stream.of(SCOPE, STREAM_NAME))
                .rolloverSizeBytes(rolloverSizeBytes)
                .build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        Assert.assertTrue(readerGroupManager.createReaderGroup(READER_GROUP, groupConfig));

        // check reader group internal stream segment rollover size
        WireCommands.SegmentAttribute attr = segmentHelper.getSegmentAttribute(
                NameUtils.getScopedReaderGroupName(SCOPE, READER_GROUP),
                new UUID(Long.MIN_VALUE, 4),
                new PravegaNodeUri(ENDPOINT, servicePort),
                "").join();
        Assert.assertEquals(attr.getValue(), rolloverSizeBytes);
        readerGroupManager.deleteReaderGroup(READER_GROUP);
        ExecutorServiceHelpers.shutdown(executor);
    }
    
    public void writeEvents(int eventsToWrite, EventStreamClientFactory clientFactory) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME,
                                                                           new JavaSerializer<>(),
                                                                           EventWriterConfig.builder().build());
        for (int i = 0; i < eventsToWrite; i++) {
            writer.writeEvent(Integer.toString(i), " Event " + i);
        }
        writer.flush();
    }
}
