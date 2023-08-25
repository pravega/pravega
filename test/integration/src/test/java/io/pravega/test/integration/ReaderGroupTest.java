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

import io.pravega.client.EventStreamClientFactory;
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
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.Data;
import org.junit.Test;

public class ReaderGroupTest {

    private static final String SCOPE = "scope";
    private static final String STREAM_NAME = "streamName";
    private static final String READER_GROUP = "ExampleReaderGroup";

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
        String endpoint = "localhost";
        int servicePort = TestUtils.getAvailableListenPort();
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store, tableStore,
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, endpoint, servicePort);
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
        String endpoint = "localhost";
        int servicePort = TestUtils.getAvailableListenPort();
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store, tableStore,
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, endpoint, servicePort);
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
