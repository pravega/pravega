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

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.test.common.TestUtils;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class AutoCheckpointTest {

    private static final long NANOS_PER_SECOND = 1000000000;

    @Test(timeout = 30000)
    public void testCheckpointsOccur() throws ReinitializationRequiredException, DurableDataLogException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroup = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world: ";
        String scope = "Scope1";
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class),
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .automaticCheckpointIntervalMillis(10000)
                                                         .stream(Stream.of(scope, streamName))
                                                         .build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        populateEvents(streamName, testString, clientFactory, serializer);

        AtomicLong fakeClock = new AtomicLong(0);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroup, serializer,
                                                                      ReaderConfig.builder().build(),
                                                                      () -> fakeClock.get(),
                                                                      () -> fakeClock.get() / NANOS_PER_SECOND);
        int numRead = 0;
        int checkpointCount = 0;
        while (numRead < 100) {
            fakeClock.addAndGet(NANOS_PER_SECOND);
            EventRead<String> event = reader.readNextEvent(1000);
            if (event.isCheckpoint()) {
                checkpointCount++;
            } else {
                String message = event.getEvent();
                assertEquals(testString + numRead, message);
                numRead++;
            }
        }
        assertTrue("Count was " + checkpointCount, checkpointCount > 5);
        assertTrue("Count was " + checkpointCount, checkpointCount < 20);
    }

    @Test(timeout = 30000)
    public void testOnlyOneOutstanding() throws ReinitializationRequiredException, DurableDataLogException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerGroup = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world: ";
        String scope = "Scope1";
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class),
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .automaticCheckpointIntervalMillis(1000)
                                                         .stream(Stream.of(scope, streamName))
                                                         .build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        populateEvents(streamName, testString, clientFactory, serializer);
        AtomicLong fakeClock = new AtomicLong(0);
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", readerGroup, serializer,
                                                                       ReaderConfig.builder().build(),
                                                                       () -> fakeClock.get(),
                                                                       () -> fakeClock.get() / NANOS_PER_SECOND);
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", readerGroup, serializer,
                                                                       ReaderConfig.builder().build(),
                                                                       () -> fakeClock.get(),
                                                                       () -> fakeClock.get() / NANOS_PER_SECOND);
        int numRead = 0;
        int checkpointCount = 0;
        while (numRead < 100) {
            fakeClock.addAndGet(NANOS_PER_SECOND);
            EventRead<String> event = reader1.readNextEvent(1000);
            if (event.isCheckpoint()) {
                checkpointCount++;
            } else {
                String message = event.getEvent();
                assertEquals(testString + numRead, message);
                numRead++;
            }
        }
        assertEquals("As there is a second reader that does not pass the checkpoint, only one should occur", 1,
                     checkpointCount);
    }
    
    private void populateEvents(String streamName, String testString, MockClientFactory clientFactory,
                                JavaSerializer<String> serializer) {
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                                                                             EventWriterConfig.builder().build());
        for (int i = 0; i < 100; i++) {
            producer.writeEvent(testString + i);
        }
        producer.flush();
    }

}
