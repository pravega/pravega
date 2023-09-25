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
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.integration.utils.IntegerSerializer;
import io.pravega.test.integration.utils.SetupUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@Slf4j
public class MultiReadersEndToEndTest {

    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    // The number of events to generate for each test.
    private static final int NUM_TEST_EVENTS = 1000;

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test(timeout = 30000)
    public void testReadersEqualToSegments() throws Exception {
        runTest(Collections.singleton("teststream1"), 2, 2);
        runTestUsingMock(Collections.singleton("teststream1"), 2, 2);
    }
    
    @Test(timeout = 30000)  
     public void testReadersLessThanSegments() throws Exception {      
        runTest(Collections.singleton("teststream2"), 2, 3);
        runTestUsingMock(Collections.singleton("teststream2"), 2, 3);
    }
    
    @Test(timeout = 30000)
    public void testReadersGreaterThanSegments() throws Exception {
        runTest(Collections.singleton("teststream3"), 3, 2);
        runTestUsingMock(Collections.singleton("teststream3"), 3, 2);
    }

    @Test(timeout = 30000)
    public void testMultiStreams() throws Exception {
        Set<String> testStreams = new HashSet<>();
        testStreams.add("teststream4");
        testStreams.add("teststream5");
        runTest(testStreams, 2, 2);
        runTestUsingMock(testStreams, 2, 2);
    }

    private void runTest(final Set<String> streamNames, final int numParallelReaders, final int numSegments)
            throws Exception {
        @Cleanup
        StreamManager streamManager = StreamManager.create(ClientConfig.builder()
                                                                       .controllerURI(SETUP_UTILS.getControllerUri()).build());
        streamManager.createScope(SETUP_UTILS.getScope());
        streamNames.stream().forEach(stream -> {
            streamManager.createStream(SETUP_UTILS.getScope(),
                                       stream,
                                       StreamConfiguration.builder()
                                               .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                               .build());
            log.info("Created stream: {}", stream);
        });

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SETUP_UTILS.getScope(), ClientConfig.builder()
                                                                                                  .controllerURI(SETUP_UTILS.getControllerUri()).build());
        streamNames.stream().forEach(stream -> {
            @Cleanup
            EventStreamWriter<Integer> eventWriter = clientFactory.createEventWriter(
                    stream, new IntegerSerializer(), EventWriterConfig.builder().build());
            for (Integer i = 0; i < NUM_TEST_EVENTS; i++) {
                eventWriter.writeEvent(String.valueOf(i), i);
            }
            eventWriter.flush();
            log.info("Wrote {} events", NUM_TEST_EVENTS);
        });

        final String readerGroupName = "testreadergroup" + RandomStringUtils.randomAlphanumeric(10).toLowerCase();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SETUP_UTILS.getScope(),
                                    ClientConfig.builder()
                                                .controllerURI(SETUP_UTILS.getControllerUri()).build());
        ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder();
        streamNames.forEach(s -> builder.stream(Stream.of(SETUP_UTILS.getScope(), s)));
        readerGroupManager.createReaderGroup(readerGroupName, builder.build());

        Collection<Integer> read = readAllEvents(numParallelReaders, clientFactory, readerGroupName, numSegments);

        Assert.assertEquals(NUM_TEST_EVENTS * streamNames.size(), read.size());
        // Check unique events.
        Assert.assertEquals(NUM_TEST_EVENTS, new TreeSet<>(read).size());
        readerGroupManager.deleteReaderGroup(readerGroupName);
    }

    private Collection<Integer> readAllEvents(final int numParallelReaders, EventStreamClientFactory clientFactory,
                                              final String readerGroupName, final int numSegments) {
        ConcurrentLinkedQueue<Integer> read = new ConcurrentLinkedQueue<>();
        @Cleanup("shutdownNow")
        final ExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(
                numParallelReaders, "testreader-pool");
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numParallelReaders; i++) {
            futures.add(executorService.submit(() -> {
                final String readerId = UUID.randomUUID().toString();
                @Cleanup
                final EventStreamReader<Integer> reader = clientFactory.createReader(readerId,
                                                                                     readerGroupName,
                                                                                     new IntegerSerializer(),
                                                                                     ReaderConfig.builder().build());
                int emptyCount = 0;
                while (emptyCount <= numSegments) {
                    try {
                        final Integer integerEventRead = reader.readNextEvent(100).getEvent();
                        if (integerEventRead != null) {
                            read.add(integerEventRead);
                            emptyCount = 0;
                        } else {
                            emptyCount++;
                        }
                    } catch (ReinitializationRequiredException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));
        }

        // Wait until all readers are done.
        futures.forEach(f -> Futures.getAndHandleExceptions(f, RuntimeException::new));
        ExecutorServiceHelpers.shutdown(executorService);
        return read;
    }
    
    private void runTestUsingMock(final Set<String> streamNames, final int numParallelReaders, final int numSegments)
            throws Exception {
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
        MockStreamManager streamManager = new MockStreamManager("scope", "localhost", servicePort);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope("scope");
        streamNames.stream().forEach(stream -> {
            streamManager.createStream("scope",
                                       stream,
                                       StreamConfiguration.builder()
                                       .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                       .build());
            @Cleanup
            EventStreamWriter<Integer> eventWriter = clientFactory.createEventWriter(stream,
                                                                                     new IntegerSerializer(),
                                                                                     EventWriterConfig.builder()
                                                                                     .build());
            for (Integer i = 0; i < NUM_TEST_EVENTS; i++) {
                eventWriter.writeEvent(String.valueOf(i), i);
            }
            eventWriter.flush();
            log.info("Wrote {} events", NUM_TEST_EVENTS);
        });

        final String readerGroupName = "testReaderGroup";
        ReaderGroupConfig.ReaderGroupConfigBuilder builder = ReaderGroupConfig.builder();
        streamNames.forEach(s -> builder.stream(Stream.of("scope", s)));
        streamManager.createReaderGroup(readerGroupName, builder.build());

        Collection<Integer> read = readAllEvents(numParallelReaders, clientFactory, readerGroupName, numSegments);

        Assert.assertEquals(NUM_TEST_EVENTS * streamNames.size(), read.size());
        // Check unique events.
        Assert.assertEquals(NUM_TEST_EVENTS, new TreeSet<>(read).size());
        streamManager.deleteReaderGroup(readerGroupName);
    }
}
