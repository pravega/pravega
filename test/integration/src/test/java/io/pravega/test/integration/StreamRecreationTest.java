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
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class StreamRecreationTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
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
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 60000)
    @SuppressWarnings("deprecation")
    public void testStreamRecreation() throws Exception {
        final String myScope = "myScope";
        final String myStream = "myStream";
        final String myReaderGroup = "myReaderGroup";
        final int numIterations = 6;

        // Create the scope and the stream.
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(myScope);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(myScope, controllerURI);
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                                                     .stream(Stream.of(myScope, myStream))
                                                                     .build();

        for (int i = 0; i < numIterations; i++) {
            log.info("Stream re-creation iteration {}.", i);
            final String eventContent = "myEvent" + String.valueOf(i);
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(i + 1))
                                                                         .build();
            EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
            streamManager.createStream(myScope, myStream, streamConfiguration);

            // Write a single event.
            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(myScope, ClientConfig.builder().controllerURI(controllerURI).build());
            @Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(myStream, new JavaSerializer<>(),
                                                                               eventWriterConfig);
            TransactionalEventStreamWriter<String> txnWriter = clientFactory.createTransactionalEventWriter(myStream,
                                                                                                            new JavaSerializer<>(),
                                                                                                            eventWriterConfig);

            // Write events regularly and with transactions.
            if (i % 2 == 0) {
                writer.writeEvent(eventContent).join();
            } else {
                Transaction<String> myTransaction = txnWriter.beginTxn();
                myTransaction.writeEvent(eventContent);
                myTransaction.commit();
                while (myTransaction.checkStatus() != Transaction.Status.COMMITTED) {
                    Exceptions.handleInterrupted(() -> Thread.sleep(100));
                }
            }

            writer.close();

            // Read the event.
            readerGroupManager.createReaderGroup(myReaderGroup, readerGroupConfig);
            readerGroupManager.getReaderGroup(myReaderGroup).resetReaderGroup(readerGroupConfig);
            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader("myReader", myReaderGroup, new JavaSerializer<>(),
                    ReaderConfig.builder().build());
            String readResult;
            do {
                readResult = reader.readNextEvent(1000).getEvent();
            } while (readResult == null);

            assertEquals("Wrong event read in re-created stream", eventContent, readResult);

            // Delete the stream.
            StreamInfo streamInfo = streamManager.fetchStreamInfo(myScope, myStream).join();
            assertFalse(streamInfo.isSealed());
            assertTrue("Unable to seal re-created stream.", streamManager.sealStream(myScope, myStream));
            streamInfo = streamManager.fetchStreamInfo(myScope, myStream).join();
            assertTrue(streamInfo.isSealed());
            assertTrue("Unable to delete re-created stream.", streamManager.deleteStream(myScope, myStream));
        }
    }
}
