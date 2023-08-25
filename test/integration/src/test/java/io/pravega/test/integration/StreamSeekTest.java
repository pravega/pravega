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
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class StreamSeekTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "testScope";
    private static final String STREAM1 = "testStreamSeek1";
    private static final String STREAM2 = "testStreamSeek2";
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final Serializer<String> serializer = new JavaSerializer<>();
    private final Random random = RandomFactory.create();
    private final Supplier<String> keyGenerator = () -> String.valueOf(random.nextInt());
    private final Function<Integer, String> getEventData = eventNumber -> String.valueOf(eventNumber) + ":constant data"; //event
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

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

    @Test(timeout = 50000)
    public void testStreamSeek() throws Exception {

        createScope(SCOPE);
        createStream(STREAM1);
        createStream(STREAM2);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM1, serializer,
                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        groupManager.createReaderGroup("group",
                                       ReaderGroupConfig.builder()
                                                        .disableAutomaticCheckpoints()
                                                        .groupRefreshTimeMillis(0)
                                                        .stream(Stream.of(SCOPE, STREAM1))
                                                        .stream(Stream.of(SCOPE, STREAM2))
                                                        .build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("group");

        //Prep the stream with data.
        //1.Write two events with event size of 30
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(1)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(2)).get();

        //2.Scale stream
        Map<Double, Double> newKeyRanges = new HashMap<>();
        newKeyRanges.put(0.0, 0.33);
        newKeyRanges.put(0.33, 0.66);
        newKeyRanges.put(0.66, 1.0);
        scaleStream(STREAM1, newKeyRanges);

        //3.Write three events with event size of 30
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(3)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(4)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(5)).get();

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", serializer,
                ReaderConfig.builder().build());

        //Offset of a streamCut is always set to zero.
        Map<Stream, StreamCut> streamCut1 = readerGroup.getStreamCuts(); //Stream cut 1
        readAndVerify(reader, 1, 2);
        assertNull(reader.readNextEvent(100).getEvent()); //Sees the segments are empty prior to scaling
        readerGroup.initiateCheckpoint("cp1", executorService()); //Checkpoint to move past the scale
        readAndVerify(reader, 3, 4, 5); // Old segments are released and new ones can be read
        Map<Stream, StreamCut> streamCut2 = readerGroup.getStreamCuts(); //Stream cut 2

        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(streamCut1).build()); //reset the readers to offset 0.
        verifyReinitializationRequiredException(reader);

        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "group", serializer,
                ReaderConfig.builder().build());

        //verify that we are at streamCut1
        readAndVerify(reader1, 1, 2);

        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(streamCut2).build()); // reset readers to post scale offset 0
        verifyReinitializationRequiredException(reader1);

        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("readerId", "group", serializer,
                ReaderConfig.builder().build());

        //verify that we are at streamCut2
        readAndVerify(reader2, 3, 4, 5);
    }

    private void scaleStream(final String streamName, final Map<Double, Double> keyRanges) throws Exception {
        Stream stream = Stream.of(SCOPE, streamName);
        Controller controller = controllerWrapper.getController();
        assertTrue(controller.scaleStream(stream, Collections.singletonList(0L), keyRanges, executorService()).getFuture().get());
    }

    private void verifyReinitializationRequiredException(EventStreamReader<String> reader) {
        try {
            reader.readNextEvent(15000);
            Assert.fail("Reinitialization Exception excepted");
        } catch (ReinitializationRequiredException e) {
            reader.close();
        }
    }

    private void readAndVerify(final EventStreamReader<String> reader, int...index) throws ReinitializationRequiredException {
        ArrayList<String> results = new ArrayList<>(index.length);
        for (int i = 0; i < index.length; i++) {
            String event = reader.readNextEvent(15000).getEvent();
            while (event == null) { //try until a non null event is read.
                event = reader.readNextEvent(15000).getEvent();
            }
            results.add(event);
        }
        Arrays.stream(index).forEach(i -> assertTrue(results.contains(getEventData.apply(i))));
    }

    private void createScope(final String scopeName) throws InterruptedException, ExecutionException {
        controllerWrapper.getControllerService().createScope(scopeName, 0L).get();
    }

    private void createStream(String streamName) throws Exception {
        Controller controller = controllerWrapper.getController();
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        controller.createStream(SCOPE, streamName, config).join();
    }
}
