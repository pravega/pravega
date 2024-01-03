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

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
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

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.segment.impl.Segment;
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
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BoundedStreamReaderTest extends LeakDetectorTestSuite {

    private static final String SCOPE = "testScope";
    private static final String STREAM1 = "testStream1";
    private static final String STREAM2 = "testStream2";
    private static final String STREAM3 = "testStream3";
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final Serializer<String> serializer = new JavaSerializer<>();
    private final Random random = new Random();
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
        super.before();
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
        super.after();
    }

    @Test(timeout = 60000)
    public void testBoundedStreamTest() throws Exception {
        createScope(SCOPE);
        createStream(STREAM1);
        createStream(STREAM2);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM1, serializer,
                EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(1)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(2)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(3)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(4)).get();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM1),
                        //startStreamCut points to the current HEAD of stream
                        StreamCut.UNBOUNDED,
                        //endStreamCut points to the offset after two events.(i.e 2 * 30(event size) = 60)
                        getStreamCut(STREAM1, 60L, 0))
                .stream(Stream.of(SCOPE, STREAM2))
                .build());

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", serializer,
                ReaderConfig.builder().build());

        //2. Verify if endStreamCut configuration is enforced.
        readAndVerify(reader, 1, 2);
        //The following read should not return events 3, 4 due to the endStreamCut configuration.
        Assert.assertNull("Null is expected", reader.readNextEvent(2000).getEvent());

        //3. Write events to the STREAM2.
        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(STREAM2, serializer,
                EventWriterConfig.builder().build());
        writer2.writeEvent(keyGenerator.get(), getEventData.apply(5)).get();
        writer2.writeEvent(keyGenerator.get(), getEventData.apply(6)).get();

        //4. Verify that events can be read from STREAM2. (Events from STREAM1 are not read since endStreamCut is reached).
        readAndVerify(reader, 5, 6);
        Assert.assertNull("Null is expected", reader.readNextEvent(2000).getEvent());

    }

    @Test(timeout = 60000)
    public void testReaderGroupWithSameBounds() throws Exception {
        createScope(SCOPE);
        createStream(STREAM1);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM1, serializer,
                                                                            EventWriterConfig.builder().build());
        // 1. Prep the stream with data.
        // Write events with event size of 30
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(1)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(2)).get();

        // 2. Create a StreamCut Pointing to offset 30L
        StreamCut streamCut = getStreamCut(STREAM1, 30L, 0);

        // 3. Create a ReaderGroup where the lower and upper bound are the same.
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM1), streamCut, streamCut)
                .build());

        // 4. Create a reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", serializer,
                                                                      ReaderConfig.builder().build());

        // 5. Verify if configuration is enforced.
        Assert.assertNull("Null is expected", reader.readNextEvent(1000).getEvent());
    }

    @Test(timeout = 60000)
    public void testBoundedStreamWithScaleTest() throws Exception {
        createScope(SCOPE);
        createStream(STREAM1);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM1, serializer,
                EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
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

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);

        ReaderGroupConfig readerGroupCfg1 = ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0)
                .stream(Stream.of(SCOPE, STREAM1),
                        //startStreamCut points to the current HEAD of stream
                        StreamCut.UNBOUNDED,
                        //endStreamCut points to the offset after two events.(i.e 2 * 30(event size) = 60)
                        getStreamCut(STREAM1, 60L, 0))
                .build();
        groupManager.createReaderGroup("group", readerGroupCfg1);
        ReaderGroup readerGroup = groupManager.getReaderGroup("group");
        //Create a reader
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId1", "group", serializer,
                ReaderConfig.builder().build());

        //2. Verify if endStreamCut configuration is enforced.
        readAndVerify(reader1, 1, 2);
        //The following read should not return events 3, 4 due to the endStreamCut configuration.
        Assert.assertNull("Null is expected", reader1.readNextEvent(2000).getEvent());

        final ReaderGroupConfig readerGroupCfg2 = ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0)
                                                             .stream(Stream.of(SCOPE, STREAM1),
                                                                     getStreamCut(STREAM1, 60L, 0),
                                                                     //endStreamCut points to the offset after two events.(i.e 2 * 30(event size) = 60)
                                                                     getStreamCut(STREAM1, 90L, 1, 2, 3))
                                                             .build();
        readerGroup.resetReaderGroup(readerGroupCfg2);
        verifyReinitializationRequiredException(reader1);

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("readerId2", "group", serializer,
                ReaderConfig.builder().build());
        assertNull(reader2.readNextEvent(100).getEvent());
        readerGroup.initiateCheckpoint("c1", executorService());
        readAndVerify(reader2, 3, 4, 5);
        Assert.assertNull("Null is expected", reader2.readNextEvent(2000).getEvent());
    }

    @Test(timeout = 60000)
    public void testBoundedStreamWithTruncationTest() throws Exception {
        createScope(SCOPE);
        createStream(STREAM3);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM3, serializer,
                EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(1)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(2)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(3)).get();
        writer1.writeEvent(keyGenerator.get(), getEventData.apply(4)).get();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        StreamCut offset30SC = getStreamCut(STREAM3, 30L, 0); // Streamcut pointing to event 2.
        StreamCut offset60SC = getStreamCut(STREAM3, 60L, 0);
        groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM3),
                        //startStreamCut points to second event in the stream.
                        offset30SC,
                        //endStreamCut points to the offset after two events.(i.e 2 * 30(event size) = 60)
                        offset60SC)
                .build());

        final ReaderGroup rg = groupManager.getReaderGroup("group");

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", serializer,
                ReaderConfig.builder().build());

        //2. Verify if endStreamCut configuration is enforced.
        readAndVerify(reader, 2);
        //The following read should not return events 3, 4 due to the endStreamCut configuration.
        Assert.assertNull("Null is expected", reader.readNextEvent(2000).getEvent());

        truncateStream(STREAM3, offset60SC);

        //Truncation should not affect the reader as it is already post the truncation point.
        Assert.assertNull("Null is expected", reader.readNextEvent(2000).getEvent());

        //Reset RG with startStreamCut which is already truncated.
        rg.resetReaderGroup(ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                             .stream(Stream.of(SCOPE, STREAM3), offset30SC, StreamCut.UNBOUNDED)
                                             .build());

        verifyReinitializationRequiredException(reader);

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("readerId2", "group", serializer,
                ReaderConfig.builder().build());

        assertThrows(TruncatedDataException.class, () -> reader2.readNextEvent(10000));
        //subsequent read should return data present post truncation, Event3 is returned here since stream was truncated @ offset 30 * 2.
        readAndVerify(reader2, 3);
    }

    /*
     * Test method to create StreamCuts. In the real world StreamCuts are obtained via the Pravega client apis.
     */
    private StreamCut getStreamCut(String streamName, long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segmentNumbers).forEach(seg -> {
            builder.put(new Segment(SCOPE, streamName, seg), offset);
        });

        return new StreamCutImpl(Stream.of(SCOPE, streamName), builder.build());
    }

    private void readAndVerify(final EventStreamReader<String> reader, int...index) throws ReinitializationRequiredException {
        ArrayList<String> results = new ArrayList<>(index.length);
        for (int i = 0; i < index.length; i++) {
            String event = reader.readNextEvent(1000).getEvent();
            while (event == null) { //try until a non null event is read.
                event = reader.readNextEvent(1000).getEvent();
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
        val f1 = controller.createStream(SCOPE, streamName, config);
        f1.get();
    }

    private void scaleStream(final String streamName, final Map<Double, Double> keyRanges) throws Exception {
        Stream stream = Stream.of(SCOPE, streamName);
        Controller controller = controllerWrapper.getController();
        assertTrue(controller.scaleStream(stream, Collections.singletonList(0L), keyRanges, executorService()).getFuture().get());
    }

    private void truncateStream(final String streamName, final StreamCut streamCut) throws Exception {
        Controller controller = controllerWrapper.getController();
        assertTrue(controller.truncateStream(SCOPE, streamName, streamCut).join());
    }

    private void verifyReinitializationRequiredException(EventStreamReader<String> reader) {
        try {
            reader.readNextEvent(15000);
            Assert.fail("Reinitialization Exception excepted");
        } catch (ReinitializationRequiredException e) {
            reader.close();
        }
    }
}
