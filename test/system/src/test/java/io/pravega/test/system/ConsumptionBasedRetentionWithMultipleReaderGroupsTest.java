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
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;


@Slf4j
@RunWith(SystemTestRunner.class)
public class ConsumptionBasedRetentionWithMultipleReaderGroupsTest extends AbstractReadWriteTest {

    private static final String SCOPE = "testConsumptionBasedRetentionScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String SCOPE_1 = "testCBR1Scope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM = "testConsumptionBasedRetentionStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM_1 = "testCBR1Stream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM_2 = "timeBasedRetentionStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP_1 = "testConsumptionBasedRetentionReaderGroup1" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP_2 = "testConsumptionBasedRetentionReaderGroup2" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP_3 = "testCBR1ReaderGroup1" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP_4 = "timeBasedRetentionReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String SIZE_30_EVENT = "data of size 30";
    private static final long CLOCK_ADVANCE_INTERVAL = 5 * 1000000000L;

    private static final int READ_TIMEOUT = 1000;
    private static final int MAX_SIZE_IN_STREAM = 180;
    private static final int MIN_SIZE_IN_STREAM = 90;
    private static final int MIN_TIME_IN_STREAM = 10;
    private static final int MAX_TIME_IN_STREAM = 600;
    private static final ScalingPolicy SCALING_POLICY = ScalingPolicy.fixed(1);
    private static final RetentionPolicy RETENTION_POLICY_BY_SIZE = RetentionPolicy.bySizeBytes(MIN_SIZE_IN_STREAM, MAX_SIZE_IN_STREAM);
    private static final RetentionPolicy RETENTION_POLICY_BY_TIME = RetentionPolicy.byTime(Duration.ofSeconds(MIN_TIME_IN_STREAM), Duration.ofSeconds(MAX_TIME_IN_STREAM));
    private static final StreamConfiguration STREAM_CONFIGURATION = StreamConfiguration.builder().scalingPolicy(SCALING_POLICY).retentionPolicy(RETENTION_POLICY_BY_SIZE).build();
    private static final StreamConfiguration TIME_BASED_RETENTION_STREAM_CONFIGURATION = StreamConfiguration.builder().scalingPolicy(SCALING_POLICY).retentionPolicy(RETENTION_POLICY_BY_TIME).build();
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(4 * 60);

    private final ReaderConfig readerConfig = ReaderConfig.builder().build();
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScheduledExecutorService streamCutExecutor = ExecutorServiceHelpers.newScheduledThreadPool(2, "streamCutExecutor");
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;
    private ClientConfig clientConfig;
    private Service controllerService = null;
    private Service segmentStoreService = null;

    /**
     * This is used to setup the various services required by the system test framework.
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        Service zkService = Utils.createZookeeperService();
        assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);
        controllerService = Utils.createPravegaControllerService(zkUris.get(0));
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }
        List<URI> controllerUris = controllerService.getServiceDetails();
        // Fetch all the RPC endpoints and construct the client URIs.
        List<String> uris = controllerUris.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        controllerURI = URI.create(TCP + String.join(",", uris));
        clientConfig = Utils.buildClientConfig(controllerURI);
        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(), executor);
        streamManager = StreamManager.create(clientConfig);

        segmentStoreService = Utils.createPravegaSegmentStoreService(zkUris.get(0), controllerURI);
    }

    @After
    public void tearDown() throws ExecutionException {
        streamManager.close();
        controller.close();
        ExecutorServiceHelpers.shutdown(executor);
        ExecutorServiceHelpers.shutdown(streamCutExecutor);
        Futures.getAndHandleExceptions(controllerService.scaleService(1), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreService.scaleService(1), ExecutionException::new);
    }

    @Test
    public void multipleSubscriberCBRTest() throws Exception {
        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, STREAM_CONFIGURATION));

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // Write 7 events to the stream.
        writingEventsToStream(7, writer, SCOPE, STREAM);

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        ReaderGroupConfig readerGroupConfig = getReaderGroupConfig(SCOPE, STREAM, ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT);
        boolean createRG1 = readerGroupManager.createReaderGroup(READER_GROUP_1, readerGroupConfig);
        assertTrue("Reader group 1 is not created", createRG1);
        boolean createRG2 = readerGroupManager.createReaderGroup(READER_GROUP_2, readerGroupConfig);
        assertTrue("Reader group 2 is not created", createRG2);

        ReaderGroup readerGroup1 = readerGroupManager.getReaderGroup(READER_GROUP_1);
        ReaderGroup readerGroup2 = readerGroupManager.getReaderGroup(READER_GROUP_2);
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader(READER_GROUP_1 + "-" + 1,
                READER_GROUP_1, new JavaSerializer<>(), readerConfig, clock::get, clock::get);
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader(READER_GROUP_2 + "-" + 1,
                READER_GROUP_2, new JavaSerializer<>(), readerConfig, clock::get, clock::get);

        // Read three events with reader1.
        readingEventsFromStream(3, reader1);

        log.info("{} generating 1st stream-cuts for {}/{}", READER_GROUP_1, SCOPE, STREAM);
        Map<Stream, StreamCut> streamCuts1 = generateStreamCuts(readerGroup1, reader1, clock);
        log.info("{} generated 1st Stream cut at -> {}", READER_GROUP_1, streamCuts1);

        // Read four events with reader2.
        readingEventsFromStream(4, reader2);

        log.info("{} generating 1st stream-cuts for {}/{}", READER_GROUP_2, SCOPE, STREAM);
        Map<Stream, StreamCut> streamCuts2 = generateStreamCuts(readerGroup2, reader2, clock);
        log.info("{} updating its retention stream-cut to {}", READER_GROUP_1, streamCuts1);
        readerGroup1.updateRetentionStreamCut(streamCuts1);
        log.info("{} updating its retention stream-cut to {}", READER_GROUP_2, streamCuts2);
        readerGroup2.updateRetentionStreamCut(streamCuts2);

        // Retention set has one stream cut at 0/210
        // READER_GROUP_1 updated stream cut at 0/90, READER_GROUP_2 updated stream cut at 0/120
        // Subscriber lower bound is 0/90, truncation should happen at this point
        // The timeout is set to 2 minutes a little longer than the retention period which is set to 1 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at streamcut generated by first subscriber
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 90.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off == 90),
                5000, 2 * 60 * 1000L);

        // fill stream with 4 events
        writingEventsToStream(4, writer, SCOPE, STREAM);

        //Read 5 events with reader 1 and reader 2
        readingEventsFromStream(5, reader1);
        readingEventsFromStream(5, reader2);

        log.info("{} generating 2nd stream-cuts for {}/{}", READER_GROUP_1, SCOPE, STREAM);
        streamCuts1 = generateStreamCuts(readerGroup1, reader1, clock);
        log.info("{} generated 2nd Stream cut at -> {}", READER_GROUP_1, streamCuts1);

        log.info("{} generating 2nd stream-cuts for {}/{}", READER_GROUP_2, SCOPE, STREAM);
        streamCuts2 = generateStreamCuts(readerGroup2, reader2, clock);

        log.info("{} updating its retention stream-cut to {}", READER_GROUP_1, streamCuts1);
        readerGroup1.updateRetentionStreamCut(streamCuts1);
        log.info("{} updating its retention stream-cut to {}", "RG2", streamCuts2);
        readerGroup2.updateRetentionStreamCut(streamCuts2);

        // Retention set has two stream cut at 0/210, 0/330
        // READER_GROUP_1 updated stream cut at 0/270, READER_GROUP_2 updated stream cut at 0/300
        // Subscriber lower bound is 0/270, but since truncating at SLB leaves less data in the stream than minimum limit
        // So truncating at stream cut 0/210 which leaves more data in the stream than min limit
        // The timeout is set to 2 minutes a little longer than the retention period which is set to 1 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at min stream cut from retention set
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 210.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off == 210),
                5000, 2 * 60 * 1000L);

        // fill stream with 5 more events
        writingEventsToStream(5, writer, SCOPE, STREAM);

        // Retention set has two stream cut at 0/330, 0/480
        // READER_GROUP_1 updated stream cut at 0/270, READER_GROUP_2 updated stream cut at 0/300
        // Subscriber lower bound is 0/270, but since truncating at SLB leaves more data in the stream than maximum limit
        // So truncating at stream cut i.e. 0/330 which leave less data in the stream the maximum limit
        // The timeout is set to 2 minutes a little longer than the retention period which is set to 1 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at max stream cut
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 330.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off == 330),
                5000, 2 * 60 * 1000L);

    }

    @Test
    public void updateRetentionPolicyForCBRTest() throws Exception {
        assertTrue("Creating scope", streamManager.createScope(SCOPE_1));
        assertTrue("Creating stream", streamManager.createStream(SCOPE_1, STREAM_1, STREAM_CONFIGURATION));
        assertTrue("Creating stream", streamManager.createStream(SCOPE_1, STREAM_2, TIME_BASED_RETENTION_STREAM_CONFIGURATION));

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_1, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_1, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(STREAM_2, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // Write 7 events to the stream_1.
        writingEventsToStream(7, writer, SCOPE_1, STREAM_1);
        // Write 10 events to the stream_2.
        writingEventsToStream(10, writer2, SCOPE_1, STREAM_2);

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE_1, clientConfig);
        ReaderGroupConfig readerGroupConfig = getReaderGroupConfig(SCOPE_1, STREAM_1, ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT);
        ReaderGroupConfig readerGroupConfig2 = getReaderGroupConfig(SCOPE_1, STREAM_2, ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT);

        assertTrue("Reader group is not created", readerGroupManager.createReaderGroup(READER_GROUP_3, readerGroupConfig));
        assertTrue("Reader group is not created", readerGroupManager.createReaderGroup(READER_GROUP_4, readerGroupConfig2));
        assertEquals(1, controller.listSubscribers(SCOPE_1, STREAM_1).join().size());
        assertEquals(1, controller.listSubscribers(SCOPE_1, STREAM_2).join().size());

        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP_3);
        @Cleanup
        ReaderGroup readerGroup2 = readerGroupManager.getReaderGroup(READER_GROUP_4);
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(READER_GROUP_3 + "-" + 1,
                READER_GROUP_3, new JavaSerializer<>(), readerConfig, clock::get, clock::get);
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader(READER_GROUP_4 + "-" + 1,
                READER_GROUP_4, new JavaSerializer<>(), readerConfig, clock::get, clock::get);

        // Read three events with reader.
        readingEventsFromStream(3, reader);
        // Read five events with reader2.
        readingEventsFromStream(5, reader2);

        log.info("{} generating 1st stream-cuts for {}/{}", READER_GROUP_3, SCOPE_1, STREAM_1);
        Map<Stream, StreamCut> streamCuts = generateStreamCuts(readerGroup, reader, clock);
        log.info("{} generating 1st stream-cuts for {}/{}", READER_GROUP_4, SCOPE_1, STREAM_2);
        Map<Stream, StreamCut> streamCuts2 = generateStreamCuts(readerGroup2, reader2, clock);

        log.info("{} updating its retention stream-cut to {}", READER_GROUP_3, streamCuts);
        readerGroup.updateRetentionStreamCut(streamCuts);
        log.info("{} updating its retention stream-cut to {}", READER_GROUP_4, streamCuts2);
        readerGroup2.updateRetentionStreamCut(streamCuts2);

        // Retention set has one stream cut at 0/210
        // READER_GROUP_3 updated stream cut at 0/90
        // Subscriber lower bound is 0/90, truncation should happen at this point
        // The timeout is set to 2 minutes a little longer than the retention period which is set to 1 minutes
        // in order to confirm that the retention has taken place.
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 90.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE_1, STREAM_1), 0L).join().values().stream().anyMatch(off -> off == 90),
                5000, 2 * 60 * 1000L);
        //READER_GROUP_4 updated stream cut at 0/150
        //Retention set has one stream cut at 0/300 but this is not satisfying the time based min criteria, so no truncation should happen
        assertEquals(true, controller.getSegmentsAtTime(
                new StreamImpl(SCOPE_1, STREAM_2), 0L).join().values().stream().anyMatch(off -> off == 0));

        ReaderGroupConfig nonSubscriberReaderGroupConfig = getReaderGroupConfig(SCOPE_1, STREAM_1, ReaderGroupConfig.StreamDataRetention.NONE);
        ReaderGroupConfig nonSubscriberReaderGroupConfig2 = getReaderGroupConfig(SCOPE_1, STREAM_2, ReaderGroupConfig.StreamDataRetention.NONE);
        //Changing the readergroup from subscriber to non-subscriber
        readerGroup.resetReaderGroup(nonSubscriberReaderGroupConfig);
        readerGroup2.resetReaderGroup(nonSubscriberReaderGroupConfig2);
        assertEquals(0, controller.listSubscribers(SCOPE_1, STREAM_1).join().size());
        assertEquals(0, controller.listSubscribers(SCOPE_1, STREAM_2).join().size());

        // Fill 5 more events to the stream.
        writingEventsToStream(5, writer, SCOPE_1, STREAM_1);
        writingEventsToStream(5, writer2, SCOPE_1, STREAM_2);

        // Retention set has two stream cut at 0/210...0/360
        // READER_GROUP_3 is not a subscriber as its retention type is updated to NONE
        // The timeout is set to 2 minutes a little longer than the retention period which is set to 1 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at streamcut in the retention set
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 210.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE_1, STREAM_1), 0L).join().values().stream().anyMatch(off -> off == 210),
                5000, 2 * 60 * 1000L);
        //Retention set has two stream cuts at 0/300...0/450
        //Stream cut 0/300 generated in the previous retention cycle satisfies the min and max criteria. So truncation should happen at this
        assertEquals(true, controller.getSegmentsAtTime(
                new StreamImpl(SCOPE_1, STREAM_2), 0L).join().values().stream().anyMatch(off -> off == 300));

        //Changing the readergroup from non-subscriber to subscriber again
        readerGroup.resetReaderGroup(readerGroupConfig);
        readerGroup2.resetReaderGroup(readerGroupConfig2);
        assertEquals(1, controller.listSubscribers(SCOPE_1, STREAM_1).join().size());
        assertEquals(1, controller.listSubscribers(SCOPE_1, STREAM_2).join().size());

        // Recreates the reader
        reader = clientFactory.createReader(READER_GROUP_3 + "-" + 1, READER_GROUP_3, new JavaSerializer<>(),
                ReaderConfig.builder().build(), clock::get, clock::get);
        reader2 = clientFactory.createReader(READER_GROUP_4 + "-" + 1, READER_GROUP_4, new JavaSerializer<>(),
                ReaderConfig.builder().build(), clock::get, clock::get);

        // fill stream with 3 events
        writingEventsToStream(3, writer, SCOPE_1, STREAM_1);
        // fill stream with 4 events
        writingEventsToStream(4, writer2, SCOPE_1, STREAM_2);

        //Read 3 events with reader from the segment offset
        readingEventsFromStream(3, reader);
        readingEventsFromStream(3, reader2);

        log.info("{} generating 2nd stream-cuts for {}/{}", READER_GROUP_3, SCOPE_1, STREAM_1);
        streamCuts = generateStreamCuts(readerGroup, reader, clock);
        log.info("{} updating its retention stream-cut to {}", READER_GROUP_3, streamCuts);
        readerGroup.updateRetentionStreamCut(streamCuts);

        log.info("{} generating 2nd stream-cuts for {}/{}", READER_GROUP_4, SCOPE_1, STREAM_2);
        streamCuts2 = generateStreamCuts(readerGroup2, reader2, clock);
        log.info("{} updating its retention stream-cut to {}", READER_GROUP_4, streamCuts2);
        readerGroup2.updateRetentionStreamCut(streamCuts2);

        // Retention set has two stream cut at 0/360, 0/450
        // READER_GROUP_3 updated stream cut at 0/300, Subscriber lower bound is 0/300
        // So truncating at stream cut 0/300 which leaves more data in the stream than min limit
        // The timeout is set to 2 minutes a little longer than the retention period which is set to 1 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at SLB
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 300.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE_1, STREAM_1), 0L).join().values().stream().anyMatch(off -> off == 300),
                5000, 2 * 60 * 1000L);
        // Retention set has two stream cuts at 0/450...0/570
        // READER_GROUP_4 updated stream cut at 0/390, Subscriber lower bound is 0/390
        // Since 0/450 is the stream cut satisfying min and max bounds and SLB is lower than this.
        // Truncation should happen at SLB
        assertEquals(true, controller.getSegmentsAtTime(
                new StreamImpl(SCOPE_1, STREAM_2), 0L).join().values().stream().anyMatch(off -> off == 390));
    }

    @Test
    public void multipleControllerFailoverCBRTest() throws Exception {
        Random random = RandomFactory.create();
        String scope = "testCBR2Scope" + random.nextInt(Integer.MAX_VALUE);
        String stream = "multiControllerStream" + random.nextInt(Integer.MAX_VALUE);
        String readerGroupName = "testmultiControllerReaderGroup" + random.nextInt(Integer.MAX_VALUE);
        // scale to three controller instances.
        scaleAndUpdateControllerURI(3);
        // scale to two segment store instances.
        Futures.getAndHandleExceptions(segmentStoreService.scaleService(2), ExecutionException::new);
        log.info("Successfully statred 2 instance of segment store service");
        assertTrue("Creating scope", streamManager.createScope(scope));
        assertTrue("Creating stream", streamManager.createStream(scope, stream, STREAM_CONFIGURATION));

        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());

        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // Write three event.
        writingEventsToStream(3, writer, scope, stream);

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        ReaderGroupConfig readerGroupConfig = getReaderGroupConfig(scope, stream, ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT);

        assertTrue("Reader group is not created", readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig));
        assertEquals(1, controller.listSubscribers(scope, stream).join().size());

        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
        AtomicLong clock = new AtomicLong();

        EventStreamReader<String> reader = clientFactory.createReader(readerGroupName + "-" + 1,
                readerGroupName, new JavaSerializer<>(), readerConfig, clock::get, clock::get);
        // Read one event with reader.
        readingEventsFromStream(1, reader);

        CompletableFuture<Checkpoint> checkpoint = initiateCheckPoint("Checkpoint", readerGroup, reader, clock);
        EventRead<String> read = reader.readNextEvent(READ_TIMEOUT);
        log.info("Reading next event after checkpoint {}", read.getEvent());
        Checkpoint cpResult = checkpoint.join();
        assertTrue(checkpoint.isDone());

        // Write two more events.
        writingEventsToStream(2, writer, scope, stream);

        // Retention set has one stream cut at 0/150
        // READER_GROUP_1 updated stream cut at 0/30
        // Subscriber lower bound is 0/30, truncation should happen at this point
        // The timeout is set to 2 minutes a little longer than the retention period which is set to 1 minutes
        // in order to confirm that the retention has taken place.
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 30.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(scope, stream), 0L).join().values().stream().anyMatch(off -> off == 30),
                5000, 2 * 60 * 1000L);

        // Validating the failover scenario - Start
        log.info("Controller and segment store failover scenario started");
        // Write three events.
        writingEventsToStream(3, writer, scope, stream);
        // Read one event with reader.
        readingEventsFromStream(1, reader);

        checkpoint = initiateCheckPoint("Checkpoint3", readerGroup, reader, clock);
        read = reader.readNextEvent(READ_TIMEOUT);
        log.info("Reading next event after checkpoint3 {}", read.getEvent());
        cpResult = checkpoint.join();
        assertTrue(checkpoint.isDone());

        //Controller Failover
        scaleAndUpdateControllerURI(1);
        log.info("Successfully scaled down controller to 1 instance");
        //SegmentStore Failover
        Futures.getAndHandleExceptions(segmentStoreService.scaleService(1), ExecutionException::new);
        log.info("Successfully scaled down segment store to 1 instance");

        // Retention set has two stream cut at 0/150...0/240
        // READER_GROUP_1 updated stream cut at 0/90
        // Subscriber lower bound is 0/90, truncation should happen at this point
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 90.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(scope, stream), 0L).join().values().stream().anyMatch(off -> off == 90),
                5000,  2 * 60 * 1000L);

        log.info("Test Executed successfully");
    }

    @Test
    public void controllerRestartCBRTest() throws Exception {
        Random random = RandomFactory.create();
        String scope = "controllerRestartScope" + random.nextInt(Integer.MAX_VALUE);
        String stream = "controllerRestartStream" + random.nextInt(Integer.MAX_VALUE);
        String readerGroupName = "controllerRestartReaderGroup" + random.nextInt(Integer.MAX_VALUE);

        assertTrue("Creating scope", streamManager.createScope(scope));
        assertTrue("Creating stream", streamManager.createStream(scope, stream, STREAM_CONFIGURATION));

        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());

        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // Write three event.
        writingEventsToStream(3, writer, scope, stream);

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        ReaderGroupConfig readerGroupConfig = getReaderGroupConfig(scope, stream, ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT);

        assertTrue("Reader group is not created", readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig));
        assertEquals(1, controller.listSubscribers(scope, stream).join().size());

        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
        AtomicLong clock = new AtomicLong();

        EventStreamReader<String> reader = clientFactory.createReader(readerGroupName + "-" + 1,
                readerGroupName, new JavaSerializer<>(), readerConfig, clock::get, clock::get);
        // Read one event with reader.
        readingEventsFromStream(1, reader);

        CompletableFuture<Checkpoint> checkpoint = initiateCheckPoint("Checkpoint", readerGroup, reader, clock);
        EventRead<String> read = reader.readNextEvent(READ_TIMEOUT);
        log.info("Reading next event after checkpoint {}", read.getEvent());
        Checkpoint cpResult = checkpoint.join();
        assertTrue(checkpoint.isDone());

        // Write two more events.
        writingEventsToStream(2, writer, scope, stream);

        // Validating the restart scenario - Start
        checkpoint = initiateCheckPoint("Checkpoint2", readerGroup, reader, clock);
        read = reader.readNextEvent(READ_TIMEOUT);
        log.info("Reading next event after checkpoint2 {}", read.getEvent());
        cpResult = checkpoint.join();
        assertTrue(checkpoint.isDone());

        //Closing all the resources before restart
        reader.close();
        readerGroup.close();
        readerGroupManager.close();
        writer.close();
        clientFactory.close();
        connectionFactory.close();
        Futures.getAndHandleExceptions(controllerService.scaleService(0), ExecutionException::new);
        log.info("Successfully stopped 1 instance of controller service");
        Futures.getAndHandleExceptions(segmentStoreService.scaleService(0), ExecutionException::new);
        log.info("Successfully stopped 1 instance of segment store service");

        Futures.getAndHandleExceptions(segmentStoreService.scaleService(1), ExecutionException::new);
        log.info("Successfully started 1 instance of segment store service");
        scaleAndUpdateControllerURI(1);
        log.info("Successfully started 1 instance of controller service");

        // Retention set has one stream cut at 0/150
        // READER_GROUP_1 updated stream cut at 0/60
        // Subscriber lower bound is 0/60, truncation should happen at this point
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 60.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(scope, stream), 0L).join().values().stream().anyMatch(off -> off == 60),
                5000,  2 * 60 * 1000L);
        log.info("Test Executed successfully");
    }

    @Test
    public void streamScalingCBRTest() throws Exception {
        Random random = RandomFactory.create();
        String scope = "streamScalingCBRScope" + random.nextInt(Integer.MAX_VALUE);
        String streamName = "streamScalingCBRStream" + random.nextInt(Integer.MAX_VALUE);
        String readerGroupName = "streamScalingCBRReaderGroup" + random.nextInt(Integer.MAX_VALUE);
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(1, 2, 1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(MIN_SIZE_IN_STREAM, MAX_SIZE_IN_STREAM))
                .build();
        assertTrue("Creating scope", streamManager.createScope(scope));
        assertTrue("Creating stream", streamManager.createStream(scope, streamName, streamConfiguration));
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // Write two events.
        writingEventsToStream(2, writer, scope, streamName);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        ReaderGroupConfig readerGroupConfig = getReaderGroupConfig(scope, streamName, ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT);

        assertTrue("Reader group is not created", readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig));
        assertEquals(1, controller.listSubscribers(scope, streamName).join().size());

        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupName + "-" + 1,
                readerGroupName, new JavaSerializer<>(), readerConfig, clock::get, clock::get);

        // Read two events with reader.
        readingEventsFromStream(2, reader);

        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);
        Stream stream = new StreamImpl(scope, streamName);
        // Stream scaling up
        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executor).getFuture().get();
        assertTrue(status);

        // Write 7 events.
        writingEventsToStream(7, writer, scope, streamName);

        EventRead<String> eosEvent = reader.readNextEvent(READ_TIMEOUT);
        assertNull(eosEvent.getEvent()); //Reader does not yet see the data because there has been no checkpoint
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("cp1", executor);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> cpEvent = reader.readNextEvent(READ_TIMEOUT);
        assertEquals("cp1", cpEvent.getCheckpointName());
        // Read three events with reader.
        readingEventsFromStream(3, reader);
        log.info("{} generating 1st stream-cut for {}/{}", readerGroupName, scope, streamName);
        Map<Stream, StreamCut> streamCuts = generateStreamCuts(readerGroup, reader, clock);

        log.info("{} updating its retention stream-cut to {}", readerGroupName, streamCuts);
        readerGroup.updateRetentionStreamCut(streamCuts);
        AssertExtensions.assertEventuallyEquals("Truncation did not take place.", true, () -> controller.getSegmentsAtTime(
                        stream, 0L).join().equals(streamCuts.values().stream().findFirst().get().asImpl().getPositions()),
                5000, 2 * 60 * 1000L);

        // Read two events with reader.
        readingEventsFromStream(2, reader);
        //Stream scaling down
        status = controller.scaleStream(stream, Arrays.asList(NameUtils.computeSegmentId(1, 1), NameUtils.computeSegmentId(2, 1)),
                Collections.singletonMap(0.0, 1.0), executor).getFuture().get();
        assertTrue(status);

        // Write 3 events.
        writingEventsToStream(3, writer, scope, streamName);
        log.info("{} generating 2nd stream-cut for {}/{}", readerGroupName, scope, streamName);
        Map<Stream, StreamCut> streamCuts2 = generateStreamCuts(readerGroup, reader, clock);

        log.info("{} updating its retention stream-cut to {}", readerGroupName, streamCuts2);
        readerGroup.updateRetentionStreamCut(streamCuts2);
        AssertExtensions.assertEventuallyEquals("Truncation did not take place.", true, () -> controller.getSegmentsAtTime(
                        stream, 0L).join().equals(streamCuts2.values().stream().findFirst().get().asImpl().getPositions()),
                5000, 2 * 60 * 1000L);
        log.info("streamScalingCBRTest executed successfully");
    }

    private void writingEventsToStream(int numberOfEvents, EventStreamWriter<String> writer, String scope, String stream) {
        for (int event = 0; event < numberOfEvents; event++) {
            log.info("Writing event to {}/{}", scope, stream);
            writer.writeEvent(SIZE_30_EVENT).join();
        }
    }

    private void readingEventsFromStream(int numberOfEvents, EventStreamReader<String> reader) {
        EventRead<String> read;
        for (int event = 0; event < numberOfEvents; event++) {
            read = reader.readNextEvent(READ_TIMEOUT);
            assertEquals(SIZE_30_EVENT, read.getEvent());
        }
    }

    private Map<Stream, StreamCut> generateStreamCuts(ReaderGroup readerGroup, EventStreamReader<String> reader, AtomicLong clock) {
        CompletableFuture<Map<Stream, StreamCut>> futureCuts = readerGroup.generateStreamCuts(streamCutExecutor);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(READ_TIMEOUT);
        assertEquals(SIZE_30_EVENT, read.getEvent());
        assertTrue("Stream-cut generation did not complete for reader group", Futures.await(futureCuts, 10000));
        return futureCuts.join();
    }

        private ReaderGroupConfig getReaderGroupConfig(String scope, String stream, ReaderGroupConfig.StreamDataRetention type) {
            ReaderGroupConfig readerGroupConfig = null;
            switch (type) {
                case MANUAL_RELEASE_AT_USER_STREAMCUT:
                case NONE:
                    readerGroupConfig = ReaderGroupConfig.builder()
                                        .retentionType(type)
                                        .disableAutomaticCheckpoints()
                                        .stream(Stream.of(scope, stream)).build();
                    break;

                case AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT:
                    readerGroupConfig = ReaderGroupConfig.builder()
                                        .retentionType(type)
                                        .stream(Stream.of(scope, stream)).build();
                    break;
            }
            return readerGroupConfig;
        }

        private CompletableFuture<Checkpoint> initiateCheckPoint(String checkPointName, ReaderGroup readerGroup, EventStreamReader<String> reader, AtomicLong clock) {
            CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint(checkPointName, executor);
            clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
            assertFalse(checkpoint.isDone());
            EventRead<String> read = reader.readNextEvent(READ_TIMEOUT);
            assertTrue(read.isCheckpoint());
            assertEquals(checkPointName, read.getCheckpointName());
            assertNull(read.getEvent());
            return checkpoint;
        }

        private void scaleAndUpdateControllerURI(int instanceCount) throws ExecutionException {
            Futures.getAndHandleExceptions(controllerService.scaleService(instanceCount), ExecutionException::new);
            List<URI> controllerUris = controllerService.getServiceDetails();
            log.info("Pravega Controller service  details: {}", controllerUris);
            List<String> uris = controllerUris.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
            assertEquals(instanceCount + " controller instances should be running", instanceCount, uris.size());
            controllerURI = URI.create(TCP + String.join(",", uris));
            clientConfig = Utils.buildClientConfig(controllerURI);
            controller = new ControllerImpl(ControllerImplConfig.builder()
                    .clientConfig(clientConfig)
                    .maxBackoffMillis(5000).build(), executor);
            streamManager = StreamManager.create(clientConfig);
        }
}
