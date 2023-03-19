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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


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
        Service controllerService = Utils.createPravegaControllerService(null);
        List<URI> controllerURIs = controllerService.getServiceDetails();
        controllerURI = controllerURIs.get(0);

        clientConfig = Utils.buildClientConfig(controllerURI);

        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(), executor);
        streamManager = StreamManager.create(clientConfig);
    }

    @After
    public void tearDown() {
        streamManager.close();
        controller.close();
        ExecutorServiceHelpers.shutdown(executor);
        ExecutorServiceHelpers.shutdown(streamCutExecutor);
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
        return ReaderGroupConfig.builder()
                .retentionType(type)
                .disableAutomaticCheckpoints()
                .stream(Stream.of(scope, stream)).build();
    }
}
