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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@Slf4j
@RunWith(SystemTestRunner.class)
public class ConsumptionBasedRetentionWithMultipleReaderGroupsTest extends AbstractReadWriteTest {

    private static final String SCOPE = "testConsumptionBasedRetentionScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM = "testConsumptionBasedRetentionStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP_1 = "testConsumptionBasedRetentionReaderGroup1" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP_2 = "testConsumptionBasedRetentionReaderGroup2" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String SIZE_30_EVENT = "data of size 30";

    private static final int READ_TIMEOUT = 1000;
    private static final int MAX_SIZE_IN_STREAM = 180;
    private static final int MIN_SIZE_IN_STREAM = 90;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(7 * 60);

    private final ReaderConfig readerConfig = ReaderConfig.builder().build();
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScheduledExecutorService streamCutExecutor = ExecutorServiceHelpers.newScheduledThreadPool(2, "streamCutExecutor");
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;

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

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(), executor);
        streamManager = StreamManager.create(clientConfig);

        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM,
                StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(1))
                        .retentionPolicy(RetentionPolicy.bySizeBytes(MIN_SIZE_IN_STREAM, MAX_SIZE_IN_STREAM)).build()));
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
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // Write 7 events to the stream.
        for (int i = 1; i <= 7; i++) {
            log.info("Writing event e{} to {}/{}", i, SCOPE, STREAM);
            writer.writeEvent(SIZE_30_EVENT).join();
        }

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        boolean createRG1 = readerGroupManager.createReaderGroup(READER_GROUP_1, ReaderGroupConfig.builder()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM)).build());
        assertTrue("Reader group 1 is not created", createRG1);
        boolean createRG2 = readerGroupManager.createReaderGroup(READER_GROUP_2, ReaderGroupConfig.builder()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM)).build());
        assertTrue("Reader group 2 is not created", createRG2);

        ReaderGroup readerGroup1 = readerGroupManager.getReaderGroup(READER_GROUP_1);
        ReaderGroup readerGroup2 = readerGroupManager.getReaderGroup(READER_GROUP_2);
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader(READER_GROUP_1 + "-" + 1,
                READER_GROUP_1, new JavaSerializer<>(), readerConfig);
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader(READER_GROUP_2 + "-" + 1,
                READER_GROUP_2, new JavaSerializer<>(), readerConfig);

        EventRead<String> read;
        EventRead<String> read2;
        // Read three events with reader1.
        for (int i = 1; i <= 3; i++) {
            read = reader1.readNextEvent(READ_TIMEOUT);
            assertEquals("data of size 30", read.getEvent());
        }
        log.info("{} generating 1st stream-cuts for {}/{}", READER_GROUP_1, SCOPE, STREAM);
        CompletableFuture<Map<Stream, StreamCut>> futureCuts1 = readerGroup1.generateStreamCuts(streamCutExecutor);
        // Wait for 5 seconds to force reader group state update. This will allow for the silent
        // checkpoint event generated as part of generateStreamCuts to be picked and processed.
        Futures.delayedFuture(Duration.ofSeconds(5), executor).join();
        read = reader1.readNextEvent(READ_TIMEOUT);
        assertEquals("data of size 30", read.getEvent());
        assertTrue("Stream-cut generation did not complete for reader group 1", Futures.await(futureCuts1, 10000));
        Map<Stream, StreamCut> streamCuts1 = futureCuts1.join();
        log.info("{} generated 1st Stream cut at -> {}", READER_GROUP_1, streamCuts1);

        // Read four events with reader2.
        for (int i = 1; i <= 4; i++) {
            read2 = reader2.readNextEvent(READ_TIMEOUT);
            assertEquals("data of size 30", read2.getEvent());
        }

        log.info("{} generating 1st stream-cuts for {}/{}", READER_GROUP_2, SCOPE, STREAM);
        CompletableFuture<Map<Stream, StreamCut>> futureCuts2 = readerGroup2.generateStreamCuts(streamCutExecutor);
        // Wait for 5 seconds to force reader group state update. This will allow for the silent
        // checkpoint event generated as part of generateStreamCuts to be picked and processed.
        Futures.delayedFuture(Duration.ofSeconds(5), executor).join();
        read2 = reader2.readNextEvent(READ_TIMEOUT);
        assertEquals("data of size 30", read2.getEvent());
        assertTrue("Stream-cut generation did not complete for reader group 2", Futures.await(futureCuts2, 10000));
        Map<Stream, StreamCut> streamCuts2 = futureCuts2.join();
        log.info("{} updating its retention stream-cut to {}", READER_GROUP_1, streamCuts1);
        readerGroup1.updateRetentionStreamCut(streamCuts1);
        log.info("{} updating its retention stream-cut to {}", READER_GROUP_2, streamCuts2);
        readerGroup2.updateRetentionStreamCut(streamCuts2);

        // Retention set has one stream cut at 0/210
        // READER_GROUP_1 updated stream cut at 0/90, READER_GROUP_2 updated stream cut at 0/120
        // Subscriber lower bound is 0/90, truncation should happen at this point
        // The timeout is set to 3 minutes a little longer than the retention period which is set to 2 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at streamcut generated by first subscriber
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 90.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off == 90),
                5000, 3 * 60 * 1000L);

        // fill stream with 4 events
        for (int i = 1; i <= 4; i++) {
            log.info("Writing event to {}/{}", SCOPE, STREAM);
            writer.writeEvent(SIZE_30_EVENT).join();
        }

        //Read 5 events with reader 1 and reader 2
        for (int i = 1; i <= 5; i++) {
            read = reader1.readNextEvent(READ_TIMEOUT);
            assertEquals("data of size 30", read.getEvent());
            read2 = reader2.readNextEvent(READ_TIMEOUT);
            assertEquals("data of size 30", read2.getEvent());
        }

        log.info("{} generating 2nd stream-cuts for {}/{}", READER_GROUP_1, SCOPE, STREAM);
        futureCuts1 = readerGroup1.generateStreamCuts(streamCutExecutor);
        // Wait for 5 seconds to force reader group state update. This will allow for the silent
        // checkpoint event generated as part of generateStreamCuts to be picked and processed.
        Futures.delayedFuture(Duration.ofSeconds(5), executor).join();
        read = reader1.readNextEvent(READ_TIMEOUT);
        assertEquals("data of size 30", read.getEvent());
        assertTrue("Stream-cut generation did not complete for reader group 1", Futures.await(futureCuts1, 10000));
        streamCuts1 = futureCuts1.join();
        log.info("{} generated 2nd Stream cut at -> {}", READER_GROUP_1, streamCuts1);

        log.info("{} generating 2nd stream-cuts for {}/{}", READER_GROUP_2, SCOPE, STREAM);
        futureCuts2 = readerGroup2.generateStreamCuts(streamCutExecutor);
        // Wait for 5 seconds to force reader group state update. This will allow for the silent
        // checkpoint event generated as part of generateStreamCuts to be picked and processed.
        Futures.delayedFuture(Duration.ofSeconds(5), executor).join();
        read2 = reader2.readNextEvent(READ_TIMEOUT);
        assertEquals("data of size 30", read2.getEvent());
        assertTrue("Stream-cut generation did not complete for reader group 2", Futures.await(futureCuts2, 10000));
        streamCuts2 = futureCuts2.join();

        log.info("{} updating its retention stream-cut to {}", READER_GROUP_1, streamCuts1);
        readerGroup1.updateRetentionStreamCut(streamCuts1);
        log.info("{} updating its retention stream-cut to {}", "RG2", streamCuts2);
        readerGroup2.updateRetentionStreamCut(streamCuts2);

        // Retention set has two stream cut at 0/210, 0/330
        // READER_GROUP_1 updated stream cut at 0/270, READER_GROUP_2 updated stream cut at 0/300
        // Subscriber lower bound is 0/270, but since truncating at SLB leaves less data in the stream than minimum limit
        // So truncating at stream cut 0/210 which leaves more data in the stream than min limit
        // The timeout is set to 3 minutes a little longer than the retention period which is set to 2 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at min stream cut from retention set
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 210.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off == 210),
                5000, 3 * 60 * 1000L);

        // fill stream with 5 more events
        for (int i = 1; i <= 5; i++) {
            log.info("Writing event to {}/{}", SCOPE, STREAM);
            writer.writeEvent(SIZE_30_EVENT).join();
        }

        // Retention set has two stream cut at 0/330, 0/480
        // READER_GROUP_1 updated stream cut at 0/270, READER_GROUP_2 updated stream cut at 0/300
        // Subscriber lower bound is 0/270, but since truncating at SLB leaves more data in the stream than maximum limit
        // So truncating at stream cut i.e. 0/330 which leave less data in the stream the maximum limit
        // The timeout is set to 3 minutes a little longer than the retention period which is set to 2 minutes
        // in order to confirm that the retention has taken place.
        // Check to make sure truncation happened at max stream cut
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 330.", true, () -> controller.getSegmentsAtTime(
                        new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off == 330),
                5000, 3 * 60 * 1000L);

    }
}
