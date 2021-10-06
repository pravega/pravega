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
import io.pravega.common.Exceptions;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SingleSubscriberUpdateRetentionStreamCutTest extends AbstractReadWriteTest {

    private static final String SCOPE = "testCBRScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM = "testCBRStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testCBRReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String SIZE_30_EVENT = "data of size 30";

    private static final int READ_TIMEOUT = 1000;
    private static final int MAX_SIZE_IN_STREAM = 300;
    private static final int MIN_SIZE_IN_STREAM = 30;

    private final ReaderConfig readerConfig = ReaderConfig.builder().build();
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScheduledExecutorService streamCutExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "streamCutExecutor");
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;

    /**
     * This is used to setup the various services required by the system test framework.
     *
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
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);

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
        ExecutorServiceHelpers.shutdown(executor);
        ExecutorServiceHelpers.shutdown(streamCutExecutor);
    }

    @Test
    public void singleSubscriberCBRTest() throws Exception {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // Write a single event.
        log.info("Writing event e1 to {}/{}", SCOPE, STREAM);
        writer.writeEvent("e1", SIZE_30_EVENT).join();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        readerGroupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM)).build());
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(READER_GROUP + "-" + 1,
                READER_GROUP, new JavaSerializer<>(), readerConfig);

        // Read one event.
        log.info("Reading event e1 from {}/{}", SCOPE, STREAM);
        EventRead<String> read = reader.readNextEvent(READ_TIMEOUT);
        assertFalse(read.isCheckpoint());
        assertEquals("data of size 30", read.getEvent());

        // Update the retention stream-cut.
        log.info("{} generating stream-cuts for {}/{}", READER_GROUP, SCOPE, STREAM);
        CompletableFuture<Map<Stream, StreamCut>> futureCuts = readerGroup.generateStreamCuts(streamCutExecutor);
        // Wait for 5 seconds to force reader group state update. This will allow for the silent
        // checkpoint event generated as part of generateStreamCuts to be picked and processed.
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(5));
        EventRead<String> emptyEvent = reader.readNextEvent(READ_TIMEOUT);
        assertTrue("Stream-cut generation did not complete", Futures.await(futureCuts, 10_000));

        Map<Stream, StreamCut> streamCuts = futureCuts.join();
        log.info("{} updating its retention stream-cut to {}", READER_GROUP, streamCuts);
        readerGroup.updateRetentionStreamCut(streamCuts);

        // Write two more events.
        log.info("Writing event e2 to {}/{}", SCOPE, STREAM);
        writer.writeEvent("e2", SIZE_30_EVENT).join();
        log.info("Writing event e3 to {}/{}", SCOPE, STREAM);
        writer.writeEvent("e3", SIZE_30_EVENT).join();

        // Check to make sure truncation happened after the first event.
        // The timeout is 5 minutes as the retention period is set to 2 minutes. We allow for 2 cycles to fully complete
        // and a little longer in order to confirm that the retention has taken place.
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 30.", true, () -> controller.getSegmentsAtTime(
                new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off >= 30),
                1000, 5 * 60 * 1000L);

        // Read next event.
        log.info("Reading event e2 from {}/{}", SCOPE, STREAM);
        read = reader.readNextEvent(READ_TIMEOUT);
        assertFalse(read.isCheckpoint());
        assertEquals("data of size 30", read.getEvent());

        // Update the retention stream-cut.
        log.info("{} generating stream-cuts for {}/{}", READER_GROUP, SCOPE, STREAM);
        CompletableFuture<Map<Stream, StreamCut>> futureCuts2 = readerGroup.generateStreamCuts(streamCutExecutor);
        // Wait for 5 seconds to force reader group state update. This will allow for the silent
        // checkpoint event generated as part of generateStreamCuts to be picked and processed.
        Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(5));
        EventRead<String> emptyEvent2 = reader.readNextEvent(READ_TIMEOUT);
        assertTrue("Stream-cut generation did not complete", Futures.await(futureCuts2, 10_000));

        Map<Stream, StreamCut> streamCuts2 = futureCuts2.join();
        log.info("{} updating its retention stream-cut to {}", READER_GROUP, streamCuts2);
        readerGroup.updateRetentionStreamCut(streamCuts2);

        // Check to make sure truncation happened after the second event.
        // The timeout is 5 minutes as the retention period is set to 2 minutes. We allow for 2 cycles to fully complete
        // and a little longer in order to confirm that the retention has taken place.
        AssertExtensions.assertEventuallyEquals("Truncation did not take place at offset 60", true, () -> controller.getSegmentsAtTime(
                new StreamImpl(SCOPE, STREAM), 0L).join().values().stream().anyMatch(off -> off >= 60),
                1000, 5 * 60 * 1000L);
    }
}
