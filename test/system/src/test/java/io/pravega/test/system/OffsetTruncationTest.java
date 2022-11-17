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
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class OffsetTruncationTest extends AbstractReadWriteTest {

    private static final String STREAM = "testOffsetTruncationStream";
    private static final String SCOPE = "testOffsetTruncationScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testOffsetTruncationRG" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final int PARALLELISM = 2;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "executor");
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(PARALLELISM);
    private final StreamConfiguration config = StreamConfiguration.builder()
                                                                  .scalingPolicy(scalingPolicy).build();
    private URI controllerURI;
    private StreamManager streamManager;

    /**
     * This is used to setup the services required by the system test framework.
     *
     * @throws MarathonException When error in setup.
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
        streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    /**
     * This test verifies that truncation works specifying an offset that applies to multiple segments. To this end,
     * the test first writes a set of events on a Stream (with multiple segments) and truncates it at a specified offset
     * (truncatedEvents). The tests asserts that readers first get a TruncatedDataException as they are attempting to
     * read a truncated segment, and then they only read the remaining events that have not been truncated.
     */
    @Test
    public void offsetTruncationTest() {
        final int totalEvents = 200;
        final int truncatedEvents = 50;

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                                                                           .clientConfig(clientConfig).build(),
                                                                           connectionFactory.getInternalExecutor());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        log.info("Invoking offsetTruncationTest test with Controller URI: {}", controllerURI);

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM)).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);

        // Write events to the Stream.
        writeEvents(clientFactory, STREAM, totalEvents);

        // Instantiate readers to consume from Stream up to truncatedEvents.
        List<CompletableFuture<Integer>> futures = readEventFutures(clientFactory, READER_GROUP, PARALLELISM, truncatedEvents);
        Futures.allOf(futures).join();
        // Ensure that we have read all the events required before initiating the checkpoint.
        assertEquals("Number of events read is not the expected one.", (Integer) truncatedEvents,
                futures.stream().map(f -> Futures.getAndHandleExceptions(f, RuntimeException::new)).reduce(Integer::sum).get());

        // Perform truncation on stream segment.
        Checkpoint cp = readerGroup.initiateCheckpoint("truncationCheckpoint", executor).join();
        StreamCut streamCut = cp.asImpl().getPositions().values().iterator().next();
        StreamCut alternativeStreamCut = readerGroup.generateStreamCuts(executor).join().get(Stream.of(SCOPE, STREAM));
        assertEquals("StreamCuts for reader group differ depending on how they are generated.", streamCut, alternativeStreamCut);
        assertTrue(streamManager.truncateStream(SCOPE, STREAM, streamCut));

        // Just after the truncation, read events from the offset defined in truncate call onwards.
        final String newGroupName = READER_GROUP + "new";
        groupManager.createReaderGroup(newGroupName, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM)).build());
        futures = readEventFutures(clientFactory, newGroupName, PARALLELISM);
        Futures.allOf(futures).join();
        assertEquals("Expected read events: ", totalEvents - truncatedEvents,
                (int) futures.stream().map(CompletableFuture::join).reduce(Integer::sum).get());
        log.debug("The stream has been successfully truncated at event {}. Offset truncation test passed.", truncatedEvents);
    }
}
