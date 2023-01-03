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

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;
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
import io.pravega.client.stream.impl.JavaSerializer;
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
import java.util.stream.Collectors;
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
public class BatchClientSimpleTest extends AbstractReadWriteTest {

    private static final String STREAM = "testBatchClientStream";
    private static final String SCOPE = "testBatchClientScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testBatchClientRG" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final int RG_PARALLELISM = 4;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(RG_PARALLELISM);
    private final StreamConfiguration config = StreamConfiguration.builder()
                                                                  .scalingPolicy(scalingPolicy).build();
    private URI controllerURI = null;
    private StreamManager streamManager = null;

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
     * This test verifies the basic functionality of {@link BatchClientFactory}, including stream metadata checks, segment
     * counts, parallel segment reads and reads with offsets using stream cuts.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void batchClientSimpleTest() {
        final int totalEvents = RG_PARALLELISM * 100;
        final int offsetEvents = RG_PARALLELISM * 20;
        final int batchIterations = 4;
        final Stream stream = Stream.of(SCOPE, STREAM);
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                                                                            connectionFactory.getInternalExecutor());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE, clientConfig);
        log.info("Invoking batchClientSimpleTest test with Controller URI: {}", controllerURI);
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                      .stream(SCOPE + "/" + STREAM).build());
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);

        log.info("Writing events to stream");
        // Write events to the Stream.
        writeEvents(clientFactory, STREAM, totalEvents);

        // Instantiate readers to consume from Stream up to truncatedEvents.
        List<CompletableFuture<Integer>> futures = readEventFutures(clientFactory, READER_GROUP, RG_PARALLELISM, offsetEvents);
        Futures.allOf(futures).join();

        // Create a stream cut on the specified offset position.
        Checkpoint cp = readerGroup.initiateCheckpoint("batchClientCheckpoint", executor).join();
        StreamCut streamCut = cp.asImpl().getPositions().values().iterator().next();

        // Instantiate the batch client and assert it provides correct stream info.
        log.debug("Creating batch client.");
        StreamInfo streamInfo = streamManager.fetchStreamInfo(SCOPE, stream.getStreamName()).join();
        log.debug("Validating stream metadata fields.");
        assertEquals("Expected Stream name: ", STREAM, streamInfo.getStreamName());
        assertEquals("Expected Scope name: ", SCOPE, streamInfo.getScope());

        // Test that we can read events from parallel segments from an offset onwards.
        log.debug("Reading events from stream cut onwards in parallel.");
        List<SegmentRange> ranges = Lists.newArrayList(batchClient.getSegments(stream, streamCut, StreamCut.UNBOUNDED).getIterator());
        assertEquals("Expected events read: ", totalEvents - offsetEvents, readFromRanges(ranges, batchClient));

        // Emulate the behavior of Hadoop client: i) Get tail of Stream, ii) Read from current point until tail, iii) repeat.
        log.debug("Reading in batch iterations.");
        StreamCut currentTailStreamCut = streamManager.fetchStreamInfo(SCOPE, stream.getStreamName()).join().getTailStreamCut();
        int readEvents = 0;
        for (int i = 0; i < batchIterations; i++) {
            writeEvents(clientFactory, STREAM, totalEvents);

            // Read all the existing events in parallel segments from the previous tail to the current one.
            ranges = Lists.newArrayList(batchClient.getSegments(stream, currentTailStreamCut, StreamCut.UNBOUNDED).getIterator());
            assertEquals("Expected number of segments: ", RG_PARALLELISM, ranges.size());
            readEvents += readFromRanges(ranges, batchClient);
            log.debug("Events read in parallel so far: {}.", readEvents);
            currentTailStreamCut = streamManager.fetchStreamInfo(SCOPE, stream.getStreamName()).join().getTailStreamCut();
        }

        assertEquals("Expected events read: .", totalEvents * batchIterations, readEvents);

        // Truncate the stream in first place.
        log.debug("Truncating stream at event {}.", offsetEvents);
        assertTrue(controller.truncateStream(SCOPE, STREAM, streamCut).join());

        // Test the batch client when we select to start reading a Stream from a truncation point.
        StreamCut initialPosition = streamManager.fetchStreamInfo(SCOPE, stream.getStreamName()).join().getHeadStreamCut();
        List<SegmentRange> newRanges = Lists.newArrayList(batchClient.getSegments(stream, initialPosition, StreamCut.UNBOUNDED).getIterator());
        assertEquals("Expected events read: ", (totalEvents - offsetEvents) + totalEvents * batchIterations,
                    readFromRanges(newRanges, batchClient));
        log.debug("Events correctly read from Stream: simple batch client test passed.");
    }

    // Start utils region

    private int readFromRanges(List<SegmentRange> ranges, BatchClientFactory batchClient) {
        List<CompletableFuture<Integer>> eventCounts = ranges
                .parallelStream()
                .map(range -> CompletableFuture.supplyAsync(() -> batchClient.readSegment(range, new JavaSerializer<>()))
                                               .thenApplyAsync(segmentIterator -> {
                                                   log.debug("Thread " + Thread.currentThread().getId() + " reading events.");
                                                   int numEvents = Lists.newArrayList(segmentIterator).size();
                                                   segmentIterator.close();
                                                   return numEvents;
                                               }))
                .collect(Collectors.toList());
        return eventCounts.stream().map(CompletableFuture::join).mapToInt(Integer::intValue).sum();
    }

    // End utils region
}
