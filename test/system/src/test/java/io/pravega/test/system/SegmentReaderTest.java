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
import io.pravega.client.admin.SegmentReaderManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.SegmentReaderSnapshotInternal;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SegmentReaderTest extends AbstractReadWriteTest {

    private static final String STREAM = "testSegmentReaderStream";
    private static final String SCOPE = "testSegmentReaderScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(3);
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
     * This test verifies the basic functionality of {@link SegmentReaderManager}, including fetching segment readers for
     * stream, parallel segment reads and segment reader snapshot check.
     */
    @Test
    public void segmentReaderTest() {
        final int totalEvents = 500;
        final Stream stream = Stream.of(SCOPE, STREAM);
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        SegmentReaderManager<String> segmentReaderManager = SegmentReaderManager.create(clientConfig, new JavaSerializer<>());
        log.info("Invoking segmentReader test with Controller URI: {}", controllerURI);

        List<SegmentReader<String>> segmentReaderList = segmentReaderManager.getSegmentReaders(stream, null).join();

        log.info("Writing events to stream {}.", stream);
        // Write events to the Stream.
        writeEvents(clientFactory, STREAM, totalEvents);
        log.info("Writing events to stream {} finished.", stream);

        log.debug("Sealing stream {}.", stream);
        streamManager.sealStream(SCOPE, STREAM);

        log.info("Reading events from stream {}.", stream);
        int totalEventRead = readFromSegmentReaders(segmentReaderList);

        assertEquals("Expected events read", totalEventRead, totalEvents);

    }

    // Start utils region

    private int readFromSegmentReaders(List<SegmentReader<String>> segmentReaders) {
        return segmentReaders.parallelStream()
                .map(reader -> CompletableFuture.supplyAsync(() -> readEvents(reader))).collect(Collectors.toList())
                .stream().map(CompletableFuture::join).mapToInt(Integer::intValue).sum();
    }

    private int readEvents(SegmentReader<String> segmentReader) {
        int eventRead = 0;

        while (true) {
            try {
                segmentReader.read(1000);
                eventRead++;
            } catch (EndOfSegmentException e) {
                log.info("Segment reader {} processed all the events.", segmentReader);
                break;
            } catch (TruncatedDataException e) {
                log.warn("TruncatedDataException occurred while reading from reader {}.", segmentReader);
            }
        }

        SegmentReaderSnapshotInternal segmentReaderSnapshot = (SegmentReaderSnapshotInternal) segmentReader.getSnapshot();
        assertTrue("IsEndOfSegment", segmentReaderSnapshot.isEndOfSegment());
        assertNotEquals("Segment reader position", segmentReaderSnapshot.getPosition(), 0);
        return eventRead;
    }

    // End utils region
}
