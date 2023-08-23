/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.hash.RandomFactory;
import io.pravega.shared.NameUtils;
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
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.*;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SegmentReaderAPITest extends AbstractReadWriteTest {
    private final static String STREAM_NAME = "testStreamSegment";
    private final static String STREAM_SCOPE = "testScopeSegment" + randomAlphanumeric(5);
    private final static String READER_GROUP = "testReaderGroupSegment";
    private final static int NUM_EVENTS = 4;
    private final Random random = RandomFactory.create();
    private static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder()
            .scalingPolicy(scalingPolicy)
            .build();
    private URI controllerURI = null;
    private StreamManager streamManager = null;

    /**
     * This is used to setup the various services required by the system test framework.
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
        controllerURI = fetchControllerURI();
        streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
    }

    @After
    public void tearDown() {
        streamManager.close();
    }

    @Test
    public void segmentReaderApiTest() throws SegmentTruncatedException {
        log.info("Invoking segmentReaderApiTest with Scope name {} and Stream Name {}", STREAM_SCOPE, STREAM_NAME);

        final Stream stream = Stream.of(STREAM_SCOPE, STREAM_NAME);
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(Utils.buildClientConfig(controllerURI));
        @Cleanup
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(Utils.buildClientConfig(controllerURI))
                .build(), connectionFactory.getInternalExecutor());

        assertTrue(controller.createScope(STREAM_SCOPE).join());
        //Creating of main and index segment
        assertTrue(controller.createStream(STREAM_SCOPE, STREAM_NAME, config).join());

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(STREAM_SCOPE, clientConfig);
        List<SegmentRange> ranges = Lists.newArrayList(batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        List<Segment> list = ranges.stream().map(SegmentRange::getSegment).collect(Collectors.toList());
        assertEquals(1, list.size());

        String indexStreamSegment = NameUtils.getIndexSegmentName(list.get(0).getScopedName());
        log.info("IndexSegment Name {}", indexStreamSegment);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(STREAM_SCOPE, Utils.buildClientConfig(controllerURI));

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // Appending data to the stream
        write30ByteEvents(NUM_EVENTS, writer);

        ReaderGroupManager groupManager = ReaderGroupManager.withScope(STREAM_SCOPE, Utils.buildClientConfig(controllerURI));
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(STREAM_SCOPE, STREAM_NAME)).build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());

        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);
        Map<Stream, StreamCut> initialPosition = new HashMap<>(readerGroup.getStreamCuts());

        //Creating the StreamCut
        StreamCut nextStreamCut = batchClient.getNextStreamCut(initialPosition.get(stream), 27L);
        log.info("Next stream Cut {}", nextStreamCut);
        assertTrue(nextStreamCut != null);
        assertTrue(nextStreamCut.asImpl().getPositions().size() == 1);
        assertEquals(30L, nextStreamCut.asImpl().getPositions().get(list.get(0)).longValue());

        //Validating the data
        assertEquals(DATA_OF_SIZE_30, reader.readNextEvent(10_000).getEvent());
        assertEquals(DATA_OF_SIZE_30, reader.readNextEvent(10_000).getEvent());
        assertEquals(DATA_OF_SIZE_30, reader.readNextEvent(10_000).getEvent());
        assertEquals(DATA_OF_SIZE_30, reader.readNextEvent(10_000).getEvent());
        assertNull(reader.readNextEvent(10_000).getEvent());

        //truncation of index segment -
        assertTrue(controller.truncateStream(STREAM_SCOPE, STREAM_NAME, nextStreamCut).join());
    }

    private URI fetchControllerURI() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }

    private void write30ByteEvents(int numberOfEvents, EventStreamWriter<String> writer) {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        IntStream.range(0, numberOfEvents).forEach(v -> writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).join());
    }
}