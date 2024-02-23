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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.SegmentReaderManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.SegmentReaderSnapshotInternal;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.test.common.LeakDetectorTestSuite;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for Segment Reader.
 */
@Slf4j
public class SegmentReaderTest extends LeakDetectorTestSuite {

    @ClassRule
    public static final PravegaResource PRAVEGA = new PravegaResource();
    private static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes
    private final JavaSerializer<String> serializer = new JavaSerializer<>();

    @Test(timeout = 50000)
    public void testSegmentReadOnSealedStream() {
        String scope = "testSegmentReaderScope";
        String stream = "testSegmentReaderStream";
        int numOfSegment = 1;
        int noOfEvents = 50;

        createStream(scope, stream, numOfSegment);
        writeEventsIntoStream(noOfEvents, scope, stream);
        sealStream(scope, stream);

        createSegmentReaderAndReadEvents(scope, stream, numOfSegment, noOfEvents, null);
    }

    @Test(timeout = 50000)
    public void testSegmentReadOnWithMultipleSegments() {
        String scope = "testMultiSegmentReaderScope";
        String stream = "testMultiSegmentReaderStream";
        int noOfEvents = 100;
        int numOfSegments = 3;

        createStream(scope, stream, numOfSegments);
        writeEventsIntoStream(noOfEvents, scope, stream);

        sealStream(scope, stream);

        createSegmentReaderAndReadEvents(scope, stream, numOfSegments, noOfEvents, null);
    }

    @Test(timeout = 50000)
    public void testSegmentReadWithTruncatedStream() {
        String scope = "testSegmentReaderWithTruncatedScope";
        String stream = "testSegmentReaderWithTruncatedStream";
        int noOfEvents = 10;
        int numOfSegments = 1;
        createStream(scope, stream, numOfSegments);
        writeEventsIntoStream(noOfEvents, scope, stream);

        StreamCut streamCut = new StreamCutImpl(Stream.of(scope, stream), ImmutableMap.of(new Segment(scope, stream, 0L), 60L));
        truncateStream(scope, stream, streamCut);

        sealStream(scope, stream);

        streamCut = new StreamCutImpl(Stream.of(scope, stream), ImmutableMap.of(new Segment(scope, stream, 0L), 0L));
        createSegmentReaderAndReadEvents(scope, stream, numOfSegments, noOfEvents-2, streamCut);
    }

    private void closeSegmentReader(final List<SegmentReader<String>> segmentReaderList) {
        segmentReaderList.forEach(reader -> {
            try {
                reader.close();
            } catch (Exception e) {
                log.error("Unable to close segment reader due to ", e);
            }
        });
    }

    private void createStream(final String scope, final String stream, final int numOfSegments) {
        log.info("Creating stream {}/{} with number of segments {}.", scope, stream, numOfSegments);
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numOfSegments))
                .build();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(PRAVEGA.getLocalController(), Mockito.mock(ConnectionPool.class));
        // create a scope
        boolean createScopeStatus = streamManager.createScope(scope);
        log.info("Create scope status {}", createScopeStatus);
        assertTrue(createScopeStatus);
        // create a stream
        boolean createStreamStatus = streamManager.createStream(scope, stream, config);
        log.info("Create stream status {}", createStreamStatus);
        assertTrue(createStreamStatus);
    }

    private void writeEventsIntoStream(final int numberOfEvents, final String scope, final String stream) {
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build();
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, serializer,
                EventWriterConfig.builder().build());
        IntStream.range(0, numberOfEvents).forEach(v -> writer.writeEvent(DATA_OF_SIZE_30).join());
    }


    private void verifySegmentReaderSnapshot(final SegmentReaderSnapshotInternal snapshotInternal,
                                             final long expectedOffset, final boolean isFinished) {
        assertEquals(expectedOffset, snapshotInternal.getPosition());
        if (isFinished) {
            assertTrue(snapshotInternal.isEndOfSegment());
        }
    }

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    private void truncateStream(final String scope, final String stream, final StreamCut streamCut) {
        log.info("Truncating two events from stream {}/{}", scope, stream);
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(PRAVEGA.getLocalController(), Mockito.mock(ConnectionPool.class));
        boolean truncatedStream = streamManager.truncateStream(scope, stream, streamCut);
        assertTrue(truncatedStream);
    }

    private void sealStream(final String scope, final String stream) {
        log.info("Sealing stream {}/{}", scope, stream);
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(PRAVEGA.getLocalController(), Mockito.mock(ConnectionPool.class));
        boolean sealedStream = streamManager.sealStream(scope, stream);
        assertTrue(sealedStream);
    }

    private void createSegmentReaderAndReadEvents(final String scope, final String stream, final int expectedReader,
                                                  final int expectedNumOfEvents, final StreamCut streamCut) {
        log.info("Creating segment reader manager.");
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build();
        @Cleanup
        SegmentReaderManager<String> segmentReaderManager = SegmentReaderManager.create(clientConfig, serializer);
        List<SegmentReader<String>> segmentReaderList = segmentReaderManager
                .getSegmentReaders(Stream.of(scope, stream), streamCut).join();
        assertEquals(expectedReader, segmentReaderList.size());

        long timeout = 1000;
        AtomicInteger totalReadEventCount = new AtomicInteger(0);
        segmentReaderList.forEach(reader -> {
            log.info("Starting reading the events.");
            int segmentReadEventCount = 0;
            while (true) {
                try {
                    verifySegmentReaderSnapshot((SegmentReaderSnapshotInternal) reader.getSnapshot(),
                            segmentReadEventCount * 30L, false);
                    assertEquals(DATA_OF_SIZE_30, reader.read(timeout));
                    totalReadEventCount.getAndIncrement();
                    segmentReadEventCount++;
                } catch (EndOfSegmentException e) {
                    verifySegmentReaderSnapshot((SegmentReaderSnapshotInternal) reader.getSnapshot(),
                            segmentReadEventCount * 30L, true);
                    break;
                } catch (TruncatedDataException e) {
                    log.warn("Truncated data found.", e);
                    segmentReadEventCount = segmentReadEventCount + 2;
                }
            }
        });
        log.info("Reading of events is successful.");
        assertEquals(expectedNumOfEvents, totalReadEventCount.get());
        closeSegmentReader(segmentReaderList);
    }

}
