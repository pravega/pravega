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

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.integration.utils.ClusterWrapper;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * As the package + name might suggest, this class is intended to hold integration tests for verifying delegation token
 * functionality.
 */
@Slf4j
public class DelegationTokenTest {

    /*
     * Note: No normal happy path integration tests were added here, as those scenarios are already covered in
     * other tests elsewhere (using the default token TTL and valid auth credentials).
     */

    @Test(timeout = 20000)
    public void testWriteSucceedsWhenTokenTtlIsMinusOne() throws ExecutionException, InterruptedException {
        // A Token TTL -1 indicates to the Controller to not set any expiry on the delegation token.
        writeAnEvent(-1);
    }

    private void writeAnEvent(int tokenTtlInSeconds) throws ExecutionException, InterruptedException {
        ClusterWrapper pravegaCluster = ClusterWrapper.builder().authEnabled(true).tokenTtlInSeconds(600).build();
        try {
            pravegaCluster.start();

            String scope = "testscope";
            String streamName = "teststream";
            int numSegments = 1;
            String message = "test message";

            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
            log.debug("Done creating client config.");

            createScopeStream(scope, streamName, numSegments, clientConfig);

            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            @Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());

            // Note: A TokenException is thrown here if token verification fails on the server.
            writer.writeEvent(message).get();

            log.debug("Done writing message '{}' to stream '{} / {}'", message, scope, streamName);
        } finally {
            pravegaCluster.close();
        }
    }

    /**
     * This test verifies that a event stream reader continues to read events as a result of automatic delegation token
     * renewal, after the initial delegation token it uses expires.
     *
     * We use an extraordinarily high test timeout and read timeouts to account for any inordinate delays that may be
     * encountered in testing environments.
     */
    @Test(timeout = 50000)
    public void testDelegationTokenGetsRenewedAfterExpiry() throws InterruptedException {
        // Delegation token renewal threshold is 5 seconds, so we are using 6 seconds as Token TTL so that token doesn't
        // get renewed before each use.
        ClusterWrapper pravegaCluster = ClusterWrapper.builder().authEnabled(true).tokenTtlInSeconds(6).build();
        try {
            pravegaCluster.start();

            final String scope = "testscope";
            final String streamName = "teststream";
            final int numSegments = 1;

            final ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(pravegaCluster.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
            log.debug("Done creating client config.");

            createScopeStream(scope, streamName, numSegments, clientConfig);

            @Cleanup
            final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

            // Perform writes on a separate thread.
            Runnable runnable = () -> {
                @Cleanup
                EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                        new JavaSerializer<String>(),
                        EventWriterConfig.builder().build());

                for (int i = 0; i < 10; i++) {
                    String msg = "message: " + i;
                    writer.writeEvent(msg).join();
                    log.debug("Done writing message '{}' to stream '{} / {}'", msg, scope, streamName);
                }
            };
            @Cleanup("interrupt")
            Thread writerThread = new Thread(runnable);
            writerThread.start();

            // Now, read the events from the stream.

            String readerGroup = UUID.randomUUID().toString().replace("-", "");
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamName))
                    .disableAutomaticCheckpoints()
                    .build();

            @Cleanup
            ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

            @Cleanup
            EventStreamReader<String> reader = clientFactory.createReader(
                    "readerId", readerGroup,
                    new JavaSerializer<String>(), ReaderConfig.builder().build());

            int j = 0;
            EventRead<String> event = null;
            do {
                event = reader.readNextEvent(2000);
                if (event.getEvent() != null) {
                    log.info("Done reading event: {}", event.getEvent());
                    j++;
                }

                // We are keeping sleep time relatively large, just to make sure that the delegation token expires
                // midway.
                Thread.sleep(500);
            } while (event.getEvent() != null);

            // Assert that we end up reading 10 events even though delegation token must have expired midway.
            //
            // To look for evidence of delegation token renewal check the logs for the following message:
            // - "Token is nearing expiry, so refreshing it"
            assertSame(10, j);
        } finally {
            pravegaCluster.close();
        }
    }

    /**
     * This test verifies that a batch client continues to read events as a result of automatic delegation token
     * renewal, after the initial delegation token it uses expires.
     * <p>
     * We use an extraordinarily high test timeout and read timeouts to account for any inordinate delays that may be
     * encountered in testing environments.
     */
    @Test(timeout = 50000)
    public void testBatchClientDelegationTokenRenewal() throws InterruptedException {
        // Delegation token renewal threshold is 5 seconds, so we are using 6 seconds as Token TTL so that token doesn't
        // get renewed before each use.
        @Cleanup
        ClusterWrapper pravegaCluster = ClusterWrapper.builder().authEnabled(true).tokenTtlInSeconds(6).build();
        pravegaCluster.start();

        final String scope = "testscope";
        final String streamName = "teststream";

        final ClientConfig clientConfig = ClientConfig.builder()
                                                      .controllerURI(URI.create(pravegaCluster.controllerUri()))
                                                      .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                                                      .build();
        log.debug("Done creating client config.");

        // Create Scope and Stream.
        createScopeStream(scope, streamName, 1, clientConfig);
        // write ten Events.
        writeTenEvents(scope, streamName, clientConfig);

        // Now, read the events from the stream using Batch client.
        @Cleanup
        BatchClientFactory batchClientFactory = BatchClientFactory.withScope(scope, clientConfig);

        List<SegmentRange> segmentRanges = Lists.newArrayList(batchClientFactory
                .getSegments(Stream.of(scope, streamName), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        assertEquals("The number of segments in the stream is 1", 1, segmentRanges.size());
        SegmentIterator<String> segmentIterator = batchClientFactory
                .readSegment(segmentRanges.get(0), new JavaSerializer<>());

        int eventReadCount = 0;

        while (segmentIterator.hasNext()) {
            // We are keeping sleep time relatively large, just to make sure that the delegation token expires
            // midway.
            Thread.sleep(500);
            String event = segmentIterator.next();
            log.debug("Done reading event {}", event);
            eventReadCount++;
        }
        // Assert that we end up reading 10 events even though delegation token must have expired midway.
        //
        // To look for evidence of delegation token renewal check the logs for the following message:
        // - "Token is nearing expiry, so refreshing it"
        assertEquals(10, eventReadCount);
    }

    private void writeTenEvents(String scope, String streamName, ClientConfig clientConfig) {
        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // Perform writes on a separate thread.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        for (int i = 0; i < 10; i++) {
            String msg = "message: " + i;
            writer.writeEvent(msg);
            log.debug("Done writing message '{}' to stream '{} / {}'", msg, scope, streamName);
        }
        writer.flush();
    }

    private void createScopeStream(String scope, String streamName, int numSegments, ClientConfig clientConfig) {
        @Cleanup final StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);
        log.debug("Done creating stream manager.");

        boolean isScopeCreated = streamManager.createScope(scope);
        assertTrue("Failed to create scope", isScopeCreated);
        log.debug("Done creating stream manager.");

        boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                                                   .scalingPolicy(ScalingPolicy
                                                                                                           .fixed(numSegments))
                                                                                                   .build());
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);
    }
}
