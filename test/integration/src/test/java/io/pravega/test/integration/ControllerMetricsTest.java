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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.net.URI;
import java.time.Duration;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.shared.MetricsNames.CONTROLLER_ZK_SESSION_EXPIRATION;
import static io.pravega.shared.MetricsNames.CREATE_STREAM;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_STREAM;
import static io.pravega.shared.MetricsNames.DELETE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.SEAL_STREAM;
import static io.pravega.shared.MetricsNames.SEAL_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.streamTags;
import static io.pravega.test.integration.ReadWriteUtils.readEvents;
import static io.pravega.test.integration.ReadWriteUtils.writeEvents;

/**
 * Check the end to end correctness of metrics published by the Controller.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class ControllerMetricsTest extends ThreadPooledTestSuite {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private StatsProvider statsProvider = null;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        MetricsConfig metricsConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofMinutes(5));

        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();
        log.info("Metrics Stats provider is started");

        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                                                  false,
                                                  false,
                                                  controllerPort,
                                                  serviceHost,
                                                  servicePort,
                                                  containerCount,
                                                  -1);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
            log.info("Metrics statsProvider is now closed.");
        }

        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 3000)
    public void testSetup() throws Exception {
        //This is to verify the setUp / tearDown methods
        //That needs to be independently validated because previous versions of this test
        //had errors in the setup which were dependent on the state of the metrics
        //So streamMetricsTest would pass or fail depending on what ran before it. 
    }
    
    /**
     * This test verifies that the appropriate metrics for Stream operations are updated correctly (counters, latency
     * histograms). Note that this test performs "at least" assertions on metrics as in an environment with concurrent
     * tests running, it might be possible that metrics get updated by other tests.
     */
    @Test(timeout = 300000)
    public void streamMetricsTest() throws Exception {
        //make unique scope to improve the test isolation.
        final String scope = "controllerMetricsTestScope" + RandomFactory.getSeed();
        final String streamName = "controllerMetricsTestStream";
        final String readerGroupName = "RGControllerMetricsTestStream";
        final int parallelism = 4;
        final int eventsWritten = 10;
        int iterations = 3;

        // At this point, we have at least 6 internal streams.
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(parallelism)).build();
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory
                .withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);

        for (int i = 0; i < iterations; i++) {
            final String iterationStreamName = streamName + i;
            final String iterationReaderGroupName = readerGroupName + RandomFactory.getSeed();

            // Check that the number of streams in metrics has been incremented.
            streamManager.createStream(scope, iterationStreamName, streamConfiguration);
            Counter createdStreamsCounter = MetricRegistryUtils.getCounter(CREATE_STREAM);
            AssertExtensions.assertGreaterThanOrEqual("The counter of created streams",
                    i, (long) createdStreamsCounter.count());
            groupManager.createReaderGroup(iterationReaderGroupName, ReaderGroupConfig.builder()
                    .disableAutomaticCheckpoints().stream(scope + "/" + iterationStreamName).build());

            for (long j = 1; j < iterations + 1; j++) {
                @Cleanup
                ReaderGroup readerGroup = groupManager.getReaderGroup(iterationReaderGroupName);
                // Update the Stream and check that the number of updated streams and per-stream updates is incremented.
                streamManager.updateStream(scope, iterationStreamName, streamConfiguration);
                Counter updatedStreamsCounter = MetricRegistryUtils.getCounter(globalMetricName(UPDATE_STREAM));
                Counter streamUpdatesCounter = MetricRegistryUtils.getCounter(UPDATE_STREAM, streamTags(scope, iterationStreamName));
                Assert.assertTrue(iterations * i + j <= updatedStreamsCounter.count());
                Assert.assertTrue(j == streamUpdatesCounter.count());

                // Read and write some events.
                writeEvents(clientFactory, iterationStreamName, eventsWritten);
                Futures.allOf(readEvents(clientFactory, iterationReaderGroupName, parallelism));

                // Get a StreamCut for truncating the Stream.
                StreamCut streamCut = readerGroup.generateStreamCuts(executorService()).join().get(Stream.of(scope, iterationStreamName));

                // Truncate the Stream and check that the number of truncated Streams and per-Stream truncations is incremented.
                streamManager.truncateStream(scope, iterationStreamName, streamCut);
                Counter streamTruncationCounter = MetricRegistryUtils.getCounter(globalMetricName(TRUNCATE_STREAM));
                Counter perStreamTruncationCounter = MetricRegistryUtils.getCounter(
                        TRUNCATE_STREAM, streamTags(scope, iterationStreamName));
                Assert.assertTrue(iterations * i + j <= streamTruncationCounter.count());
                Assert.assertTrue(j == perStreamTruncationCounter.count());
            }

            // Check metrics accounting for sealed and deleted streams.
            streamManager.sealStream(scope, iterationStreamName);
            Counter streamSealCounter = MetricRegistryUtils.getCounter(SEAL_STREAM);
            Assert.assertTrue(i + 1 <= streamSealCounter.count());
            streamManager.deleteStream(scope, iterationStreamName);
            Counter streamDeleteCounter = MetricRegistryUtils.getCounter(DELETE_STREAM);
            Assert.assertTrue(i + 1 <= streamDeleteCounter.count());
        }

        //Put assertion on different lines so it can tell more information in case of failure.
        Timer latencyValues1 = MetricRegistryUtils.getTimer(CREATE_STREAM_LATENCY);
        Assert.assertNotNull(latencyValues1);
        AssertExtensions.assertGreaterThanOrEqual("Number of iterations and latency count do not match.",
                iterations, latencyValues1.count());

        Timer latencyValues2 = MetricRegistryUtils.getTimer(SEAL_STREAM_LATENCY);
        Assert.assertNotNull(latencyValues2);
        Assert.assertEquals(iterations, latencyValues2.count());

        Timer latencyValues3 = MetricRegistryUtils.getTimer(DELETE_STREAM_LATENCY);
        Assert.assertNotNull(latencyValues3);
        Assert.assertEquals(iterations, latencyValues3.count());

        Timer latencyValues4 = MetricRegistryUtils.getTimer(UPDATE_STREAM_LATENCY);
        Assert.assertNotNull(latencyValues4);
        Assert.assertEquals(iterations * iterations, latencyValues4.count());

        Timer latencyValues5 = MetricRegistryUtils.getTimer(TRUNCATE_STREAM_LATENCY);
        Assert.assertNotNull(latencyValues5);
        Assert.assertEquals(iterations * iterations, latencyValues5.count());
    }

    /**
     * This test verifies that the Controller increments the metric for Zookeeper session expiration events correctly.
     *
     * @throws Exception
     */
    @Test(timeout = 25000)
    public void zookeeperMetricsTest() throws Exception {
        Counter zkSessionExpirationCounter = MetricRegistryUtils.getCounter(CONTROLLER_ZK_SESSION_EXPIRATION);
        double previousCount = zkSessionExpirationCounter == null ? 0.0 : zkSessionExpirationCounter.count();
        controllerWrapper.forceClientSessionExpiry();
        AssertExtensions.assertEventuallyEquals(previousCount + 1.0, () -> {
            Counter counter = MetricRegistryUtils.getCounter(CONTROLLER_ZK_SESSION_EXPIRATION);
            return counter == null ? 0 : counter.count();
        }, 25000L);
    }
}
