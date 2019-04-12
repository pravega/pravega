/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.MetricsNames.CONTROLLER_ZK_SESSION_EXPIRATION;
import static io.pravega.shared.MetricsNames.CREATE_STREAM;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_STREAM;
import static io.pravega.shared.MetricsNames.DELETE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.INITIAL_SEGMENTS_COUNT;
import static io.pravega.shared.MetricsNames.SEAL_STREAM;
import static io.pravega.shared.MetricsNames.SEAL_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM;
import static io.pravega.shared.MetricsNames.SEGMENTS_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENTS_MERGES;
import static io.pravega.shared.MetricsNames.SEGMENTS_SPLITS;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.streamTags;
import static io.pravega.test.common.AssertExtensions.assertEventuallyNearlyEquals;
import static io.pravega.test.integration.ReadWriteUtils.readEvents;
import static io.pravega.test.integration.ReadWriteUtils.writeEvents;

/**
 * Check the end to end correctness of metrics published by the Controller.
 */
@Slf4j
public class ControllerMetricsTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;
    private StatsProvider statsProvider = null;

    @Before
    public void setUp() throws Exception {
        MetricsConfig metricsConfig = MetricsConfig.builder()
                                                   .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                                                   .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofMinutes(5));

        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();
        log.info("Metrics Stats provider is started");

        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount,
                9091);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
            log.info("Metrics statsProvider is now closed.");
        }

        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
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
            Counter createdStreamsCounter = MetricRegistryUtils.getCounter(getPrefixedMetricName(CREATE_STREAM));
            AssertExtensions.assertGreaterThanOrEqual("The counter of created streams",
                    i, (long) createdStreamsCounter.count());
            groupManager.createReaderGroup(iterationReaderGroupName, ReaderGroupConfig.builder()
                    .disableAutomaticCheckpoints().stream(scope + "/" + iterationStreamName).build());

            for (long j = 1; j < iterations + 1; j++) {
                @Cleanup
                ReaderGroup readerGroup = groupManager.getReaderGroup(iterationReaderGroupName);
                // Update the Stream and check that the number of updated streams and per-stream updates is incremented.
                streamManager.updateStream(scope, iterationStreamName, streamConfiguration);
                Counter updatedStreamsCounter = MetricRegistryUtils.getCounter(getPrefixedMetricName(globalMetricName(UPDATE_STREAM)));
                Counter streamUpdatesCounter = MetricRegistryUtils.getCounter(
                        getPrefixedMetricName(UPDATE_STREAM), streamTags(scope, iterationStreamName));
                Assert.assertTrue(iterations * i + j <= updatedStreamsCounter.count());
                Assert.assertEquals(j, streamUpdatesCounter.count(), 0.);

                // Read and write some events.
                writeEvents(clientFactory, iterationStreamName, eventsWritten);
                Futures.allOf(readEvents(clientFactory, iterationReaderGroupName, parallelism));

                // Get a StreamCut for truncating the Stream.
                StreamCut streamCut = readerGroup.generateStreamCuts(executor).join().get(Stream.of(scope, iterationStreamName));

                // Truncate the Stream and check that the number of truncated Streams and per-Stream truncations is incremented.
                streamManager.truncateStream(scope, iterationStreamName, streamCut);
                Counter streamTruncationCounter = MetricRegistryUtils.getCounter(getPrefixedMetricName(globalMetricName(TRUNCATE_STREAM)));
                Counter perStreamTruncationCounter = MetricRegistryUtils.getCounter(
                        getPrefixedMetricName(TRUNCATE_STREAM), streamTags(scope, iterationStreamName));
                Assert.assertTrue(iterations * i + j <= streamTruncationCounter.count());
                Assert.assertEquals(j, perStreamTruncationCounter.count(), 0.1);
            }

            // Check metrics accounting for sealed and deleted streams.
            streamManager.sealStream(scope, iterationStreamName);
            Counter streamSealCounter = MetricRegistryUtils.getCounter(getPrefixedMetricName(SEAL_STREAM));
            Assert.assertTrue(i + 1 <= streamSealCounter.count());
            streamManager.deleteStream(scope, iterationStreamName);
            Counter streamDeleteCounter = MetricRegistryUtils.getCounter(getPrefixedMetricName(DELETE_STREAM));
            Assert.assertTrue(i + 1 <= streamDeleteCounter.count());
        }

        //Put assertion on different lines so it can tell more information in case of failure.
        Timer latencyValues1 = MetricRegistryUtils.getTimer(getControllerPrefixedMetricName(CREATE_STREAM_LATENCY));
        Assert.assertNotNull(latencyValues1);
        Assert.assertTrue(iterations <= latencyValues1.count());  //also system streams created so count() is bigger

        Timer latencyValues2 = MetricRegistryUtils.getTimer(getControllerPrefixedMetricName(SEAL_STREAM_LATENCY));
        Assert.assertNotNull(latencyValues2);
        Assert.assertEquals(iterations, latencyValues2.count());

        Timer latencyValues3 = MetricRegistryUtils.getTimer(getControllerPrefixedMetricName(DELETE_STREAM_LATENCY));
        Assert.assertNotNull(latencyValues3);
        Assert.assertEquals(iterations, latencyValues3.count());

        Timer latencyValues4 = MetricRegistryUtils.getTimer(getControllerPrefixedMetricName(UPDATE_STREAM_LATENCY));
        Assert.assertNotNull(latencyValues4);
        Assert.assertEquals(iterations * iterations, latencyValues4.count());

        Timer latencyValues5 = MetricRegistryUtils.getTimer(getControllerPrefixedMetricName(TRUNCATE_STREAM_LATENCY));
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
        Counter zkSessionExpirationCounter = MetricRegistryUtils.getCounter(getPrefixedMetricName(CONTROLLER_ZK_SESSION_EXPIRATION));
        Assert.assertNull(zkSessionExpirationCounter);
        controllerWrapper.forceClientSessionExpiry();
        while (zkSessionExpirationCounter == null) {
            Thread.sleep(100);
            zkSessionExpirationCounter = MetricRegistryUtils.getCounter(getPrefixedMetricName(CONTROLLER_ZK_SESSION_EXPIRATION));
        }
        Assert.assertEquals(zkSessionExpirationCounter.count(), 1, 0.1);
    }

    /**
     * This test verifies the correct reporting of segment splits, merges and count metrics. Concretely, this test
     * checks that segment splits and merges are not initialized upon a createStream operation. Moreover, we also make
     * sure to report the initial number of Stream segment in "segment count" metric. After reporting this value once,
     * we need to remove this metric from the local cache of the Controller handling the createStream operation to avoid
     * conflicts with another Controller that may handle a scaleStream request. Finally, this test checks that upon a
     * scaleStream request, the segment splits, merges and count metrics are correctly reported.
     *
     * @throws InterruptedException
     */
    @Test(timeout = 20000)
    public void mergesSplitsAndSegmentCountMetricsTest() throws Exception {
        final String scope = "mergesSplitsAndSegmentCountMetricsTestScope";
        final String streamName = "mergesSplitsAndSegmentCountMetricsTestStream";
        final int minNumSegments = 1;

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.byEventRate(10, 2, minNumSegments))
                                                                     .build();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, streamConfiguration);

        // Segment count, splits and merges should remain not initialized.
        Assert.assertNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(globalMetricName(SEGMENTS_SPLITS))));
        Assert.assertNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_SPLITS), streamTags(scope, streamName)));
        Assert.assertNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(globalMetricName(SEGMENTS_MERGES))));
        Assert.assertNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_MERGES), streamTags(scope, streamName)));
        Assert.assertNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_COUNT), streamTags(scope, streamName)));

        // Check that the initial number of segments for this Stream is reported correctly.
        final int timeout = 5000;
        Assert.assertNotNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(INITIAL_SEGMENTS_COUNT), streamTags(scope, streamName)));
        assertEventuallyNearlyEquals((double) minNumSegments, () ->
                MetricRegistryUtils.getGauge(getPrefixedMetricName(INITIAL_SEGMENTS_COUNT), streamTags(scope, streamName)).value(), timeout);

        // Scale the Stream and check that all these metrics have been set up again.
        performScaleStream(scope, streamName);

        // Check that all the metrics are being reported, now originated from the scale request.
        Assert.assertNotNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(globalMetricName(SEGMENTS_SPLITS))));
        assertEventuallyNearlyEquals(1.0, () ->
                MetricRegistryUtils.getGauge(getPrefixedMetricName(globalMetricName(SEGMENTS_SPLITS))).value(), timeout);
        Assert.assertNotNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_SPLITS), streamTags(scope, streamName)));
        assertEventuallyNearlyEquals(1.0, () ->
                MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_SPLITS), streamTags(scope, streamName)).value(), timeout);
        Assert.assertNotNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(globalMetricName(SEGMENTS_MERGES))));
        assertEventuallyNearlyEquals(0.0, () ->
                MetricRegistryUtils.getGauge(getPrefixedMetricName(globalMetricName(SEGMENTS_MERGES))).value(), timeout);
        Assert.assertNotNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_MERGES), streamTags(scope, streamName)));
        assertEventuallyNearlyEquals(0.0, () ->
                MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_MERGES), streamTags(scope, streamName)).value(), timeout);
        Assert.assertNotNull(MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_COUNT), streamTags(scope, streamName)));
        assertEventuallyNearlyEquals(3.0, () ->
                MetricRegistryUtils.getGauge(getPrefixedMetricName(SEGMENTS_COUNT), streamTags(scope, streamName)).value(), timeout);
    }

    private void performScaleStream(String scope, String streamName) throws InterruptedException {
        @Cleanup
        Controller controller = controllerWrapper.getController();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://localhost"))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "event1").join();
        Stream stream = new StreamImpl(scope, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().join();
        Assert.assertTrue(result);
        writer.writeEvent("0", "event2").join();
    }

    private static String getPrefixedMetricName(String metricName) {
        return "pravega." + metricName;
    }

    private static String getControllerPrefixedMetricName(String metricName) {
        return "pravega.controller." + metricName;
    }
}
