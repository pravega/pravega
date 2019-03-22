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
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.MetricsNames.CREATE_STREAM;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_STREAM;
import static io.pravega.shared.MetricsNames.DELETE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.SEAL_STREAM;
import static io.pravega.shared.MetricsNames.SEAL_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsNames.nameFromStream;
import static io.pravega.test.integration.ReadWriteUtils.readEvents;
import static io.pravega.test.integration.ReadWriteUtils.writeEvents;
import static org.mockito.Mockito.mock;

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

        server = new PravegaConnectionListener(false, servicePort, store, mock(TableStore.class));
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
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
    public void streamMetricsTest() {
        final String scope = "controllerMetricsTestScope";
        final String streamName = "controllerMetricsTestStream";
        final String readerGroupName = "RGControllerMetricsTestStream";
        final int parallelism = 4;
        final int eventsWritten = 10;
        int iterations = 3;

        // At this point, we have at least 6 internal streams.
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(parallelism))
                                                                     .build();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, ClientConfig.builder()
                                                                                                       .controllerURI(controllerURI)
                                                                                                       .build());
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);

        for (int i = 0; i < iterations; i++) {
            final String iterationStreamName = streamName + i;
            final String iterationReaderGroupName = readerGroupName + RandomFactory.getSeed();

            // Check that the number of streams in metrics has been incremented.
            streamManager.createStream(scope, iterationStreamName, streamConfiguration);
            Counter createdStreamsCounter = MetricRegistryUtils.getCounter(getCounterMetricName(CREATE_STREAM));
            AssertExtensions.assertGreaterThanOrEqual("The counter of created streams", i, (long) createdStreamsCounter.count());
            groupManager.createReaderGroup(iterationReaderGroupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                                                .stream(scope + "/" + iterationStreamName)
                                                                                                .build());

            for (long j = 1; j < iterations + 1; j++) {
                @Cleanup
                ReaderGroup readerGroup = groupManager.getReaderGroup(iterationReaderGroupName);
                // Update the Stream and check that the number of updated streams and per-stream updates is incremented.
                streamManager.updateStream(scope, iterationStreamName, streamConfiguration);
                Counter updatedStreamsCounter = MetricRegistryUtils.getCounter(getCounterMetricName(globalMetricName(UPDATE_STREAM)));
                Counter streamUpdatesCounter = MetricRegistryUtils.getCounter(
                        getCounterMetricName(nameFromStream(UPDATE_STREAM, scope, iterationStreamName)));
                Assert.assertTrue(iterations * i + j <= updatedStreamsCounter.count());
                Assert.assertTrue(j <= streamUpdatesCounter.count());

                // Read and write some events.
                writeEvents(clientFactory, iterationStreamName, eventsWritten);
                Futures.allOf(readEvents(clientFactory, iterationReaderGroupName, parallelism));

                // Get a StreamCut for truncating the Stream.
                StreamCut streamCut = readerGroup.generateStreamCuts(executor).join().get(Stream.of(scope, iterationStreamName));

                // Truncate the Stream and check that the number of truncated Streams and per-Stream truncations is incremented.
                streamManager.truncateStream(scope, iterationStreamName, streamCut);
                Counter streamTruncationCounter = MetricRegistryUtils.getCounter(getCounterMetricName(globalMetricName(UPDATE_STREAM)));
                Counter perStreamTruncationCounter = MetricRegistryUtils.getCounter(
                        getCounterMetricName(nameFromStream(UPDATE_STREAM, scope, iterationStreamName)));
                Assert.assertTrue(iterations * i + j <= streamTruncationCounter.count());
                Assert.assertTrue(j <= perStreamTruncationCounter.count());
            }

            // Check metrics accounting for sealed and deleted streams.
            streamManager.sealStream(scope, iterationStreamName);
            Counter streamSealCounter = MetricRegistryUtils.getCounter(getCounterMetricName(SEAL_STREAM));
            Assert.assertTrue(i + 1 <= streamSealCounter.count());
            streamManager.deleteStream(scope, iterationStreamName);
            Counter streamDeleteCounter = MetricRegistryUtils.getCounter(getCounterMetricName(DELETE_STREAM));
            Assert.assertTrue(i + 1 <= streamDeleteCounter.count());
        }

        checkStatsRegisteredValues(iterations, CREATE_STREAM_LATENCY, SEAL_STREAM_LATENCY, DELETE_STREAM_LATENCY);
        checkStatsRegisteredValues(iterations * iterations, UPDATE_STREAM_LATENCY, TRUNCATE_STREAM_LATENCY);
    }

    private void checkStatsRegisteredValues(int minExpectedValues, String...metricNames) {
        for (String metricName: metricNames) {
            Timer latencyValues = MetricRegistryUtils.getTimer(getTimerMetricName(metricName));
            Assert.assertNotNull(latencyValues);
            Assert.assertTrue(minExpectedValues <= latencyValues.takeSnapshot().count());
        }
    }

    private static String getCounterMetricName(String metricName) {
        return "pravega." + metricName + ".Counter";
    }

    private static String getTimerMetricName(String metricName) {
        return "pravega.controller." + metricName;
    }
}
