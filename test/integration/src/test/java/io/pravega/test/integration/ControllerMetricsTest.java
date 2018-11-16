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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
import static io.pravega.shared.MetricsNames.nameFromStream;
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

    @BeforeClass
    public static void initialize() {
        if (MetricRegistryUtils.getCounter(getCounterMetricName(CREATE_STREAM)) == null) {
            MetricsProvider.initialize(MetricsConfig.builder()
                                                    .with(MetricsConfig.ENABLE_STATISTICS, true)
                                                    .with(MetricsConfig.ENABLE_CSV_REPORTER, true)
                                                    .build());
            MetricsProvider.getMetricsProvider().start();
        }
    }

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
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
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @AfterClass
    public static void cleanUp() {
        MetricsProvider.getMetricsProvider().close();
    }

    @Test(timeout = 300000)
    public void streamMetricsTest() {
        final String scope = "controllerMetricsTestScope";
        final int parallelism = 4;
        int streamCount = 6;
        int iterations = 3;
        Counter createdStreamsCounter = MetricRegistryUtils.getCounter(getCounterMetricName(CREATE_STREAM));

        // At this point, we have 6 internal streams.
        Assert.assertEquals(streamCount, createdStreamsCounter.getCount());
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(parallelism))
                                                                     .build();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);
        streamCount++;

        for (int i = 0; i < iterations; i++) {
            final String streamName = "controllerMetricsTestStream" + i;
            final String readerGroupName = "RGControllerMetricsTestStream" + RandomFactory.getSeed();

            // Check that the number of streams in metrics has been incremented.
            streamManager.createStream(scope, streamName, streamConfiguration);
            Assert.assertEquals(streamCount + i, createdStreamsCounter.getCount());
            groupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                             .stream(scope + "/" + streamName)
                                                                             .build());
            // Account for the Reader Group stream created.
            streamCount++;

            for (long j = 1; j < iterations + 1; j++) {
                @Cleanup
                ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);
                // Update the Stream and check that the number of updated streams and per-stream updates is incremented.
                streamManager.updateStream(scope, streamName, streamConfiguration);
                Counter updatedStreamsCounter = MetricRegistryUtils.getCounter(getCounterMetricName(UPDATE_STREAM));
                Counter streamUpdatesCounter = MetricRegistryUtils.getCounter(getCounterMetricName(nameFromStream(UPDATE_STREAM, scope, streamName)));
                Assert.assertEquals(iterations * i + j, updatedStreamsCounter.getCount());
                Assert.assertEquals(j, streamUpdatesCounter.getCount());

                // Read and write some events.
                writeEvents(clientFactory, streamName, 10);
                readEvents(clientFactory, readerGroupName, 2);

                // Get a StreamCut for truncating the Stream.
                StreamCut streamCut = readerGroup.generateStreamCuts(executor).join().get(Stream.of(scope, streamName));

                // Truncate the Stream and check that the number of truncated Streams and per-Stream truncations is incremented.
                streamManager.truncateStream(scope, streamName, streamCut);
                Counter streamTruncationCounter = MetricRegistryUtils.getCounter(getCounterMetricName(UPDATE_STREAM));
                Counter perStreamTruncationCounter = MetricRegistryUtils.getCounter(getCounterMetricName(nameFromStream(UPDATE_STREAM, scope, streamName)));
                Assert.assertEquals(iterations * i + j, streamTruncationCounter.getCount());
                Assert.assertEquals(j, perStreamTruncationCounter.getCount());
            }

            // Check metrics accounting for sealed and deleted streams.
            streamManager.sealStream(scope, streamName);
            Counter streamSealCounter = MetricRegistryUtils.getCounter(getCounterMetricName(SEAL_STREAM));
            Assert.assertEquals(i + 1, streamSealCounter.getCount());
            streamManager.deleteStream(scope, streamName);
            Counter streamDeleteCounter = MetricRegistryUtils.getCounter(getCounterMetricName(DELETE_STREAM));
            Assert.assertEquals(i + 1, streamDeleteCounter.getCount());
        }

        checkStatsRegisteredValues(12, CREATE_STREAM_LATENCY);
        checkStatsRegisteredValues(iterations * iterations, UPDATE_STREAM_LATENCY, TRUNCATE_STREAM_LATENCY);
        checkStatsRegisteredValues(iterations, SEAL_STREAM_LATENCY, DELETE_STREAM_LATENCY);
    }

    private void checkStatsRegisteredValues(int expectedValues, String...metricNames) {
        for (String metricName: metricNames) {
            Timer latencyValues = MetricRegistryUtils.getTimer(getTimerMetricName(metricName));
            Assert.assertNotNull(latencyValues);
            Assert.assertEquals(expectedValues, latencyValues.getSnapshot().size());
        }
    }

    private static String getCounterMetricName(String metricName) {
        return "pravega." + metricName + ".Counter";
    }

    private static String getTimerMetricName(String metricName) {
        return "pravega.controller." + metricName;
    }
}
