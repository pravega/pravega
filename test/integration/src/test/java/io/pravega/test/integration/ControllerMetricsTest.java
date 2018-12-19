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
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION;

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
                                                   .with(MetricsConfig.ENABLE_CSV_REPORTER, false)
                                                   .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                                                   .build();
        metricsConfig.setDynamicCacheEvictionDurationMs(300000);

        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();
        log.info("Metrics Stats provider is started");

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
     * Verify that transaction metrics counters are being correctly reported.
     */
    @Test(timeout = 20000)
    public void transactionMetricsTest() throws TxnFailedException {
        final String scope = "transactionMetricsTestScope";
        final String streamName = "transactionMetricsTestStream";
        final int parallelism = 4;
        int iterations = 6;

        // At this point, we have at least 6 internal streams.
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(parallelism))
                                                                     .build();
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, streamConfiguration);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, ClientConfig.builder()
                                                                                 .controllerURI(controllerURI)
                                                                                 .build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        for (int i = 0; i < iterations; i++) {
            Transaction<String> transaction = writer.beginTxn();

            // Get the createTransactions metric.
            Counter createTransactions = getCounter(getCounterMetricName(CREATE_TRANSACTION + "." + scope + "." + streamName));
            Assert.assertNotNull(createTransactions);
            Assert.assertEquals(i + 1, createTransactions.getCount());

            // Write some data in the transaction.
            for (int j = 0; j < 100; j++) {
                transaction.writeEvent(String.valueOf(j));
            }

            if (i % 2 == 0) {
                transaction.commit();
                checkCommitOrAbortMetric(getCounter(getCounterMetricName(COMMIT_TRANSACTION + "." + scope + "." + streamName)), i / 2 + 1);
            } else {
                transaction.abort();
                checkCommitOrAbortMetric(getCounter(getCounterMetricName(ABORT_TRANSACTION + "." + scope + "." + streamName)), i / 2 + 1);
            }
        }
    }

    private Counter getCounter(String counterName) {
        Counter counter = null;
        // Access the cache until the metric is available.
        while (counter == null) {
            counter = MetricRegistryUtils.getCounter(counterName);
            Exceptions.handleInterrupted(() -> Thread.sleep(100));
        }

        return counter;
    }

    private void checkCommitOrAbortMetric(Counter metric, int expectedValue) {
        // Check that the metric is being correctly reported.
        boolean updatedCounter = false;
        do {
            try {
                Assert.assertNotNull(metric);
                Assert.assertEquals(expectedValue, metric.getCount());
                updatedCounter = true;
            } catch (AssertionError e) {
                log.info("Metric not updated in the cache. Retrying.", e);
                Exceptions.handleInterrupted(() -> Thread.sleep(100));
            }
        } while (!updatedCounter);
    }

    private static String getCounterMetricName(String metricName) {
        return "pravega." + metricName + ".Counter";
    }
}
