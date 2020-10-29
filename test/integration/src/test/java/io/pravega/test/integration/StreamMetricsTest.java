/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.shared.MetricsTags.streamTags;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class StreamMetricsTest {

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StatsProvider statsProvider = null;
    private ServiceBuilder serviceBuilder = null;
    private AutoScaleMonitor monitor = null;
    private int controllerPort;
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Before
    public void setup() throws Exception {
        controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start Metrics service
        log.info("Initializing metrics provider ...");

        MetricsConfig metricsConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofSeconds(60));

        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();
        log.info("Metrics Stats provider is started");

        // 2. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 3. Start Pravega SegmentStore service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        monitor = new AutoScaleMonitor(store, AutoScalerConfig.builder().build());
        TableStore tableStore = serviceBuilder.createTableStoreService();

        this.server = new PravegaConnectionListener(false, false, "localhost", servicePort, store, tableStore,
                monitor.getStatsRecorder(), monitor.getTableSegmentStatsRecorder(), new PassingTokenVerifier(),
                null, null, true, this.serviceBuilder.getLowPriorityExecutor());
        this.server.startListening();

        // 4. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
    }

    @After
    public void tearDown() throws Exception {
        if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
            log.info("Metrics statsProvider is now closed.");
        }

        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }

        if (this.server != null) {
            this.server.close();
            this.server = null;
        }

        if (this.monitor != null) {
            this.monitor.close();
            this.monitor = null;
        }

        if (this.serviceBuilder != null) {
            this.serviceBuilder.close();
            this.serviceBuilder = null;
        }

        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void testStreamsAndScopesBasicMetricsTests() throws Exception {
        String scopeName = "scopeBasic";
        String streamName = "streamBasic";

        // Here, the system scope and streams are already created.
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_SCOPE).count());
        assertEquals(8, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_STREAM).count());

        controllerWrapper.getControllerService().createScope(scopeName).get();
        if (!controller.createStream(scopeName, streamName, config).get()) {
            log.error("Stream {} for basic testing already existed, exiting", scopeName + "/" + streamName);
            return;
        }
        // Check that the new scope and stream are accounted in metrics.
        assertEquals(2, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_SCOPE).count());
        assertEquals(9, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_STREAM).count());

        // Update the Stream.
        controllerWrapper.getControllerService().updateStream(scopeName, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(10)).build()).get();
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.globalMetricName(MetricsNames.UPDATE_STREAM)).count());

        // Seal the Stream.
        controllerWrapper.getControllerService().sealStream(scopeName, streamName).get();
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.SEAL_STREAM).count());

        controllerWrapper.getControllerService().addSubscriber(scopeName, streamName, "subscriber1").get();
        ImmutableMap<Long, Long> streamCut1 = ImmutableMap.of(0L, 10L);
        controllerWrapper.getControllerService().updateSubscriberStreamCut(scopeName, streamName, "subscriber1", streamCut1).get();
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.ADD_SUBSCRIBER).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.UPDATE_SUBSCRIBER).count());
        controllerWrapper.getControllerService().deleteSubscriber(scopeName, streamName, "subscriber1").get();
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.REMOVE_SUBSCRIBER).count());

        // Delete the Stream and Scope and check for the respective metrics.
        controllerWrapper.getControllerService().deleteStream(scopeName, streamName).get();
        controllerWrapper.getControllerService().deleteScope(scopeName).get();
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.DELETE_STREAM).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.DELETE_SCOPE).count());

        // Exercise the metrics for failed stream and scope creation/deletion.
        StreamMetrics.getInstance().createScopeFailed("failedScope");
        StreamMetrics.getInstance().createStreamFailed("failedScope", "failedStream");
        StreamMetrics.getInstance().deleteScopeFailed("failedScope");
        StreamMetrics.getInstance().deleteStreamFailed("failedScope", "failedStream");
        StreamMetrics.getInstance().updateStreamFailed("failedScope", "failedStream");
        StreamMetrics.getInstance().truncateStreamFailed("failedScope", "failedStream");
        StreamMetrics.getInstance().sealStreamFailed("failedScope", "failedStream");
        StreamMetrics.getInstance().addSubscriberFailed("failedScope", "failedStream");
        StreamMetrics.getInstance().deleteSubscriberFailed("failedScope", "failedStream");
        StreamMetrics.getInstance().updateTruncationSCFailed("failedScope", "failedStream");
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_SCOPE_FAILED).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_STREAM_FAILED).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.DELETE_STREAM_FAILED).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.DELETE_SCOPE_FAILED).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.UPDATE_STREAM_FAILED).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.TRUNCATE_STREAM_FAILED).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.SEAL_STREAM_FAILED).count());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.ADD_SUBSCRIBER_FAILED).count());
    }

    @Test(timeout = 30000)
    public void testSegmentSplitMerge() throws Exception {
        String scaleScopeName = "scaleScope";
        String scaleStreamName = "scaleStream";

        controllerWrapper.getControllerService().createScope(scaleScopeName).get();
        if (!controller.createStream(scaleScopeName, scaleStreamName, config).get()) {
            log.error("Stream {} for scale testing already existed, exiting", scaleScopeName + "/" + scaleStreamName);
            return;
        }
        Stream scaleStream = new StreamImpl(scaleScopeName, scaleStreamName);

        //split to 3 segments
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.33);
        keyRanges.put(0.33, 0.66);
        keyRanges.put(0.66, 1.0);

        if (!controller.scaleStream(scaleStream, Collections.singletonList(0L), keyRanges, executor).getFuture().get()) {
            log.error("Scale stream: splitting segment into three failed, exiting");
            return;
        }

        assertEquals(3, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(1, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(0, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleScopeName, scaleStreamName)).value());

        //merge back to 2 segments
        keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);

        if (!controller.scaleStream(scaleStream, Arrays.asList(1L, 2L, 3L), keyRanges, executor).getFuture().get()) {
            log.error("Scale stream: merging segments into two failed, exiting");
            return;
        }

        assertEquals(2, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(1, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleScopeName, scaleStreamName)).value());
        assertEquals(1, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleScopeName, scaleStreamName)).value());
    }

    @Test(timeout = 30000)
    public void testTransactionMetrics() throws Exception {
        String txScopeName = "scopeTx";
        String txStreamName = "streamTx";

        controllerWrapper.getControllerService().createScope(txScopeName).get();
        if (!controller.createStream(txScopeName, txStreamName, config).get()) {
            log.error("Stream {} for tx testing already existed, exiting", txScopeName + "/" + txStreamName);
            return;
        }

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(txScopeName, ClientConfig.builder()
                .controllerURI(URI.create("tcp://localhost:" + controllerPort)).build());
        @Cleanup
        TransactionalEventStreamWriter<String> writer = clientFactory.createTransactionalEventWriter(Stream.of(txScopeName, txStreamName).getStreamName(),
                new JavaSerializer<>(), EventWriterConfig.builder().build());

        Transaction<String> transaction = writer.beginTxn();

        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_TRANSACTION, streamTags(txScopeName, txStreamName)).count());

        transaction.writeEvent("Test");
        transaction.flush();
        transaction.commit();

        AssertExtensions.assertEventuallyEquals(true, () -> transaction.checkStatus().equals(Transaction.Status.COMMITTED), 10000);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getCounter(MetricsNames.COMMIT_TRANSACTION, streamTags(txScopeName, txStreamName)) != null, 10000);
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.COMMIT_TRANSACTION, streamTags(txScopeName, txStreamName)).count());

        Transaction<String> transaction2 = writer.beginTxn();
        transaction2.writeEvent("Test");
        transaction2.abort();

        AssertExtensions.assertEventuallyEquals(true, () -> transaction2.checkStatus().equals(Transaction.Status.ABORTED), 10000);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getCounter(MetricsNames.ABORT_TRANSACTION, streamTags(txScopeName, txStreamName)) != null, 10000);
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.ABORT_TRANSACTION, streamTags(txScopeName, txStreamName)).count());
    }

    @Test(timeout = 30000)
    public void testRollingTxnMetrics() throws Exception {
        String scaleRollingTxnScopeName = "scaleRollingTxnScope";
        String scaleRollingTxnStreamName = "scaleRollingTxnStream";

        controllerWrapper.getControllerService().createScope(scaleRollingTxnScopeName).get();
        if (!controller.createStream(scaleRollingTxnScopeName, scaleRollingTxnStreamName, config).get()) {
            fail("Stream " + scaleRollingTxnScopeName + "/" + scaleRollingTxnStreamName + " for scale testing already existed, test failed");
        }

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scaleRollingTxnScopeName, ClientConfig.builder()
                .controllerURI(URI.create("tcp://localhost:" + controllerPort)).build());
        @Cleanup
        TransactionalEventStreamWriter<String> writer = clientFactory.createTransactionalEventWriter(Stream.of(scaleRollingTxnScopeName, scaleRollingTxnStreamName).getStreamName(),
                new JavaSerializer<>(), EventWriterConfig.builder().build());
        Transaction<String> transaction = writer.beginTxn();
        transaction.writeEvent("Transactional content");

        //split to 3 segments
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.25);
        keyRanges.put(0.25, 0.75);
        keyRanges.put(0.75, 1.0);

        Stream scaleRollingTxnStream = new StreamImpl(scaleRollingTxnScopeName, scaleRollingTxnStreamName);
        if (!controller.scaleStream(scaleRollingTxnStream, Collections.singletonList(0L), keyRanges, executor).getFuture().get()) {
            fail("Scale stream: splitting segment into three failed, exiting");
        }

        assertEquals(3, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value());
        assertEquals(1, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value());
        assertEquals(0, (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value());

        transaction.flush();
        transaction.commit();

        String message = "Inconsistency found between metadata and metrics";
        AssertExtensions.assertEventuallyEquals(message, 3L, () -> (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_COUNT, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value(), 500, 30000);
        AssertExtensions.assertEventuallyEquals(message, 2L, () -> (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_SPLITS, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value(), 200, 30000);
        AssertExtensions.assertEventuallyEquals(message, 1L, () -> (long) MetricRegistryUtils.getGauge(MetricsNames.SEGMENTS_MERGES, streamTags(scaleRollingTxnScopeName, scaleRollingTxnStreamName)).value(), 200, 30000);
    }
}
