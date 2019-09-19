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

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
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
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.shared.MetricsTags.containerTag;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Slf4j
public class StreamSegmentContainerMetricsTest {

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StatsProvider statsProvider = null;
    private ServiceBuilder serviceBuilder = null;
    private AutoScaleMonitor monitor = null;

    @Before
    public void setup() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start Metrics service
        log.info("Initializing metrics provider ...");

        MetricsConfig metricsConfig = MetricsConfig.builder()
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
                null, null, true);
        this.server.startListening();

        // 4. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testStreamSegmentContainerMetrics() throws Exception {

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        String containerMetricsTestScopeName = "containerMetricsTestScopeName";
        String containerMetricsTestStreamName = "containerMetricsTestStreamName";

        controllerWrapper.getControllerService().createScope(containerMetricsTestScopeName).get();
        if (!controller.createStream(containerMetricsTestScopeName, containerMetricsTestStreamName, config).get()) {
            log.error("Stream {} for scale testing already existed, exiting", containerMetricsTestScopeName + "/" + containerMetricsTestStreamName);
            return;
        }
        Stream scaleStream = new StreamImpl(containerMetricsTestScopeName, containerMetricsTestStreamName);

        //split segments
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.33);
        keyRanges.put(0.33, 0.66);
        keyRanges.put(0.66, 1.0);
        if (!controller.scaleStream(scaleStream, Collections.singletonList(0L), keyRanges, executor).getFuture().get()) {
            log.error("Splitting segments failed, abort testing");
            return;
        }

        //merge segments
        keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);
        if (!controller.scaleStream(scaleStream, Arrays.asList(1L, 2L, 3L), keyRanges, executor).getFuture().get()) {
            log.error("Merging segments failed, abort testing");
            return;
        }

        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag(0)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag(0)));
        assertNotNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag(0)));

        shutdown();

        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag(0)));
        assertNull(MetricRegistryUtils.getMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag(0)));
    }

    public void shutdown() throws Exception {
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

    }
}
