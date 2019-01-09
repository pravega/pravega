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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
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
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;


@Slf4j
public class MetricsTest {

    private static final String STREAM_NAME = "testMetricsStream" + new Random().nextInt(Integer.MAX_VALUE);
    private static final long TOTAL_NUM_EVENTS = 10;
    String scope = "testMetricsScope";
    String readerGroupName = "testMetricsReaderGroup";
    ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    String readerName = "reader" + new Random().nextInt(Integer.MAX_VALUE);
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StatsProvider statsProvider = null;

    @Before
    public void setup() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        this.server = new PravegaConnectionListener(false, servicePort, store);
        this.server.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();

        // 4. Start Metrics service
        log.info("Initializing metrics provider ...");

        MetricsConfig metricsConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_CSV_REPORTER, false).with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofSeconds(5));

        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();
        log.info("Metrics Stats provider is started");
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
        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }
    }

    @Test
    public void metricsTimeBasedCacheEvictionTest() throws InterruptedException, ExecutionException {
        try (ConnectionFactory cf = new ConnectionFactoryImpl(ClientConfig.builder().build());
             StreamManager streamManager = new StreamManagerImpl(controller, cf)) {
            boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Create scope status {}", createScopeStatus);

            boolean createStreamStatus = streamManager.createStream(scope, STREAM_NAME, config);
            log.info("Create stream status {}", createStreamStatus);
        }

        try (ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory)) {
            EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());
            String event = "12345";
            long bytesWritten = TOTAL_NUM_EVENTS * event.length() * 4;

            for (int i = 0; i < TOTAL_NUM_EVENTS; i++) {
                try {
                    log.info("Writing event {}", event);
                    writer1.writeEvent("", event);
                    writer1.flush();
                } catch (Throwable e) {
                    log.warn("Test exception writing events: {}", e);
                    break;
                }
            }

            String readerGroupName1 = readerGroupName + "1";
            log.info("Creating Reader group : {}", readerGroupName1);

            readerGroupManager.createReaderGroup(readerGroupName1, ReaderGroupConfig.builder().stream(Stream.of(scope, STREAM_NAME)).build());

            EventStreamReader<String> reader1 = clientFactory.createReader(readerName,
                    readerGroupName1,
                    new JavaSerializer<String>(),
                    ReaderConfig.builder().build());

            for (int j = 0; j < TOTAL_NUM_EVENTS; j++) {
                try {
                    String eventRead1 = reader1.readNextEvent(SECONDS.toMillis(2)).getEvent();
                    log.info("Reading event {}", eventRead1);
                } catch (ReinitializationRequiredException e) {
                    log.warn("Test Exception while reading from the stream", e);
                }
            }

            long initialCount = MetricRegistryUtils.getCounter("pravega.segmentstore.segment.read_bytes." + scope + "." + STREAM_NAME + ".0.#epoch.0.Counter").getCount();
            Assert.assertEquals(bytesWritten, initialCount);

            Exceptions.handleInterrupted(() -> Thread.sleep(10 * 1000));

            String readerGroupName2 = readerGroupName + "2";
            log.info("Creating Reader group : {}", readerGroupName2);

            readerGroupManager.createReaderGroup(readerGroupName2, ReaderGroupConfig.builder().stream(Stream.of(scope, STREAM_NAME)).build());

            EventStreamReader<String> reader2 = clientFactory.createReader(readerName,
                    readerGroupName2,
                    new JavaSerializer<String>(),
                    ReaderConfig.builder().build());

            for (int q = 0; q < TOTAL_NUM_EVENTS; q++) {
                try {
                    String eventRead2 = reader2.readNextEvent(SECONDS.toMillis(2)).getEvent();
                    log.info("Reading event {}", eventRead2);
                } catch (ReinitializationRequiredException e) {
                    log.warn("Test Exception while reading from the stream", e);
                }
            }

            long countAfterCacheEvicted = MetricRegistryUtils.getCounter("pravega.segmentstore.segment.read_bytes." + scope + "." + STREAM_NAME + ".0.#epoch.0.Counter").getCount();

            //Metric is evicted from Cache, after cache eviction duration
            //Count starts from 0, rather than adding up to previously ready bytes, as cache is evicted.
            Assert.assertEquals(bytesWritten, countAfterCacheEvicted);

            Map<Double, Double> map = new HashMap<>();
            map.put(0.0, 1.0);

            //Seal segment 0, create segment 1
            CompletableFuture<Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scope, STREAM_NAME),
                    Collections.singletonList(0L),
                    map,
                    executorService).getFuture();
            Assert.assertTrue(scaleStatus.get());

            EventStreamWriter<String> writer2 = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());

            for (int i = 0; i < TOTAL_NUM_EVENTS; i++) {
                try {
                    log.info("Writing event {}", event);
                    writer2.writeEvent("", event);
                    writer2.flush();
                } catch (Throwable e) {
                    log.warn("Test exception writing events: {}", e);
                    break;
                }
            }

            Exceptions.handleInterrupted(() -> Thread.sleep(10 * 1000));

            for (int j = 0; j < TOTAL_NUM_EVENTS; j++) {
                try {
                    String eventRead2 = reader1.readNextEvent(SECONDS.toMillis(2)).getEvent();
                    log.info("Reading event {}", eventRead2);
                } catch (ReinitializationRequiredException e) {
                    log.warn("Test Exception while reading from the stream", e);
                }
            }

            long countFromSecondSegment = MetricRegistryUtils.getCounter("pravega.segmentstore.segment.read_bytes." + scope + "." + STREAM_NAME + ".1.#epoch.1.Counter").getCount();

            Assert.assertEquals(bytesWritten, countFromSecondSegment);

            readerGroupManager.deleteReaderGroup(readerGroupName1);
            readerGroupManager.deleteReaderGroup(readerGroupName2);

            CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, STREAM_NAME);
            log.info("Sealing stream {}", STREAM_NAME);
            assertTrue(sealStreamStatus.get());

            CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, STREAM_NAME);
            log.info("Deleting stream {}", STREAM_NAME);
            assertTrue(deleteStreamStatus.get());

            CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
            log.info("Deleting scope {}", scope);
            assertTrue(deleteScopeStatus.get());
        }

        log.info("Metrics Time based Cache Eviction test succeeds");
    }
       
}
