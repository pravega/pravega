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
import io.pravega.client.ClientFactory;
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
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_BYTES;
import static java.util.concurrent.TimeUnit.SECONDS;


@Slf4j
public class MetricsTest {

    private static final String STREAM_NAME = "testMetricsStream" + new Random().nextInt(Integer.MAX_VALUE);
    private static final long TOTAL_NUM_EVENTS = 100;
    String scope = "testMultiReaderWriterScope";
    String readerGroupName = "testMultiReaderWriterReaderGroup";
    ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
    StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();
    String readerName = "reader" + new Random().nextInt(Integer.MAX_VALUE);
    Timer timer = new Timer();
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StatsProvider statsProvider;
    private AtomicLong eventData = new AtomicLong();
    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("statsLogger");
    final OpStatsLogger segmentReadBytes = statsLogger.createStats(SEGMENT_READ_BYTES);


    @Before
    public void setup() throws Exception {

        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();
        log.info("Starting Zk server");

        // 2. Start Pravega SegmentStore service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        this.server = new PravegaConnectionListener(false, servicePort, store);
        this.server.startListening();
        log.info("Starting Pravega SegmentStore server");

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
        log.info("Starting Pravega Controller server");

        // 4. Start Metrics service
        log.info("Initializing metrics provider ...");
        MetricsProvider.initialize(MetricsConfig.builder().with(MetricsConfig.DYNAMIC_CACHE_EVICTION_DURATION_MINUTES, 1).build());
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();
        log.info("Metrics Stats provider is started");
    }


    @After
    public void tearDown() throws Exception {

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
        if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
            log.info("Metrics statsProvider is now closed.");
        }
    }

    @Test
    public void metricsTimeBasedCacheEvictionTest() {

        try (StreamManager streamManager = new StreamManagerImpl(controller)) {

            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Create scope status {}", createScopeStatus);

            Boolean createStreamStatus = streamManager.createStream(scope, STREAM_NAME, config);
            log.info("Create stream status {}", createStreamStatus);
        }

        try (ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory)) {

            EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<String>(),
                    EventWriterConfig.builder().build());

            String event = "event";
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
            log.info("Closing writer {}", writer1);
            writer1.close();

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
                    segmentReadBytes.reportSuccessEvent(timer.getElapsed());
                } catch (ReinitializationRequiredException e) {
                    log.warn("Test Exception while reading from the stream", e);
                }

            }
            log.info("Closing reader {}", reader1);
            reader1.close();

            Exceptions.handleInterrupted(() -> Thread.sleep(1 * 60 * 1000));
            segmentReadBytes.reportSuccessEvent(timer.getElapsed());

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
                    segmentReadBytes.reportSuccessEvent(timer.getElapsed());
                } catch (ReinitializationRequiredException e) {
                    log.warn("Test Exception while reading from the stream", e);

                }
            }
            log.info("Closing reader {}", reader2);
            reader2.close();

        }
        log.info("Metrics Time based Cache Eviction test succeeds");
    }
}
