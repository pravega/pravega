/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.common.util.Retry;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.host.stat.SegmentStatsFactory;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.TestingServerStarter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class EndToEndAutoScaleUpWithTxnTest {
    static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                 .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                                 .build();

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServerStarter().start();
            int port = Config.SERVICE_PORT;
            @Cleanup
            ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
            Controller controller = controllerWrapper.getController();
            controllerWrapper.getControllerService().createScope(NameUtils.INTERNAL_SCOPE_NAME).get();

            @Cleanup
            ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
            @Cleanup
            ClientFactory internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, connectionFactory);

            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            @Cleanup
            SegmentStatsFactory segmentStatsFactory = new SegmentStatsFactory();
            SegmentStatsRecorder statsRecorder = segmentStatsFactory.createSegmentStatsRecorder(store,
                    internalCF,
                    AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                            .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0).build());

            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, "localhost", 12345, store,
                    statsRecorder, null, null, null, true);
            server.startListening();

            controllerWrapper.awaitRunning();
            controllerWrapper.getControllerService().createScope("test").get();

            controller.createStream("test", "test", CONFIG).get();
            @Cleanup
            MockClientFactory clientFactory = new MockClientFactory("test", controller);

            // Mocking pravega service by putting scale up and scale down requests for the stream
            EventWriterConfig writerConfig = EventWriterConfig.builder()
                                                              .transactionTimeoutTime(30000)
                                                              .build();
            EventStreamWriter<String> test = clientFactory.createEventWriter("test", new JavaSerializer<>(), writerConfig);

            // region Successful commit tests
            Transaction<String> txn1 = test.beginTxn();

            txn1.writeEvent("1");
            txn1.flush();

            Map<Double, Double> map = new HashMap<>();
            map.put(0.0, 1.0 / 3.0);
            map.put(1.0 / 3.0, 2.0 / 3.0);
            map.put(2.0 / 3.0, 1.0);
            Stream stream = new StreamImpl("test", "test");
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            controller.startScale(stream, Collections.singletonList(0L), map).get();
            Transaction<String> txn2 = test.beginTxn();

            txn2.writeEvent("2");
            txn2.flush();
            txn2.commit();
            txn1.commit();

            Thread.sleep(1000);

            @Cleanup
            ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl("test", controller, clientFactory, connectionFactory);
            readerGroupManager.createReaderGroup("readergrp", ReaderGroupConfig.builder().stream("test/test").build());

            final EventStreamReader<String> reader = clientFactory.createReader("1",
                    "readergrp",
                    new JavaSerializer<>(),
                    ReaderConfig.builder().build());

            String event1 = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
            String event2 = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
            assert event1.equals("1");
            assert event2.equals("2");
            final AtomicBoolean done = new AtomicBoolean(false);

            startWriter(test, done);

            Retry.withExpBackoff(10, 10, 100, 10000)
                    .retryingOn(NotDoneException.class)
                    .throwingOn(RuntimeException.class)
                    .runAsync(() -> controller.getCurrentSegments("test", "test")
                            .thenAccept(streamSegments -> {
                                if (streamSegments.getSegments().stream().anyMatch(x -> StreamSegmentNameUtils.getEpoch(x.getSegmentId()) > 5)) {
                                    System.err.println("Success");
                                    log.info("Success");
                                    System.exit(0);
                                } else {
                                    throw new NotDoneException();
                                }
                            }), Executors.newSingleThreadScheduledExecutor())
                    .exceptionally(e -> {
                        System.err.println("Failure");
                        log.error("Failure");
                        System.exit(1);
                        return null;
                    }).get();
        } catch (Throwable e) {
            System.err.print("Test failed with exception: " + e.getMessage());
            log.error("Test failed with exception: {}", e);
            System.exit(-1);
        }

        System.exit(0);
    }

    private static void startWriter(EventStreamWriter<String> test, AtomicBoolean done) {
        CompletableFuture.runAsync(() -> {
            while (!done.get()) {
                try {
                    Transaction<String> transaction = test.beginTxn();

                    for (int i = 0; i < 1000; i++) {
                        transaction.writeEvent("0", "txntest");
                    }
                    Thread.sleep(900);
                    transaction.commit();
                } catch (Throwable e) {
                    System.err.println("test exception writing events " + e.getMessage());
                    log.error("test exception writing events {}", e);
                }
            }
        });
    }
}
