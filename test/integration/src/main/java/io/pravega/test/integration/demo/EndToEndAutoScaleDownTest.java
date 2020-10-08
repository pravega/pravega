/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.util.Retry;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestingServerStarter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

@Slf4j
public class EndToEndAutoScaleDownTest {
    static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                 .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                                 .build();

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServerStarter().start();

            int port = Config.SERVICE_PORT;
            @Cleanup
            ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port, false);
            Controller controller = controllerWrapper.getController();

            controllerWrapper.getControllerService().createScope(NameUtils.INTERNAL_SCOPE_NAME).get();
            ClientFactoryImpl internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, new ConnectionFactoryImpl(ClientConfig.builder().build()));

            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            TableStore tableStore = serviceBuilder.createTableStoreService();
            @Cleanup
            AutoScaleMonitor autoScaleMonitor = new AutoScaleMonitor(store,
                    internalCF,
                    AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                                    .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                                    .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 5)
                                    .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 30).build());

            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, false, "localhost", 12345, store, tableStore,
                    autoScaleMonitor.getStatsRecorder(), autoScaleMonitor.getTableSegmentStatsRecorder(), null, null, null, true,
                    serviceBuilder.getLowPriorityExecutor());
            server.startListening();
            controllerWrapper.awaitRunning();
            controllerWrapper.getControllerService().createScope("test").get();

            controller.createStream("test", "test", CONFIG).get();

            Stream stream = new StreamImpl("test", "test");
            Map<Double, Double> map = new HashMap<>();
            map.put(0.0, 0.33);
            map.put(0.33, 0.66);
            map.put(0.66, 1.0);
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();

            Retry.withExpBackoff(10, 10, 100, 10000)
                    .retryingOn(NotDoneException.class)
                    .throwingOn(RuntimeException.class)
                    .runAsync(() -> controller.getCurrentSegments("test", "test")
                            .thenAccept(streamSegments -> {
                                if (streamSegments.getSegments().size() < 3) {
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
            System.exit(-1);
        }

        System.exit(0);
    }
}
