/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.host.stat.AutoScalerConfig;
import com.emc.pravega.service.server.host.stat.SegmentStatsFactory;
import com.emc.pravega.service.server.host.stat.SegmentStatsRecorder;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

@Slf4j
public class EndToEndAutoScaleDownTest {
    static final StreamConfiguration CONFIG =
            StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(
                    ScalingPolicy.byEventRate(10, 2, 1)).build();

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServer();

            ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), true);
            Controller controller = controllerWrapper.getController();

            controllerWrapper.getControllerService().createScope("pravega").get();
            ClientFactory internalCF = new ClientFactoryImpl("pravega", controller, new ConnectionFactoryImpl(false));

            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize().get();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

            SegmentStatsRecorder statsRecorder = new SegmentStatsFactory().createSegmentStatsRecorder(store,
                    internalCF,
                    AutoScalerConfig.builder().with(AutoScalerConfig.SCALE_MUTE_IN_SECONDS, 0)
                            .with(AutoScalerConfig.SCALE_COOLDOWN_IN_SECONDS, 0)
                            .with(AutoScalerConfig.SCALE_CACHE_CLEANUP_IN_SECONDS, 5)
                            .with(AutoScalerConfig.SCALE_CACHE_EXPIRY_IN_SECONDS, 30).build());

            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store, statsRecorder);
            server.startListening();
            controllerWrapper.getControllerService().createScope("test").get();

            controller.createStream(CONFIG).get();

            Stream stream = new StreamImpl("test", "test");
            Map<Double, Double> map = new HashMap<>();
            map.put(0.0, 0.33);
            map.put(0.33, 0.66);
            map.put(0.66, 1.0);
            controller.scaleStream(stream, Collections.singletonList(0), map).get();

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
