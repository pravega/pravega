/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.host.stat.AutoScalerConfig;
import com.emc.pravega.service.server.host.stat.SegmentStatsFactory;
import com.emc.pravega.service.server.host.stat.SegmentStatsRecorder;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.shared.NameUtils;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.testcommon.ZKCuratorUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class EndToEndAutoScaleUpWithTxnTest {
    static final StreamConfiguration CONFIG =
            StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(
                    ScalingPolicy.byEventRate(10, 2, 3)).build();

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = ZKCuratorUtils.createTestServer();
            int port = Config.SERVICE_PORT;
            @Cleanup
            ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
            controllerWrapper.awaitRunning();
            Controller controller = controllerWrapper.getController();
            controllerWrapper.getControllerService().createScope(NameUtils.INTERNAL_SCOPE_NAME).get();

            @Cleanup
            ClientFactory internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, new ConnectionFactoryImpl(false));

            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize().get();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            @Cleanup
            SegmentStatsFactory segmentStatsFactory = new SegmentStatsFactory();
            SegmentStatsRecorder statsRecorder = segmentStatsFactory.createSegmentStatsRecorder(store,
                    internalCF,
                    AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                            .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0).build());

            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, "localhost", 12345, store,
                    statsRecorder);
            server.startListening();

            controllerWrapper.getControllerService().createScope("test").get();

            controller.createStream(CONFIG).get();
            @Cleanup
            MockClientFactory clientFactory = new MockClientFactory("test", controller);

            // Mocking pravega service by putting scale up and scale down requests for the stream
            EventStreamWriter<String> test = clientFactory.createEventWriter(
                    "test", new JavaSerializer<>(), EventWriterConfig.builder().build());

            final AtomicBoolean done = new AtomicBoolean(false);

            startWriter(test, done);

            Retry.withExpBackoff(10, 10, 100, 10000)
                    .retryingOn(NotDoneException.class)
                    .throwingOn(RuntimeException.class)
                    .runAsync(() -> controller.getCurrentSegments("test", "test")
                            .thenAccept(streamSegments -> {
                                if (streamSegments.getSegments().size() > 3) {
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
                    Transaction<String> transaction = test.beginTxn(5000, 3600000, 60000);

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
