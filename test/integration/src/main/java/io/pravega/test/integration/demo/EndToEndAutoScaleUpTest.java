/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.test.integration.demo;

import io.pravega.client.ClientFactory;
import io.pravega.server.controller.service.util.Config;
import io.pravega.common.util.Retry;
import io.pravega.server.segmentstore.contracts.StreamSegmentStore;
import io.pravega.server.segmentstore.service.host.handler.PravegaConnectionListener;
import io.pravega.server.segmentstore.service.host.stat.AutoScalerConfig;
import io.pravega.server.segmentstore.service.host.stat.SegmentStatsFactory;
import io.pravega.server.segmentstore.service.host.stat.SegmentStatsRecorder;
import io.pravega.server.segmentstore.service.store.ServiceBuilder;
import io.pravega.server.segmentstore.service.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.netty.ConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.test.common.TestingServerStarter;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

@Slf4j
public class EndToEndAutoScaleUpTest {
    static final StreamConfiguration CONFIG =
            StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(
                    ScalingPolicy.byEventRate(10, 2, 3)).build();

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServerStarter().start();

            int port = Config.SERVICE_PORT;
            @Cleanup
            ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port, false);
            Controller controller = controllerWrapper.getController();

            ClientFactory internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, new ConnectionFactoryImpl(false));

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
                    statsRecorder);
            server.startListening();

            controllerWrapper.awaitRunning();

            controllerWrapper.getControllerService().createScope("test").get();

            controller.createStream(CONFIG).get();
            @Cleanup
            MockClientFactory clientFactory = new MockClientFactory("test", controller);

            // Mocking pravega service by putting scale up and scale down requests for the stream
            EventStreamWriter<String> test = clientFactory.createEventWriter(
                    "test", new JavaSerializer<>(), EventWriterConfig.builder().build());

            // keep writing. Scale should happen
            long start = System.currentTimeMillis();
            char[] chars = new char[1];
            Arrays.fill(chars, 'a');

            String str = new String(chars);

            CompletableFuture.runAsync(() -> {
                while (System.currentTimeMillis() - start < Duration.ofMinutes(3).toMillis()) {
                    try {
                        test.writeEvent("0", str).get();
                    } catch (Throwable e) {
                        System.err.println("test exception writing events " + e.getMessage());
                        break;
                    }
                }
            });

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
            System.exit(-1);
        }

        System.exit(0);
    }
}
