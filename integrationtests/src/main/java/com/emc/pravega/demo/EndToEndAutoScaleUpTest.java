/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.host.stat.SegmentStatsFactory;
import com.emc.pravega.service.server.host.stat.SegmentStatsRecorder;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamSegments;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockClientFactory;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class EndToEndAutoScaleUpTest {
    static StreamConfiguration config =
            StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(
                    new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC, 10, 2, 3)).build();

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServer();

            ControllerWrapper controller = ControllerWrapper.getControllerWrapper(zkTestServer.getConnectString());
            ClientFactory internalCF = new ClientFactoryImpl("pravega", controller, new ConnectionFactoryImpl(false));

            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize().get();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            SegmentStatsRecorder statsRecorder = SegmentStatsFactory.createSegmentStatsRecorder(store, "pravega", "requeststream",
                    internalCF, Duration.ofMinutes(0),
                    Duration.ofMinutes(0), Duration.ofMinutes(10), Duration.ofMinutes(10));

            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store, statsRecorder);
            server.startListening();

            controller.createStream(config).get();
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

            Thread.sleep(130 * 1000);

            StreamSegments streamSegments = controller.getCurrentSegments("test", "test").get();
            if (streamSegments.getSegments().size() > 3) {
                System.err.println("Success scale up");
            } else {
                System.err.println("Failure");
                System.exit(1);
            }
        } catch (Throwable e) {
            System.err.print("Test failed with exception: " + e.getMessage());
            System.exit(-1);
        }

        System.exit(0);
    }
}
