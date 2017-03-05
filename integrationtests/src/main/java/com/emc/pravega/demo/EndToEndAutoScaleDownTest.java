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
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class EndToEndAutoScaleDownTest {
    static final StreamConfiguration CONFIG =
            StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(
                    new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC, 10, 2, 1)).build();

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

            SegmentStatsRecorder statsRecorder = new SegmentStatsFactory().createSegmentStatsRecorder(store, "pravega", "requeststream",
                    internalCF, Duration.ofMinutes(0), Duration.ofMinutes(0), Duration.ofSeconds(5),
                    Duration.ofSeconds(30));

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

            // test scale down
            Thread.sleep(Duration.ofMinutes(1).toMillis());

            StreamSegments streamSegments = controller.getCurrentSegments("test", "test").get();
            if (streamSegments.getSegments().size() < 3) {
                System.err.println("Success");
                System.exit(0);
            } else {
                System.out.println("Failure");
                System.exit(1);
            }
        } catch (Throwable e) {
            System.err.print("Test failed with exception: " + e.getMessage());
            System.exit(-1);
        }

        System.exit(0);
    }
}
