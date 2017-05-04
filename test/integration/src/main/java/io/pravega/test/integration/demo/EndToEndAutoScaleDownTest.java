/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.demo;

import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.controller.util.Config;
import io.pravega.common.util.Retry;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.host.stat.AutoScalerConfig;
import io.pravega.service.server.host.stat.SegmentStatsFactory;
import io.pravega.service.server.host.stat.SegmentStatsRecorder;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.test.common.TestingServerStarter;
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
            TestingServer zkTestServer = new TestingServerStarter().start();

            int port = Config.SERVICE_PORT;
            @Cleanup
            ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port, false);
            Controller controller = controllerWrapper.getController();

            controllerWrapper.getControllerService().createScope(NameUtils.INTERNAL_SCOPE_NAME).get();
            ClientFactory internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, new ConnectionFactoryImpl(false));

            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            @Cleanup
            SegmentStatsFactory segmentStatsFactory = new SegmentStatsFactory();
            SegmentStatsRecorder statsRecorder = segmentStatsFactory.createSegmentStatsRecorder(store,
                    internalCF,
                    AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                            .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                            .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 5)
                            .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 30).build());

            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, "localhost", 12345, store,
                    statsRecorder);
            server.startListening();
            controllerWrapper.awaitRunning();
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
