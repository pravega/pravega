/**
 * Copyright Pravega Authors.
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

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.Retry;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestingServerStarter;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;

@Slf4j
public class EndToEndAutoScaleUpTest {
    static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                 .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 3))
                                                                 .build();

    public static void main(String[] args) throws Exception {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServerStarter().start();

            int port = Config.SERVICE_PORT;
            @Cleanup
            ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port, false);
            Controller controller = controllerWrapper.getController();

            ClientFactoryImpl internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, new SocketConnectionFactoryImpl(ClientConfig.builder().build()));

            @Cleanup("shutdownNow")
            val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
            @Cleanup
            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            serviceBuilder.initialize();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
            TableStore tableStore = serviceBuilder.createTableStoreService();
            @Cleanup
            AutoScaleMonitor autoScaleMonitor = new AutoScaleMonitor(store, internalCF,
                    AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                            .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0).build());
            IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store);

            @Cleanup
            PravegaConnectionListener server = new PravegaConnectionListener(false, false, "localhost", 12345, store, tableStore,
                    autoScaleMonitor.getStatsRecorder(), autoScaleMonitor.getTableSegmentStatsRecorder(), null, null, null, true,
                    serviceBuilder.getLowPriorityExecutor(), Config.TLS_PROTOCOL_VERSION.toArray(new String[Config.TLS_PROTOCOL_VERSION.size()]), indexAppendProcessor);
            server.startListening();

            controllerWrapper.awaitRunning();

            controllerWrapper.getControllerService().createScope("test", 0L).get();

            controller.createStream("test", "test", CONFIG).get();
            @Cleanup
            MockClientFactory clientFactory = new MockClientFactory("test", controller, internalCF.getConnectionPool());

            // Mocking pravega service by putting scale up and scale down requests for the stream
            @Cleanup
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
                            }), executor)
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
