/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.demo;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.controller.requesthandler.TxTimeoutStreamScheduler;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.monitor.MonitorFactory;
import com.emc.pravega.service.monitor.ThresholdMonitor;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockClientFactory;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class EndToEndAutoScaleTest {
    static StreamConfigurationImpl config = new StreamConfigurationImpl("test", "test",
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_KBPS, 1, 1, 3));

    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServer();

        ControllerWrapper controller = ControllerWrapper.getControllerWrapper(zkTestServer.getConnectString());
        ClientFactory internalCF = new ClientFactoryImpl("pravega", controller, new ConnectionFactoryImpl(false));
        ThresholdMonitor monitor = (ThresholdMonitor) MonitorFactory.createMonitor(MonitorFactory.MonitorType.ThresholdMonitor);
        if (monitor != null) {
            monitor.setClientFactory(internalCF);
        }

        TxTimeoutStreamScheduler.setClientFactory(internalCF);
        MonitorFactory.setClientFactory(internalCF);

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
        server.startListening();

        try {
            controller.createStream(config).get();
            MockClientFactory clientFactory = new MockClientFactory("test", controller);

            // Mocking pravega service by putting scale up and scale down requests for the stream
            EventStreamWriter<String> test = clientFactory.createEventWriter(
                    "test", new JavaSerializer<>(), new EventWriterConfig(null));

            // keep writing. Scale should happen
            long start = System.currentTimeMillis();
            char[] chars = new char[1000];
            Arrays.fill(chars, 'a');

            String str = new String(chars);

            while (true) {
                try {
                    test.writeEvent("1", str);
                } catch (Exception e) {
                    log.error("test exception writing events {}", e.getMessage());
                }

                if (System.currentTimeMillis() - start > Duration.ofMinutes(3).toMillis()) {
                    StreamSegments streamSegments = controller.getCurrentSegments("test", "test").get();
                    if (streamSegments.getSegments().size() > 3) {
                        System.err.println("Success");
                    } else {
                        System.out.println("Failure");
                    }
                    break;
                }
                // check if scale has happened
            }
        } catch (Exception e) {
            log.error("Test failed with exception: {}", e.getMessage());
            System.exit(-1);
        }

        System.exit(0);
    }
}
