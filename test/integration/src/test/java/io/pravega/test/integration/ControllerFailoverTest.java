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
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.net.URI;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for validating controller fail over behaviour.
 */
@Slf4j
public class ControllerFailoverTest {
    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream";

    private final int servicePort = TestUtils.getAvailableListenPort();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        // 1. Start ZK
        zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SSS
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();
    }

    @After
    public void cleanup() throws Exception {
        if (server != null) {
            server.close();
        }
        if (serviceBuilder != null) {
            serviceBuilder.close();
        }
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    @Test(timeout = 120000)
    public void testSessionExpiryToleranceMinimalServices() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int containerCount = 4;
        @Cleanup
        final ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                false, controllerPort, serviceHost, servicePort, containerCount, -1);
        testSessionExpiryTolerance(controllerWrapper, controllerPort);
    }

    @Test(timeout = 120000)
    public void testSessionExpiryToleranceAllServices() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int containerCount = 4;
        @Cleanup
        final ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                false, controllerPort, serviceHost, servicePort, containerCount, TestUtils.getAvailableListenPort());
        testSessionExpiryTolerance(controllerWrapper, controllerPort);
    }

    private void testSessionExpiryTolerance(final ControllerWrapper controllerWrapper, final int controllerPort) throws Exception {

        controllerWrapper.awaitRunning();

        // Simulate ZK session timeout
        controllerWrapper.forceClientSessionExpiry();

        // Now, that session has expired, lets do some operations.
        controllerWrapper.awaitPaused();

        controllerWrapper.awaitRunning();

        URI controllerURI = URI.create("tcp://localhost:" + controllerPort);
        @Cleanup
        StreamManager streamManager = StreamManager.create( ClientConfig.builder().controllerURI(controllerURI).build());

        // Create scope
        streamManager.createScope(SCOPE);

        // Create stream
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        streamManager.createStream(SCOPE, STREAM, streamConfiguration);

        streamManager.sealStream(SCOPE, STREAM);

        streamManager.deleteStream(SCOPE, STREAM);

        streamManager.deleteScope(SCOPE);

        controllerWrapper.close();

        controllerWrapper.awaitTerminated();
    }

    @Test(timeout = 30000)
    public void testStop() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int containerCount = 4;
        @Cleanup
        final ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                false, controllerPort, serviceHost, servicePort, containerCount, TestUtils.getAvailableListenPort());
        controllerWrapper.awaitRunning();
        controllerWrapper.close();
    }
}
