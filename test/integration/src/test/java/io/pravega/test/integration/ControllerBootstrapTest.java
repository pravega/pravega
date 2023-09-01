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

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.util.concurrent.CompletableFuture;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Collection of tests to validate controller bootstrap sequence.
 */
public class ControllerBootstrapTest {

    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream";

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final int servicePort = TestUtils.getAvailableListenPort();
    private TestingServer zkTestServer;
    private ControllerWrapper controllerWrapper;
    private PravegaConnectionListener server;
    private ServiceBuilder serviceBuilder;
    private StreamSegmentStore store;
    private TableStore tableStore;

    @Before
    public void setup() throws Exception {
        final String serviceHost = "localhost";
        final int containerCount = 4;

        // 1. Start ZK
        zkTestServer = new TestingServerStarter().start();
        // 2. Start controller
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                                                  controllerPort, serviceHost, servicePort, containerCount);

    }

    @After
    public void cleanup() throws Exception {
        if (controllerWrapper != null) {
            controllerWrapper.close();
        }
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

    @Test(timeout = 20000)
    public void bootstrapTest() throws Exception {
        Controller controller = controllerWrapper.getController();

        // Now start Pravega service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        store = serviceBuilder.createStreamSegmentService();
        tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        // Create test scope. This operation should succeed.
        Boolean scopeStatus = controller.createScope(SCOPE).join();
        Assert.assertEquals(true, scopeStatus);

        // Try creating a stream. It should not complete until Pravega host has started.
        // After Pravega host starts, stream should be successfully created.
        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(1))
                                                                     .build();
        CompletableFuture<Boolean> streamStatus = controller.createStream(SCOPE, STREAM, streamConfiguration);
        Assert.assertTrue(!streamStatus.isDone());
        // Ensure that create stream succeeds.

        Boolean status = streamStatus.join();
        Assert.assertEquals(true, status);

        // Now create transaction should succeed.
        CompletableFuture<TxnSegments> txIdFuture = controller.createTransaction(new StreamImpl(SCOPE, STREAM), 10000);

        TxnSegments id = txIdFuture.join();
        Assert.assertNotNull(id);

        controllerWrapper.awaitRunning();
    }
}
