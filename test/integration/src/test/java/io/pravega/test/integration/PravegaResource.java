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

import io.pravega.client.control.impl.Controller;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;

import java.net.URI;

/**
 * This class contains the rules to start and stop in Pravega for integration tests.
 * This resource can be configured for a test using @ClassRule and it will ensure standalone is started once
 * for all the methods in a test and is shutdown once all the tests methods have completed execution.
 *
 * - Usage pattern to start it once for all @Test methods in a class
 *  <pre>
 *  &#64;ClassRule
 *  public static final PravegaResource PRAVEGA = new PravegaResource();
 *  </pre>
 *  - Usage pattern to start it before every @Test method.
 *  <pre>
 *  &#64;Rule
 *  public final PravegaResource pravega = new PravegaResource();
 *  </pre>
 *
 */
@Slf4j
public class PravegaResource extends ExternalResource {
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int containerCount = 4;

    // These are initialized in "before" to allow a PravegaResource to be 'reused' because tests that extend from abstract classes may do this.
    private TestingServer zkTestServer;
    private ControllerWrapper controllerWrapper;
    private PravegaConnectionListener server;
    private ServiceBuilder serviceBuilder;

    @Override
    protected void before() throws Exception {
        // 1. Start ZK
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        // 2. Start controller
        // the default watermarking frequency on the controller is 10 seconds.
        System.setProperty("controller.watermarking.frequency.seconds", "5");
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        
        controllerWrapper.awaitRunning();
    }

    @Override
    @SneakyThrows
    protected void after() {
        if (controllerWrapper != null) {
            controllerWrapper.close();
            controllerWrapper = null;
        }
        if (server != null) {
            server.close();
            server = null;
        }
        if (serviceBuilder != null) {
            serviceBuilder.close();
            serviceBuilder = null;
        }
        if (zkTestServer != null) {
            zkTestServer.close();
            zkTestServer = null;
        }
    }

    public Controller getLocalController() {
        return controllerWrapper.getController();
    }

    public URI getControllerURI() {
        return URI.create("tcp://" + serviceHost + ":" + controllerPort);
    }
}


