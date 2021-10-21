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
package io.pravega.controller.server;

import com.google.common.util.concurrent.Service;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public abstract class ZkBasedControllerServiceMainTest extends ControllerServiceMainTest {
    private TestingServer zkServer;

    ZkBasedControllerServiceMainTest() {
        super(false);
    }

    @Override
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkServer.getConnectString())
                                                          .initialSleepInterval(500)
                                                          .maxRetries(1)
                                                          .sessionTimeoutMs(1000)
                                                          .namespace("pravega/" + UUID.randomUUID())
                                                          .build();
        storeClientConfig = getStoreConfig(zkClientConfig);
    }

    abstract StoreClientConfig getStoreConfig(ZKClientConfig zkClientConfig);

    @Override
    public void tearDown() throws IOException {
        zkServer.close();
    }

    static class MockControllerServiceStarter extends ControllerServiceStarter {
        @Getter
        private final CompletableFuture<Void> signalStartupCalled;
        @Getter
        private final CompletableFuture<Void> startupLatch;
        @Getter
        private final CompletableFuture<Void> signalShutdownStarted;
        @Getter
        private final CompletableFuture<Void> waitingForShutdownSignal;
        
        MockControllerServiceStarter(ControllerServiceConfig serviceConfig, StoreClient storeClient,
                                     CompletableFuture<Void> signalStartupCalled, CompletableFuture<Void> startupLatch,
                                     CompletableFuture<Void> signalShutdownStarted, CompletableFuture<Void> waitingForShutdownSignal) {
            super(serviceConfig, storeClient);
            this.signalShutdownStarted = signalShutdownStarted;
            this.waitingForShutdownSignal = waitingForShutdownSignal;
            this.signalStartupCalled = signalStartupCalled;
            this.startupLatch = startupLatch;
        }

        @Override
        protected void startUp() {
            signalStartupCalled.complete(null);
            startupLatch.join();
        }

        @Override
        protected void shutDown() throws Exception {
            signalShutdownStarted.complete(null);
            waitingForShutdownSignal.join();
        }
    }

    @Test(timeout = 100000)
    public void testZKSessionExpiry() throws Exception {
        CompletableFuture<Void> signalShutdownStarted = new CompletableFuture<>();
        CompletableFuture<Void> waitingForShutdownSignal = new CompletableFuture<>();

        ConcurrentLinkedQueue<StoreClient> clientQueue = new ConcurrentLinkedQueue<>();
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                (x, y) -> {
                    clientQueue.add(y);
                    return new MockControllerServiceStarter(x, y, new CompletableFuture<>(), CompletableFuture.completedFuture(null), signalShutdownStarted, waitingForShutdownSignal);
                });

        controllerServiceMain.startAsync();
        controllerServiceMain.awaitRunning();

        MockControllerServiceStarter controllerServiceStarter = (MockControllerServiceStarter) controllerServiceMain.awaitServiceStarting();
        controllerServiceStarter.awaitRunning();

        assertEquals(1, clientQueue.size());
        CuratorFramework curatorClient = (CuratorFramework) clientQueue.poll().getClient();
        // Simulate ZK session timeout
        // we will submit zk session expiration and 
        curatorClient.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

        CompletableFuture<Void> callBackCalled = new CompletableFuture<>();
        // issue a zkClient request.. 
        curatorClient.getData().inBackground((client1, event) -> {
            callBackCalled.complete(null);
        }).forPath("/test");

        // verify that termination is started. We will first make sure curator calls the callback before we let the 
        // ControllerServiceStarter to shutdown completely. This simulates grpc behaviour where grpc waits until all 
        // outstanding calls are complete before shutting down. 
        signalShutdownStarted.join();
        callBackCalled.join();

        // Now that callback has been called we can signal the shutdown of ControllerServiceStarter to complete. 
        waitingForShutdownSignal.complete(null);

        // Now, that session has expired, lets wait for starter to start again.
        controllerServiceMain.awaitServicePausing().awaitTerminated();
        controllerServiceMain.awaitServiceStarting().awaitRunning();

        // assert that previous curator client has indeed shutdown. 
        assertEquals(curatorClient.getState(), CuratorFrameworkState.STOPPED);

        // assert that a new curator client is added to the queue and it is in started state
        assertEquals(1, clientQueue.size());
        assertEquals(((CuratorFramework) clientQueue.peek().getClient()).getState(), CuratorFrameworkState.STARTED);

        controllerServiceMain.stopAsync();
        controllerServiceMain.awaitServicePausing().awaitTerminated();
        controllerServiceMain.awaitTerminated();
    }

    @Test(timeout = 10000L)
    public void testZKSessionExpiryBeforeStartupCompletes() throws Exception {
        CompletableFuture<StoreClient> storeClientFuture = new CompletableFuture<>();
        CompletableFuture<Void> waitForStartUpCalled = new CompletableFuture<>();
        CompletableFuture<Void> signalStartup = new CompletableFuture<>();
        AtomicReference<ControllerServiceStarter> starter = new AtomicReference<>();
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                (x, y) -> {
                    storeClientFuture.complete(y);
                    starter.set(new MockControllerServiceStarter(x, y, waitForStartUpCalled, signalStartup,
                            new CompletableFuture<>(), CompletableFuture.completedFuture(null)));
                    return starter.get();
                });

        controllerServiceMain.startAsync();
        waitForStartUpCalled.join();
        
        CuratorFramework curatorClient = (CuratorFramework) storeClientFuture.join().getClient();
        // Simulate ZK session timeout
        // we will submit zk session expiration and 
        curatorClient.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();

        Futures.completeAfter(() -> starter.get().getStoreClientFailureFuture(), signalStartup);
        
        // Now, that session has expired, lets wait for starter to start again.
        AssertExtensions.assertThrows(IllegalStateException.class, controllerServiceMain::awaitTerminated);
        assertTrue(controllerServiceMain.failureCause() instanceof IllegalStateException);
        assertEquals(controllerServiceMain.state(), Service.State.FAILED);
    }
}
