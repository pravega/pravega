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
package io.pravega.controller;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.test.common.TestingServerStarter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;


import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class PravegaZkCuratorResource extends ExternalResource {
    public ScheduledExecutorService executor;
    public CuratorFramework client;

    public TestingServer zkTestServer;
    public StoreClient storeClient;

    @Override
    public void before() throws Exception {
        //Instantiate test ZK service
        zkTestServer = new TestingServerStarter().start();
        String connectionString = zkTestServer.getConnectString();

        //Initialize the executor service.
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");

        //Initialize ZK client
        client = CuratorFrameworkFactory.newClient(connectionString, new RetryOneTime(2000));
        client.start();
        storeClient = StoreClientFactory.createZKStoreClient(client);
    }

    @Override
    @SneakyThrows
    public void after() {
        ExecutorServiceHelpers.shutdown(executor);
        client.close();
        storeClient.close();
        zkTestServer.close();
    }
}
