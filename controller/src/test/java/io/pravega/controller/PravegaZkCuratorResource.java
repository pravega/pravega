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

import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.test.common.TestingServerStarter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;

@Slf4j
public class PravegaZkCuratorResource extends ExternalResource {
    public CuratorFramework client;

    public TestingServer zkTestServer;
    public StoreClient storeClient;
    public RetryPolicy retryPolicy;
    public int sessionTimeoutMs;
    public int connectionTimeoutMs;

    public PravegaZkCuratorResource() {
       this(new ExponentialBackoffRetry(200, 10, 5000));
    }

    public PravegaZkCuratorResource(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public PravegaZkCuratorResource(int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public void before() throws Exception {
        //Instantiate test ZK service
        zkTestServer = new TestingServerStarter().start();
        String connectionString = zkTestServer.getConnectString();

        //Initialize ZK client
        if (sessionTimeoutMs == 0 && connectionTimeoutMs == 0) {
            client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        } else {
            client = CuratorFrameworkFactory.newClient(connectionString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        }
        client.start();
        storeClient = StoreClientFactory.createZKStoreClient(client);
    }

    @Override
    @SneakyThrows
    public void after() {
        storeClient.close();
        client.close();
        zkTestServer.close();
    }
}