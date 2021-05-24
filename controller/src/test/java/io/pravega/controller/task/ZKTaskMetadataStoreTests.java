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
package io.pravega.controller.task;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.test.common.TestingServerStarter;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.ClassRule;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

/**
 * Zk task metadata store tests.
 */
public class ZKTaskMetadataStoreTests extends TaskMetadataStoreTests {

    private final static RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(RETRY_POLICY);
    private TestingServer zkServer;
    private ScheduledExecutorService executor;
    private CuratorFramework cli;

    @Override
    public void setupTaskStore() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
//        zkServer = new TestingServerStarter().start();
//        zkServer.start();
//        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
//        cli.start();
        taskMetadataStore = TaskStoreFactory.createZKStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
    }

    @Override
    public void cleanupTaskStore() throws IOException {
//        if (cli != null) {
//            cli.close();
//        }
//
//        if (zkServer != null) {
//            zkServer.close();
//        }

        if (executor != null) {
            ExecutorServiceHelpers.shutdown(executor);
        }
    }
}
