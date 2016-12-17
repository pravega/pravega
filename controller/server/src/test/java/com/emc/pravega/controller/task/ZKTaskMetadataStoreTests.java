/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.task;

import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Zk task metadata store tests.
 */
public class ZKTaskMetadataStoreTests extends TaskMetadataStoreTests {

    private TestingServer zkServer;

    @Override
    public void setupTaskStore() throws Exception {
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        zkServer = new TestingServer();
        zkServer.start();
        CuratorFramework cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        taskMetadataStore = TaskStoreFactory.createStore(new ZKStoreClient(cli), executor);
    }

    @Override
    public void cleanupTaskStore() throws IOException {
        zkServer.close();
    }
}
