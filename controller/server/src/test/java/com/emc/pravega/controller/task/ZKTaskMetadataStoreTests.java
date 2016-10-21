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

import com.emc.pravega.controller.store.stream.StoreConfiguration;
import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskNotFoundException;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * ZK task metadata store tests.
 */
public class ZKTaskMetadataStoreTests {

    private final String resource = "resource1";
    private final String host1 = "host1";
    private final String host2 = "host2";
    private final TaskData taskData = new TaskData();

    private final TaskMetadataStore taskMetadataStore;

    public ZKTaskMetadataStoreTests() throws Exception {
        final TestingServer zkServer;
        zkServer = new TestingServer();
        zkServer.start();
        StoreConfiguration config = new StoreConfiguration(zkServer.getConnectString());
        taskMetadataStore = TaskStoreFactory.createStore(TaskStoreFactory.StoreType.Zookeeper, config);
        taskData.setMethodName("test");
        taskData.setMethodVersion("1.0");
        taskData.setParameters(new String[]{"string1"});
    }

    @Test
    public void testFolderOperations() throws ExecutionException, InterruptedException {
        final String child1 = "child1";
        final String child2 = "child2";

        taskMetadataStore.putChild(host1, child1).get();
        taskMetadataStore.putChild(host1, child2).get();

        List<String> children = taskMetadataStore.getChildren(host1).get();
        assertEquals(children.size(), 2);

        taskMetadataStore.removeChild(host1, child1, true).get();

        children = taskMetadataStore.getChildren(host1).get();
        assertEquals(children.size(), 1);

        taskMetadataStore.removeChild(host1, "randomChild", true).get();

        children = taskMetadataStore.getChildren(host1).get();
        assertEquals(children.size(), 1);

        taskMetadataStore.removeChild(host1, child2, true).get();

        children = taskMetadataStore.getChildren(host1).get();
        assertEquals(children.size(), 0);
    }

    @Test
    public void lockUnlockTests() throws ExecutionException, InterruptedException {

        taskMetadataStore.lock(resource, taskData, host1, null).get();

        TaskData data = taskMetadataStore.getTask(resource).get();
        assertArrayEquals(taskData.serialize(), data.serialize());

        taskMetadataStore.lock(resource, taskData, host2, host1).get();

        data = taskMetadataStore.getTask(resource).get();
        assertArrayEquals(taskData.serialize(), data.serialize());

        taskMetadataStore.unlock(resource, host2).get();

        try {
            taskMetadataStore.getTask(resource).join();
            assertTrue(false);
        } catch (Exception e) {
            // TaskNotFound exception is expected
            assertTrue(e.getCause() instanceof TaskNotFoundException);
        }

        taskMetadataStore.lock(resource, taskData, host1, null).get();

        data = taskMetadataStore.getTask(resource).get();
        assertArrayEquals(taskData.serialize(), data.serialize());

        taskMetadataStore.unlock(resource, host1).get();

        try {
            taskMetadataStore.getTask(resource).join();
            assertTrue(false);
        } catch (Exception e) {
            // TaskNotFound exception is expected
            assertTrue(e.getCause() instanceof TaskNotFoundException);
        }
    }

    @Test
    public void lockFailureTest() throws ExecutionException, InterruptedException {

        taskMetadataStore.lock(resource, taskData, host1, null).get();

        TaskData data = taskMetadataStore.getTask(resource).get();
        assertArrayEquals(taskData.serialize(), data.serialize());

        try {
            taskMetadataStore.lock(resource, taskData, host2, null).join();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof LockFailedException);
        }
    }
}
