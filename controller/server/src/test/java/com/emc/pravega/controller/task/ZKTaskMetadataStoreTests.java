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
import com.emc.pravega.controller.store.stream.StoreConfiguration;
import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaggedResource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.store.task.UnlockFailedException;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ZK task metadata store tests.
 */
public class ZKTaskMetadataStoreTests {

    private final Resource resource = new Resource("scope", "stream1");
    private final String host1 = "host1";
    private final String host2 = "host2";
    private final String threadId1 = UUID.randomUUID().toString();
    private final String threadId2 = UUID.randomUUID().toString();
    private final TaskData taskData = new TaskData();

    private final TaskMetadataStore taskMetadataStore;

    public ZKTaskMetadataStoreTests() throws Exception {
        final TestingServer zkServer;
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        zkServer = new TestingServer();
        zkServer.start();
        StoreConfiguration config = new StoreConfiguration(zkServer.getConnectString());
        taskMetadataStore = TaskStoreFactory.createStore(new ZKStoreClient(config), executor);
        taskData.setMethodName("test");
        taskData.setMethodVersion("1.0");
        taskData.setParameters(new String[]{"string1"});
    }

    @Test
    public void testFolderOperations() throws ExecutionException, InterruptedException {
        final TaggedResource child1 = new TaggedResource(UUID.randomUUID().toString(), resource);
        final TaggedResource child2 = new TaggedResource(UUID.randomUUID().toString(), resource);
        final TaggedResource child3 = new TaggedResource(UUID.randomUUID().toString(), resource);

        taskMetadataStore.putChild(host1, child1).get();
        taskMetadataStore.putChild(host1, child2).get();

        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(host1).get();
        assertTrue(child.isPresent());
        assertTrue(child.get().getResource().equals(resource));

        taskMetadataStore.removeChild(host1, child1, true).get();

        child = taskMetadataStore.getRandomChild(host1).get();
        assertTrue(child.isPresent());
        assertTrue(child.get().getResource().equals(resource));

        taskMetadataStore.removeChild(host1, child3, true).get();

        child = taskMetadataStore.getRandomChild(host1).get();
        assertTrue(child.isPresent());
        assertTrue(child.get().getResource().equals(resource));

        taskMetadataStore.removeChild(host1, child2, true).get();

        child = taskMetadataStore.getRandomChild(host1).get();
        assertFalse(child.isPresent());
    }

    @Test
    public void lockUnlockTests() throws ExecutionException, InterruptedException {

        taskMetadataStore.lock(resource, taskData, host1, threadId1, null, null).get();

        Optional<TaskData> data = taskMetadataStore.getTask(resource, host1, threadId1).get();
        assertTrue(data.isPresent());
        assertArrayEquals(taskData.serialize(), data.get().serialize());

        taskMetadataStore.lock(resource, taskData, host2, threadId2, host1, threadId1).get();

        data = taskMetadataStore.getTask(resource, host2, threadId2).get();
        assertTrue(data.isPresent());
        assertArrayEquals(taskData.serialize(), data.get().serialize());

        taskMetadataStore.unlock(resource, host2, threadId2).get();

        data = taskMetadataStore.getTask(resource, host2, threadId2).join();
        assertFalse(data.isPresent());

        taskMetadataStore.lock(resource, taskData, host1, threadId1, null, null).get();

        data = taskMetadataStore.getTask(resource, host1, threadId1).get();
        assertTrue(data.isPresent());
        assertArrayEquals(taskData.serialize(), data.get().serialize());

        taskMetadataStore.unlock(resource, host1, threadId1).get();

        data = taskMetadataStore.getTask(resource, host1, threadId1).join();
        assertFalse(data.isPresent());
    }

    @Test
    public void lockFailureTest() throws ExecutionException, InterruptedException {

        taskMetadataStore.lock(resource, taskData, host1, threadId1, null, null).get();

        Optional<TaskData> data = taskMetadataStore.getTask(resource, host1, threadId1).get();
        assertTrue(data.isPresent());
        assertArrayEquals(taskData.serialize(), data.get().serialize());

        try {
            taskMetadataStore.lock(resource, taskData, host2, threadId2, null, null).join();
        } catch (Exception e) {
            assertTrue(e instanceof CompletionException);
            assertTrue(e.getCause() instanceof LockFailedException);
        }

        try {
            taskMetadataStore.lock(resource, taskData, host2, threadId2, "junk", "junk").join();
        } catch (Exception e) {
            assertTrue(e instanceof CompletionException);
            assertTrue(e.getCause() instanceof LockFailedException);
        }

        try {
            taskMetadataStore.unlock(resource, host2, threadId2).join();
        } catch (Exception e) {
            assertTrue(e instanceof CompletionException);
            assertTrue(e.getCause() instanceof UnlockFailedException);
        }

        taskMetadataStore.unlock(resource, host1, threadId1).join();

        data = taskMetadataStore.getTask(resource, host1, threadId1).get();
        assertFalse(data.isPresent());
    }
}
