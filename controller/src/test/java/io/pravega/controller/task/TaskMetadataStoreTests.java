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

import io.pravega.controller.store.task.LockFailedException;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaggedResource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.UnlockFailedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * ZK task metadata store tests.
 */
public abstract class TaskMetadataStoreTests {

    protected TaskMetadataStore taskMetadataStore;

    private final Resource resource = new Resource("scope", "stream1");
    private final String host1 = "host1";
    private final String host2 = "host2";
    private final String threadId1 = UUID.randomUUID().toString();
    private final String threadId2 = UUID.randomUUID().toString();
    private final TaskData taskData;

    public TaskMetadataStoreTests() {
        taskData = new TaskData("test", "1.0", new String[]{"string1"});
    }

    @Before
    public abstract void setupTaskStore() throws Exception;

    @After
    public abstract void cleanupTaskStore() throws IOException;

    @Test(timeout = 10000)
    public void testFolderOperations() throws ExecutionException, InterruptedException {
        final TaggedResource child1 = new TaggedResource(UUID.randomUUID().toString(), resource);
        final TaggedResource child2 = new TaggedResource(UUID.randomUUID().toString(), resource);
        final TaggedResource child3 = new TaggedResource(UUID.randomUUID().toString(), resource);

        Set<String> hosts = taskMetadataStore.getHosts().get();
        assertTrue(hosts.isEmpty());

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

        hosts = taskMetadataStore.getHosts().get();
        assertEquals(1, hosts.size());

        child = taskMetadataStore.getRandomChild(host1).get();
        assertTrue(child.isPresent());
        assertTrue(child.get().getResource().equals(resource));

        taskMetadataStore.removeChild(host1, child2, true).get();

        child = taskMetadataStore.getRandomChild(host1).get();
        assertFalse(child.isPresent());
    }

    @Test(timeout = 10000)
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

        Void result = taskMetadataStore.unlock(resource, host1, threadId1).join();
        assertNull(result);
    }

    @Test(timeout = 10000)
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
