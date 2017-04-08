/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.task;

import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.store.task.LockType;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaggedResource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.UnlockFailedException;
import com.emc.pravega.testcommon.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ZK task metadata store tests.
 */
@Slf4j
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

    @Test(timeout = 10000)
    public void readLockUnlockTest() {
        int seqNumber = taskMetadataStore.lock(resource, LockType.READ, taskData, host1, threadId1,
                Optional.<Integer>empty(), null, null).join();
        int seqNumber2 = taskMetadataStore.lock(resource, LockType.READ, taskData, host2, threadId2,
                Optional.<Integer>empty(), null, null).join();
        taskMetadataStore.unlock(resource, LockType.READ, seqNumber, host1, threadId1).join();
        taskMetadataStore.unlock(resource, LockType.READ, seqNumber2, host2, threadId2).join();
    }

    @Test(timeout = 10000)
    public void readWriteLockUnlockTest() {
        // Write lock followed by read lock should block
        testSequentialLockBlock(LockType.READ, LockType.WRITE);
        testSequentialLockBlock(LockType.WRITE, LockType.READ);
        testSequentialLockBlock(LockType.WRITE, LockType.WRITE);
    }

    private void testSequentialLockBlock(LockType type1, LockType type2) {
        int seqNumber = taskMetadataStore.lock(resource, type1, taskData, host1, threadId1,
                Optional.<Integer>empty(), null, null).join();

        CompletableFuture<Integer> future = taskMetadataStore.lock(resource, type2, taskData, host2, threadId2,
                Optional.<Integer>empty(), null, null);

        Optional<Pair<TaskData, Integer>> data = taskMetadataStore.getTask(resource, host1, threadId1).join();
        assertTrue(data.isPresent());
        assertEquals(seqNumber, data.get().getRight().intValue());
        assertArrayEquals(taskData.serialize(), data.get().getLeft().serialize());

        // Validate that the second lock is not yet acquired.
        try {
            future.get(200, TimeUnit.MILLISECONDS);
            Assert.fail("Acquiring lock unexpected");
        } catch (InterruptedException e) {
            log.error("Error waiting for lock to be acquired", e);
        } catch (ExecutionException e) {
            log.error("Error waiting for lock to be acquired", e);
            Assert.fail("");
        } catch (TimeoutException e) {
            Assert.assertTrue(true);
        }

        taskMetadataStore.unlock(resource, type1, seqNumber, host1, threadId1).join();

        int seqNumber2 = future.join();
        taskMetadataStore.unlock(resource, type2, seqNumber2, host2, threadId2).join();
    }

    @Test(timeout = 10000)
    public void writeLockUnlockTests() throws ExecutionException, InterruptedException {

        int seqNumber = taskMetadataStore.lock(resource, LockType.WRITE, taskData, host1, threadId1,
                Optional.empty(), null, null).get();

        Optional<Pair<TaskData, Integer>> data = taskMetadataStore.getTask(resource, host1, threadId1).get();
        assertTrue(data.isPresent());
        assertEquals(seqNumber, data.get().getRight().intValue());
        assertArrayEquals(taskData.serialize(), data.get().getLeft().serialize());

        taskMetadataStore.lock(resource, LockType.WRITE, taskData, host2, threadId2,
                Optional.of(seqNumber), host1, threadId1).get();

        data = taskMetadataStore.getTask(resource, host2, threadId2).get();
        assertTrue(data.isPresent());
        assertEquals(seqNumber, data.get().getRight().intValue());
        assertArrayEquals(taskData.serialize(), data.get().getLeft().serialize());

        taskMetadataStore.unlock(resource, LockType.WRITE, seqNumber, host2, threadId2).get();

        data = taskMetadataStore.getTask(resource, host2, threadId2).join();
        assertFalse(data.isPresent());

        seqNumber = taskMetadataStore.lock(resource, LockType.WRITE, taskData, host1, threadId1,
                Optional.empty(), null, null).get();

        data = taskMetadataStore.getTask(resource, host1, threadId1).get();
        assertTrue(data.isPresent());
        assertEquals(seqNumber, data.get().getRight().intValue());
        assertArrayEquals(taskData.serialize(), data.get().getLeft().serialize());

        taskMetadataStore.unlock(resource, LockType.WRITE, seqNumber, host1, threadId1).get();

        data = taskMetadataStore.getTask(resource, host1, threadId1).join();
        assertFalse(data.isPresent());
    }

    @Test(timeout = 10000)
    public void lockFailureTest() {

        int seqNumber = taskMetadataStore.lock(resource, LockType.WRITE, taskData, host1, threadId1,
                Optional.<Integer>empty(), null, null).join();

        Optional<Pair<TaskData, Integer>> data = taskMetadataStore.getTask(resource, host1, threadId1).join();
        assertTrue(data.isPresent());
        assertEquals(seqNumber, data.get().getRight().intValue());
        assertArrayEquals(taskData.serialize(), data.get().getLeft().serialize());

        CompletableFuture<Integer> lockFuture = taskMetadataStore.lock(resource, LockType.WRITE, taskData, host2,
                threadId2, Optional.<Integer>empty(), null, null);
        assertTrue(!lockFuture.isDone());

        AssertExtensions.assertThrows("LockFailedException expected",
                taskMetadataStore.lock(resource, LockType.WRITE, taskData, host2, threadId2,
                        Optional.of(10), "junk", "junk"),
                e -> e instanceof LockFailedException);

        AssertExtensions.assertThrows("Unlock fails if lock is not owned by the owner",
                taskMetadataStore.unlock(resource, LockType.WRITE, seqNumber, host2, threadId2),
                e -> e instanceof UnlockFailedException);

        AssertExtensions.assertThrows("Lock cannot be released before acquiring it",
                taskMetadataStore.unlock(resource, LockType.WRITE, 1, host2, threadId2),
                e -> e instanceof UnlockFailedException);

        AssertExtensions.assertThrows("Non-existent lock cannot be released",
                taskMetadataStore.unlock(resource, LockType.WRITE, 10, host2, threadId2),
                e -> e instanceof UnlockFailedException);

        taskMetadataStore.unlock(resource, LockType.WRITE, seqNumber, host1, threadId1).join();

        data = taskMetadataStore.getTask(resource, host1, threadId1).join();
        assertFalse(data.isPresent());

        seqNumber = lockFuture.join();
        taskMetadataStore.unlock(resource, LockType.WRITE, seqNumber, host2, threadId2).join();
    }
}
