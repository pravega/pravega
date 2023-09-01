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
package io.pravega.controller.store.task;

import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Zookeeper based task store.
 */
@Slf4j
class ZKTaskMetadataStore extends AbstractTaskMetadataStore {

    private final CuratorFramework client;
    private final String taskRoot = "/taskIndex";

    ZKTaskMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        super(new ZKHostIndex(client, "/hostTaskIndex", executor), executor);
        this.client = client;
    }

    @Override
    Void acquireLock(final Resource resource,
                             final TaskData taskData,
                             final String owner,
                             final String threadId) {
        // test and set implementation
        try {
            // for fresh lock, create the node and write its data.
            // if the node successfully got created, locking has succeeded,
            // else locking has failed.
            log.info("Acquiring lock with the owner {}, threadId {}", owner, threadId);
            LockData lockData = new LockData(owner, threadId, taskData.serialize());
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(getTaskPath(resource), lockData.serialize());
            return null;
        } catch (Exception e) {
            throw new LockFailedException(resource.getString(), e);
        }
    }

    @Override
    Void transferLock(final Resource resource,
                              final String owner,
                              final String threadId,
                              final String oldOwner,
                              final String oldThreadId) {
        boolean lockAcquired = false;

        // test and set implementation
        try {
            // Read the existing data along with its version.
            // Update data if version hasn't changed from the previously read value.
            // If update is successful, lock acquired else lock failed.
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath(getTaskPath(resource));
            LockData lockData = LockData.deserialize(data);
            log.info("Transfer lock with the lockData.getHostId {}, oldOwner {}, oldThreadId {}, " +
                    "owner {} and threadId {}", lockData.getHostId(), oldOwner, oldThreadId, owner, threadId);
            lockData = new LockData(owner, threadId, lockData.getTaskData());

            client.setData().withVersion(stat.getVersion())
                        .forPath(getTaskPath(resource), lockData.serialize());
        } catch (Exception e) {
            throw new LockFailedException(resource.getString(), e);
        }
        return null;
    }

    @Override
    Void removeLock(final Resource resource, final String owner, final String tag) {

        try {
            // test and set implementation
            Stat stat = new Stat();
            byte[] data = client.getData().storingStatIn(stat).forPath(getTaskPath(resource));
            if (data != null && data.length > 0) {
                LockData lockData = LockData.deserialize(data);
                if (lockData.isOwnedBy(owner, tag)) {

                    //Guaranteed Delete
                    //Solves this edge case: deleting a node can fail due to connection issues. Further, if the node was
                    //ephemeral, the node will not get auto-deleted as the session is still valid. This can wreak havoc
                    //with lock implementations.
                    //When guaranteed is set, Curator will record failed node deletions and attempt to delete them in the
                    //background until successful. NOTE: you will still get an exception when the deletion fails. But, you
                    //can be assured that as long as the CuratorFramework instance is open attempts will be made to delete
                    //the node.
                    client.delete()
                            .guaranteed()
                            .withVersion(stat.getVersion())
                            .forPath(getTaskPath(resource));
                } else {

                    log.warn("removeLock on resource {} failed, lock not owned by owner ({}, {})", resource, owner, tag);
                    throw new UnlockFailedException(resource.getString());

                }

            } else {

                client.delete()
                        .guaranteed()
                        .withVersion(stat.getVersion())
                        .forPath(getTaskPath(resource));
            }
            return null;

        } catch (KeeperException.NoNodeException e) {
            log.debug("removeLock on {} completed; resource was not locked", resource);
            return null;
        } catch (Exception e) {
            throw new UnlockFailedException(resource.getString(), e);
        }
    }

    @Override
    public CompletableFuture<Optional<TaskData>> getTask(final Resource resource,
                                                         final String owner,
                                                         final String tag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(owner);
            Preconditions.checkNotNull(tag);

            try {

                byte[] data = client.getData().forPath(getTaskPath(resource));

                if (data == null || data.length <= 0) {
                    log.debug("Empty data found for resource {}.", resource);
                    return Optional.empty();
                } else {
                    LockData lockData = LockData.deserialize(data);
                    return Optional.of(TaskData.deserialize(lockData.getTaskData()));
                }

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node {} does not exist.", getTaskPath(resource));
                return Optional.empty();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    private String getTaskPath(final Resource resource) {
        return taskRoot + "/" + getNode(resource);
    }
}
