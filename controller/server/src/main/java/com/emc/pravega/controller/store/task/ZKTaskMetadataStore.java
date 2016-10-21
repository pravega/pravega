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
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.store.stream.StoreConfiguration;
import com.emc.pravega.controller.task.TaskData;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Zookeeper based task store
 */
@Slf4j
class ZKTaskMetadataStore implements TaskMetadataStore {

    private final CuratorFramework client;

    private final String hostRoot = "/hostIndex";
    private final String taskRoot = "/taskIndex";

    public ZKTaskMetadataStore(StoreConfiguration config) {
        this.client = CuratorFrameworkFactory.newClient(config.getConnectionString(), new ExponentialBackoffRetry(1000, 3));
        this.client.start();
    }

    // todo: make all operations non-blocking
    // todo: potentially merge this class with stream metadata store

    @Override
    public CompletableFuture<Void> lock(String resource, TaskData taskData, String owner, String oldOwner) {
        boolean lockAcquired = false;
        try {
            // test and set implementation

            if (oldOwner == null || oldOwner.isEmpty()) {
                try {
                    // for fresh lock, create the node and write its data.
                    // if the node successfully got created, locking has succeeded,
                    // else locking has failed.
                    LockData lockData = new LockData(owner, taskData.serialize());
                    client.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(getTaskPath(resource), lockData.serialize());
                    lockAcquired = true;
                } catch (KeeperException.NodeExistsException e) {
                    throw new LockFailedException(resource, e);
                }
            } else {
                try {
                    // read the existing data along with its version
                    // update the data if version hasn't changed from the read value
                    // if update is successful, lock has been obtained
                    // else lock has failed
                    Stat stat = new Stat();
                    byte[] data = client.getData().storingStatIn(stat).forPath(getTaskPath(resource));
                    LockData lockData = LockData.deserialize(data);
                    if (lockData.getHostId().equals(oldOwner)) {
                        lockData = new LockData(owner, lockData.getTaskData());

                        client.setData().withVersion(stat.getVersion())
                                .forPath(getTaskPath(resource), lockData.serialize());
                        lockAcquired = true;
                    }
                } catch (KeeperException e) {
                    throw new LockFailedException(resource, e);
                }
            }
        } catch (Exception e) {
            log.error("Error locking resource.", e);
            throw new LockFailedException(resource, e);
        }

        if (lockAcquired) {
            return CompletableFuture.completedFuture(null);
        } else {
            throw new LockFailedException(resource);
        }
    }

    @Override
    public CompletableFuture<Void> unlock(String resource, String owner) {
        try {
                // test and set implementation
            Stat stat = new Stat();
                byte[] data = client.getData().storingStatIn(stat).forPath(getTaskPath(resource));
                if (data != null && data.length > 0) {
                    LockData lockData = LockData.deserialize(data);
                    if (lockData.getHostId().equals(owner)) {
                        client.delete().withVersion(stat.getVersion()).forPath(getTaskPath(resource));
                    }
                } else {
                    client.delete().withVersion(stat.getVersion()).forPath(getTaskPath(resource));
                }
        } catch (KeeperException.NoNodeException  e) {
            log.debug("Lock not present.", e);
            CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new UnlockFailedException(resource, e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TaskData> getTask(String resource) {
        try {
            byte[] data = client.getData().forPath(getTaskPath(resource));
            if (data == null || data.length <= 0) {
                log.debug(String.format("Empty data found for resource %s.", resource));
                throw new TaskNotFoundException(resource);
            } else {
                LockData lockData = LockData.deserialize(data);
                TaskData taskData = TaskData.deserialize(lockData.getTaskData());
                return CompletableFuture.completedFuture(taskData);
            }
        } catch (KeeperException.NoNodeException e) {
            log.debug("Node does not exist.", e);
            throw new TaskNotFoundException(resource, e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> putChild(String parent, String child) {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(getHostPath(parent, child));
            return CompletableFuture.completedFuture(null);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Node exists.", e);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeChild(String parent, String child, boolean deleteEmptyParent) {
        try {
            client.delete().forPath(getHostPath(parent, child));

            if (deleteEmptyParent) {
                // if there are no children for the failed host, remove failed host znode
                Stat stat = new Stat();
                client.getData().storingStatIn(stat).forPath(getHostPath(parent));
                if (stat.getNumChildren() == 0) {
                    client.delete().withVersion(stat.getVersion()).forPath(getHostPath(parent));
                }
            }
            return CompletableFuture.completedFuture(null);
        } catch (KeeperException.NoNodeException e) {
            log.debug("Node does not exist.", e);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeNode(String parent) {
        try {
            client.delete().forPath(getHostPath(parent));
            return CompletableFuture.completedFuture(null);
        } catch (KeeperException.NoNodeException e) {
            log.debug("Node does not exist.", e);
            return CompletableFuture.completedFuture(null);
        } catch (KeeperException.NotEmptyException e) {
            log.debug("Node not empty.", e);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String parent) {
        try {
            return CompletableFuture.completedFuture(client.getChildren().forPath(getHostPath(parent)));
        } catch (KeeperException.NoNodeException e) {
            log.debug("Node does not exist.", e);
            return CompletableFuture.completedFuture(Collections.emptyList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getTaskPath(String resource) {
        return taskRoot + "/" + resource;
    }

    private String getHostPath(String hostId, String resource) {
        return hostRoot + "/" + hostId + "/" + resource;
    }

    private String getHostPath(String hostId) {
        return hostRoot + "/" + hostId;
    }
}
