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

    // todo: potentially merge this class with stream metadata store

    @Override
    public CompletableFuture<Void> lock(String resource, TaskData taskData, String owner, String oldOwner) {
        return CompletableFuture.supplyAsync(() -> {
            boolean lockAcquired = false;
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
                } catch (Exception e) {
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
                } catch (Exception e) {
                    throw new LockFailedException(resource, e);
                }
            }

            if (lockAcquired) {
                return null;
            } else {
                throw new LockFailedException(resource);
            }
        });
    }

    @Override
    public CompletableFuture<Void> unlock(String resource, String owner) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // test and set implementation
                Stat stat = new Stat();
                byte[] data = client.getData().storingStatIn(stat).forPath(getTaskPath(resource));
                if (data != null && data.length > 0) {
                    LockData lockData = LockData.deserialize(data);
                    if (lockData.getHostId().equals(owner)) {

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
                    }

                } else {

                    client.delete()
                            .guaranteed()
                            .withVersion(stat.getVersion())
                            .forPath(getTaskPath(resource));
                }
                return null;

            } catch (KeeperException.NoNodeException e) {
                log.debug("Lock not present on resource " + resource, e);
                return null;
            } catch (Exception e) {
                throw new UnlockFailedException(resource, e);
            }
        });
    }

    @Override
    public CompletableFuture<TaskData> getTask(String resource) {
        return CompletableFuture.supplyAsync(() -> {
            try {

                byte[] data = client.getData().forPath(getTaskPath(resource));

                if (data == null || data.length <= 0) {
                    log.debug(String.format("Empty data found for resource %s.", resource));
                    throw new TaskNotFoundException(resource);
                } else {
                    LockData lockData = LockData.deserialize(data);
                    return TaskData.deserialize(lockData.getTaskData());
                }

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node does not exist.", e);
                throw new TaskNotFoundException(resource, e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> putChild(String parent, String child) {
        return CompletableFuture.supplyAsync(() -> {
            try {

                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(getHostPath(parent, child));

                return null;

            } catch (KeeperException.NodeExistsException e) {
                log.debug("Node exists.", e);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> removeChild(String parent, String child, boolean deleteEmptyParent) {

        return CompletableFuture.supplyAsync(() -> {
            try {
                client.delete()
                        .forPath(getHostPath(parent, child));

                if (deleteEmptyParent) {
                    // if there are no children for the failed host, remove failed host znode
                    Stat stat = new Stat();
                    client.getData()
                            .storingStatIn(stat)
                            .forPath(getHostPath(parent));

                    if (stat.getNumChildren() == 0) {
                        client.delete()
                                .withVersion(stat.getVersion())
                                .forPath(getHostPath(parent));
                    }
                }
                return null;
            } catch (KeeperException.NoNodeException e) {
                log.debug("Node does not exist.", e);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> removeChildren(String parent, List<String> children, boolean deleteEmptyParent) {

        return CompletableFuture.supplyAsync(() -> {
            try {

                for (String child : children) {
                    client.delete()
                            .forPath(getHostPath(parent, child));
                }

                if (deleteEmptyParent) {
                    // if there are no children for the parent, remove parent znode
                    Stat stat = new Stat();
                    client.getData()
                            .storingStatIn(stat)
                            .forPath(getHostPath(parent));

                    if (stat.getNumChildren() == 0) {
                        client.delete()
                                .withVersion(stat.getVersion())
                                .forPath(getHostPath(parent));
                    }
                }
                return null;
            } catch (KeeperException.NoNodeException e) {
                log.debug("Node does not exist.", e);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> removeNode(String parent) {
        return CompletableFuture.supplyAsync(() -> {
            try {

                client.delete().forPath(getHostPath(parent));
                return null;

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node does not exist.", e);
                return null;
            } catch (KeeperException.NotEmptyException e) {
                log.debug("Node not empty.", e);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String parent) {
        return CompletableFuture.supplyAsync(() -> {
            try {

                return client.getChildren().forPath(getHostPath(parent));

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node does not exist.", e);
                return Collections.emptyList();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
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
