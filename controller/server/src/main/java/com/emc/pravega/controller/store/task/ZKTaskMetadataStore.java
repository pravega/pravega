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
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Zookeeper based task store
 */
@Slf4j
class ZKTaskMetadataStore implements TaskMetadataStore {

    private static final long LOCK_WAIT_TIME = 1;
    private final CuratorFramework client;

    private final String hostRoot = "/hostIndex";
    private final String lockRoot = "/locks";
    private final String taskRoot = "/tasks";
    private final String mutexLockRoot = "/mutexLocks";

    private final String hostId;

    public ZKTaskMetadataStore(StoreConfiguration config, String hostId) {
        this.client = CuratorFrameworkFactory.newClient(config.getConnectionString(), new ExponentialBackoffRetry(1000, 3));
        this.client.start();
        this.hostId = hostId;
    }

    @Override
    public CompletableFuture<Void> lock(String resource, TaskData taskData, String oldHost) {
        boolean lockAcquired = false;
        InterProcessMutex mutex = new InterProcessMutex(client, getMutexLockPath(resource));
        try {
            boolean success = mutex.acquire(LOCK_WAIT_TIME, TimeUnit.SECONDS);
            if (success) {
                // test and set implementation
                Stat stat = client.checkExists().forPath(getLockPath(resource));
                if (oldHost == null || oldHost.isEmpty()) {
                    // fresh lock
                    if (stat == null || stat.getDataLength() == 0) {
                        LockData lockData = new LockData(this.hostId, taskData.serialize());
                        client.create()
                                .creatingParentsIfNeeded()
                                .withMode(CreateMode.PERSISTENT)
                                .forPath(getLockPath(resource), lockData.serialize());
                        lockAcquired = true;
                    }
                } else {
                    // replace old host
                    if (stat != null && stat.getDataLength() > 0) {
                        byte[] data = client.getData().forPath(getLockPath(resource));
                        LockData lockData = LockData.deserialize(data);
                        if (lockData.getHostId().equals(oldHost)) {
                            lockData = new LockData(this.hostId, lockData.getTaskData());
                            client.create()
                                    .creatingParentsIfNeeded()
                                    .withMode(CreateMode.PERSISTENT)
                                    .forPath(getLockPath(resource), lockData.serialize());
                            lockAcquired = true;
                        }
                    }
                }
                // finally release lock
                mutex.release();
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
    public CompletableFuture<Void> unlock(String resource) {
        InterProcessMutex mutex = new InterProcessMutex(client, getMutexLockPath(resource));
        try {
            boolean success = mutex.acquire(LOCK_WAIT_TIME, TimeUnit.SECONDS);
            if (success) {
                // test and set implementation
                byte[] data = client.getData().forPath(getLockPath(resource));
                if (data != null) {
                    LockData lockData = LockData.deserialize(data);
                    if (lockData.getHostId().equals(this.hostId)) {
                        client.delete().forPath(getLockPath(resource));
                    }
                }
                // finally release lock
                mutex.release();
            }
        } catch (Exception e) {
            log.error("Error locking resource.", e);
            throw new UnlockFailedException(resource, e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<byte[]> get(String resource) {
        try {
            byte[] data = client.getData().forPath(getResourcePath(resource));
            return CompletableFuture.completedFuture(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> putChild(String resource) {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(getHostPath(this.hostId, resource));
            return CompletableFuture.completedFuture(null);
        } catch (KeeperException.NodeExistsException e) {
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeChild(String resource) {
        try {
            client.delete().forPath(getHostPath(this.hostId, resource));
            return CompletableFuture.completedFuture(null);
        } catch (KeeperException.NoNodeException e) {
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> removeChild(String failedHostId, String resource) {
        try {
            client.delete().forPath(getHostPath(failedHostId, resource));

            // if there are no children for the failed host, remove failed host znode
            List<String> children = client.getChildren().forPath(getHostPath(failedHostId));
            if (children == null || children.isEmpty()) {
                client.delete().forPath(getHostPath(failedHostId));
            }
            return CompletableFuture.completedFuture(null);
        } catch (KeeperException.NoNodeException e) {
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String hostId) {
        try {
            return CompletableFuture.completedFuture(client.getChildren().forPath(getHostPath(hostId)));
        } catch (KeeperException.NoNodeException e) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getMutexLockPath(String resource) {
        return mutexLockRoot + "/" + resource;
    }

    private String getLockPath(String resource) {
        return lockRoot + "/" + resource;
    }

    private String getResourcePath(String resource) {
        return taskRoot + "/" + resource;
    }

    private String getHostPath(String hostId, String resource) {
        return hostRoot + "/" + hostId + "/" + resource;
    }

    private String getHostPath(String hostId) {
        return hostRoot + "/" + hostId;
    }
//    @Override
//    public CompletableFuture<List<TaskData>> getOrphanedTasks() {
//        List<TaskData> tasks = new ArrayList<>();
//        try {
//            List<String> children = client.getChildren().forPath(Paths.STREAM_TASK_ROOT);
//            for (String streamName : children) {
//                // find the task details for this stream's update operation
//                byte[] data = client.getData().forPath(Paths.STREAM_TASK_ROOT + streamName);
//                if (data != null && data.length > 0) {
//                    // if no one is holding a lock, try to lock the task and execute it
//                    List<String> locks = client.getChildren().forPath(Paths.STREAM_LOCKS_ROOT + streamName);
//                    if (locks != null && locks.size() == 0) {
//                        tasks.add(TaskData.deserialize(data));
//                    }
//                }
//            }
//            return CompletableFuture.completedFuture(tasks);
//        } catch (Exception ex) {
//            CompletableFuture<List<TaskData>> future = new CompletableFuture<>();
//            future.completeExceptionally(ex);
//            return future;
//        }
//    }
}
