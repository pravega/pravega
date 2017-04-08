/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Zookeeper based task store.
 */
@Slf4j
class ZKTaskMetadataStore extends AbstractTaskMetadataStore {

    private final static String TAG_SEPARATOR = "_%%%_";
    private final static String RESOURCE_PART_SEPARATOR = "_%_";
    private final static String HOST_ROOT = "/hostIndex";
    private final static String TASK_ROOT = "/taskIndex";
    private final CuratorFramework client;

    ZKTaskMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        super(executor);
        this.client = client;
    }

    @Override
    public CompletableFuture<Integer> createLockNode(final Resource resource,
                               final LockType type,
                               final String owner,
                               final String tagId,
                               final TaskData taskData) {
        return CompletableFuture.supplyAsync(() -> {
            LockData lockData = new LockData(owner, tagId, taskData.serialize());
            try {
                String path = client.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                        .forPath(getLockPath(resource, type), lockData.serialize());
                return getSeqNumber(path);
            } catch (Exception e) {
                throw new LockFailedException(resource.toString(), e);
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> transferLockNode(final Resource resource,
                                  final LockType type,
                                  final int seqNumber,
                                  final String owner,
                                  final String tagId,
                                  final String oldOwner,
                                  final String oldTagId) {
        return CompletableFuture.supplyAsync(() -> {
            String lockPath = getLockPath(resource, type, seqNumber);

            // test and set implementation
            try {
                // Read the existing data along with its version.
                // Update data if version hasn't changed from the previously read value.
                // If update is successful, lock acquired,
                // else somebody else has successfully acquired the lock, and this attempt fails.
                Stat stat = new Stat();
                byte[] data = client.getData().storingStatIn(stat).forPath(lockPath);
                LockData lockData = LockData.deserialize(data);
                if (lockData.isOwnedBy(oldOwner, oldTagId)) {
                    lockData = new LockData(owner, tagId, lockData.getTaskData());
                    client.setData().withVersion(stat.getVersion())
                            .forPath(lockPath, lockData.serialize());
                    return null;
                } else {
                    throw new LockFailedException(
                            String.format("Failed transferring lock on resource %s, as it is not owned by (%s,%s)",
                                    resource.getString(), oldOwner, oldTagId));
                }
            } catch (Exception e) {
                throw new LockFailedException(resource.getString(), e);
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> checkIfLockAcquired(final Resource resource,
                                                        final LockType type,
                                                        final int seqNumber,
                                                        final AtomicReference<Boolean> lockAcquired) {
        String resourcePath = getTaskPath(resource);
        CompletableFuture<Void> resourceChanged = new CompletableFuture<>();
        try {
            List<String> children = client.getChildren()
                    .usingWatcher((Watcher) event -> resourceChanged.complete(null))
                    .forPath(resourcePath);
            if (isLockAcquired(type, seqNumber, children)) {
                lockAcquired.set(true);
                resourceChanged.complete(null);
            }
            return resourceChanged;
        } catch (Exception e) {
            log.warn("Error fetching children of {}, {}", resourcePath, e.getMessage());
            resourceChanged.complete(null);
            return FutureHelpers.delayedFuture(Duration.ofMillis(100), this.executor);
        }
    }

    @Override
    Void removeLock(final Resource resource, final LockType type, final int seqNumber, final String owner, final String tag) {

        try {
            // Ensure that the lock (type, seqNumber) is held before attempting to unlock.
            if (!isLockHeld(resource, type, seqNumber)) {
                String errorMsg = String.format("Lock node %s does not hold the lock on resource %s",
                        getLockNodeName(type, seqNumber), resource.getString());
                log.warn(errorMsg);
                throw new UnlockFailedException(errorMsg);
            }

            // Ensure that the lock node is owned by the (owner, tag) before deleting the lock node.
            Stat stat = new Stat();
            String lockPath = getLockPath(resource, type, seqNumber);
            byte[] data = client.getData().storingStatIn(stat).forPath(lockPath);
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
                            .forPath(lockPath);
                } else {
                    String errorMsg = String.format("Lock %s on resource %s not owned by owner %s: tag %s",
                            getLockNodeName(type, seqNumber), resource.getString(), owner, tag);
                    log.warn(errorMsg);
                    throw new UnlockFailedException(errorMsg);
                }
                // finally delete the resource node if it is empty
                deleteEmptyResource(resource);
            } else {
                String errorMsg = String.format("Corrupt lock data for resource %s and lock node %s",
                        resource.getString(), lockPath);
                log.warn(errorMsg);
                throw new UnlockFailedException(errorMsg);
            }
            return null;
        } catch (KeeperException.NoNodeException e) {
            log.info(String.format("Lock node %s not present under resource %s",
                    getLockNodeName(type, seqNumber), resource));
            return null;
        } catch (UnlockFailedException e) {
            throw e;
        } catch (Exception e) {
            throw new UnlockFailedException(resource.getString(), e);
        }
    }

    private boolean isLockHeld(Resource resource, LockType type, int seqNumber) throws Exception {
        String resourcePath = getTaskPath(resource);
        List<String> children = client.getChildren().forPath(resourcePath);
        return isLockAcquired(type, seqNumber, children);
    }

    private void deleteEmptyResource(Resource resource) {
        String path = getTaskPath(resource);
        try {
            client.delete().forPath(path);
        } catch (Exception e) {
            // It is ok to not delete the resource node, hence just log the error and proceed.
            log.warn("Failed trying to check if resource {} is empty and deleting it if empty. Error: {}",
                    resource.getString(), e.getMessage());
        }
    }

    @Override
    public CompletableFuture<Optional<Pair<TaskData, Integer>>> getTask(final Resource resource,
                                                         final String owner,
                                                         final String tag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(owner);
            Preconditions.checkNotNull(tag);

            try {
                List<String> children = client.getChildren().forPath(getTaskPath(resource));

                for (String child : children) {
                    byte[] data = client.getData().forPath(getTaskPath(resource) + "/" + child);

                    if (data != null && data.length > 0) {
                        LockData lockData = LockData.deserialize(data);
                        if (lockData.isOwnedBy(owner, tag)) {
                            return Optional.of(new ImmutablePair<>(TaskData.deserialize(lockData.getTaskData()),
                                    getSeqNumber(child)));
                        }
                    }
                }

                log.debug("Resource {} not owned by pair ({}, {})", resource.getString(), owner, tag);
                return Optional.empty();
            } catch (KeeperException.NoNodeException e) {
                log.debug("Node {} does not exist.", getTaskPath(resource));
                return Optional.empty();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Void> putChild(final String parent, final TaggedResource child) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(parent);
            Preconditions.checkNotNull(child);

            try {

                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(getHostPath(parent, child));

                return null;

            } catch (KeeperException.NodeExistsException e) {
                log.debug("Node {} exists.", getHostPath(parent, child));
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Void> removeChild(final String parent, final TaggedResource child, final boolean deleteEmptyParent) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(parent);
            Preconditions.checkNotNull(child);

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
                log.debug("Node {} does not exist.", getNode(child));
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Void> removeNode(final String parent) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(parent);

            try {

                client.delete().forPath(getHostPath(parent));
                return null;

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node {} does not exist.", getHostPath(parent));
                return null;
            } catch (KeeperException.NotEmptyException e) {
                log.debug("Node {} not empty.", getHostPath(parent));
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Optional<TaggedResource>> getRandomChild(final String parent) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(parent);

            try {

                List<String> children = client.getChildren().forPath(getHostPath(parent));
                if (children.isEmpty()) {
                    return Optional.empty();
                } else {
                    Random random = new Random();
                    return Optional.of(getTaggedResource(children.get(random.nextInt(children.size()))));
                }

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node {} does not exist.", getHostPath(parent));
                return Optional.empty();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    private String getTaskPath(final Resource resource) {
        return TASK_ROOT + "/" + getNode(resource);
    }

    private String getHostPath(final String hostId, final TaggedResource resource) {
        return HOST_ROOT + "/" + hostId + "/" + getNode(resource);
    }

    private String getHostPath(final String hostId) {
        return HOST_ROOT + "/" + hostId;
    }

    private String getNode(final Resource resource) {
        return resource.getString().replaceAll("/", RESOURCE_PART_SEPARATOR);
    }

    private String getNode(final TaggedResource resource) {
        return getNode(resource.getResource()) + TAG_SEPARATOR + resource.getTag();
    }

    private Resource getResource(final String node) {
        String[] parts = node.split(RESOURCE_PART_SEPARATOR);
        return new Resource(parts);
    }

    private TaggedResource getTaggedResource(final String node) {
        String[] splits = node.split(TAG_SEPARATOR);
        return new TaggedResource(splits[1], getResource(splits[0]));
    }

    private String getLockPath(Resource resource, LockType type, int seqNumber) {
        return String.format("%s/%s", getTaskPath(resource), getLockNodeName(type, seqNumber));
    }

    private String getLockPath(Resource resource, LockType type) {
        return String.format("%s/%s", getTaskPath(resource), type.toString());
    }
}
