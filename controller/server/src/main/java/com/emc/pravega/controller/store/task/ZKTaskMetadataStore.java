/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.task;

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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Zookeeper based task store.
 */
@Slf4j
class ZKTaskMetadataStore extends AbstractTaskMetadataStore {

    private final static String TAG_SEPARATOR = "_%%%_";
    private final static String RESOURCE_PART_SEPARATOR = "_%_";
    private final CuratorFramework client;
    private final static String HOST_ROOT = "/hostIndex";
    private final static String TASK_ROOT = "/taskIndex";
    private final static String WRITE_PREFIX = LockType.WRITE.toString();

    ZKTaskMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        super(executor);
        this.client = client;
    }

    @Override
    Integer acquireLock(final Resource resource,
                        final LockType type,
                             final TaskData taskData,
                             final String owner,
                             final String threadId) {
        // Create a sequential persistent node with LockData(owner, threadId, taskData) as data
        int seqNumber = createLockNode(resource, type, owner, threadId, taskData);

        // Wait until seqNumber is the smallest numbered node among children of resource node
        waitUntilLockAcquired(resource, type, seqNumber);

        return seqNumber;
    }

    @Override
    Integer transferLock(final Resource resource,
                         final LockType type,
                         final String owner,
                         final String threadId,
                         final int seqNumber,
                         final String oldOwner,
                         final String oldThreadId) {
        transferLockNode(resource, type, seqNumber, owner, threadId, oldOwner, oldThreadId);

        // Wait until seqNumber is the smallest numbered node among children of resource node
        waitUntilLockAcquired(resource, type, seqNumber);

        return seqNumber;
    }

    private int createLockNode(Resource resource, LockType type, String owner, String threadId, TaskData taskData) {
        LockData lockData = new LockData(owner, threadId, taskData.serialize());
        try {
            String path = client.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(getLockPath(resource, type), lockData.serialize());
            return getSeqNumber(path);
        } catch (Exception e) {
            throw new LockFailedException(resource.toString(), e);
        }
    }

    private void transferLockNode(Resource resource, LockType type, int seqNumber, String owner, String threadId, String oldOwner, String oldThreadId) {
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
            if (lockData.isOwnedBy(oldOwner, oldThreadId)) {
                lockData = new LockData(owner, threadId, lockData.getTaskData());

                client.setData().withVersion(stat.getVersion())
                        .forPath(lockPath, lockData.serialize());
            } else {
                throw new LockFailedException(
                        String.format("Failed transferring lock on resource %s, as it is not owned by (%s,%s)",
                        resource.getString(), oldOwner, oldThreadId));
            }
        } catch (Exception e) {
            throw new LockFailedException(resource.getString(), e);
        }
    }

    /**
     * Waits until the lock represented by the specified sequence number is acquired.
     *
     * @param resource  resource.
     * @param seqNumber sequence number.
     */
    private void waitUntilLockAcquired(Resource resource, LockType type, int seqNumber) {
        String resourcePath = getTaskPath(resource);
        while (true) {
            CompletableFuture<Void> resourceChanged = new CompletableFuture<>();
            List<String> children = getChildrenAndWatch(resourcePath, resourceChanged);
            if (isLockAcquired(type, seqNumber, children)) {
                break;
            } else {
                resourceChanged.join();
            }
        }
    }

    /**
     * Fetch children of a given resourcePath and add a watch that completes resourceChanged future whenever
     * children of resourcePath change.
     *
     * @param resourcePath resource path.
     * @param resourceChanged future that completes when resourcePath changes.
     * @return Children of the given resource path.
     */
    private List<String> getChildrenAndWatch(String resourcePath, CompletableFuture<Void> resourceChanged) {
        try {
            return client.getChildren()
                    .usingWatcher((Watcher) event -> resourceChanged.complete(null))
                    .forPath(resourcePath);
        } catch (Exception e) {
            log.warn("Error fetching children of {}, {}", resourcePath, e.getMessage());
            resourceChanged.complete(null);
            return Collections.emptyList();
        }
    }

    /**
     * For write lock:
     *     if the given seqNumber is the smallest among sequence numbers of children return true, else false.
     * For read lock:
     *     if the none of the write lock children have sequence number smaller than the given sequence number return
     *     true, else false.
     *
     * @param children list of children.
     * @param seqNumber sequence number.
     * @return boolean indicating whether the lock has been acquired.
     */
    private boolean isLockAcquired(LockType type, int seqNumber, List<String> children) {
        if (children == null || children.isEmpty()) {
            return false;
        }
        if (type == LockType.READ) {
            return children.stream().allMatch(child -> !isWriteLock(child) || getSeqNumber(child) > seqNumber);
        } else {
            return children.stream().allMatch(child -> getSeqNumber(child) >= seqNumber);
        }
    }

    @Override
    Void removeLock(final Resource resource, final LockType type, final int seqNumber, final String owner, final String tag) {
        String lockPath = getLockPath(resource, type, seqNumber);

        try {
            // test and delete implementation
            Stat stat = new Stat();
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
                    log.warn(String.format("Lock not owned by owner %s: thread %s", owner, tag));
                    throw new UnlockFailedException(resource.getString());
                }
                // finally delete the resource node if it is empty
                deleteEmptyResource(resource);
            } else {
                log.warn("Corrupt lock data for resource {} and lock node {}", resource.getString(), lockPath);
                // But clean up the lock node, so as to prevent the resource from being blocked for read/writes forever.
                client.delete()
                        .guaranteed()
                        .withVersion(stat.getVersion())
                        .forPath(getTaskPath(resource));
            }
            return null;

        } catch (KeeperException.NoNodeException e) {
            log.debug("Lock not present on resource {}", resource);
            return null;
        } catch (Exception e) {
            throw new UnlockFailedException(resource.getString(), e);
        }
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
        return String.format("%s/%s%010d", getTaskPath(resource), type.toString(), seqNumber);
    }

    private String getLockPath(Resource resource, LockType type) {
        return String.format("%s/%s", getTaskPath(resource), type.toString());
    }

    private boolean isWriteLock(String lockNode) {
        return lockNode.startsWith(WRITE_PREFIX);
    }

    private int getSeqNumber(String lockNode) {
        return Integer.parseInt(lockNode.substring(lockNode.length() - 10));
    }
}
