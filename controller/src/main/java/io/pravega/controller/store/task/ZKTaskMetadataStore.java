/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.task;

import io.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Zookeeper based task store.
 */
@Slf4j
class ZKTaskMetadataStore extends AbstractTaskMetadataStore {

    private final static String TAG_SEPARATOR = "_%%%_";
    private final static String RESOURCE_PART_SEPARATOR = "_%_";
    private final CuratorFramework client;
    private final String hostRoot = "/hostIndex";
    private final String taskRoot = "/taskIndex";

    ZKTaskMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        super(executor);
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
            if (lockData.isOwnedBy(oldOwner, oldThreadId)) {
                lockData = new LockData(owner, threadId, lockData.getTaskData());

                client.setData().withVersion(stat.getVersion())
                        .forPath(getTaskPath(resource), lockData.serialize());
                lockAcquired = true;
            }
        } catch (Exception e) {
            throw new LockFailedException(resource.getString(), e);
        }

        if (lockAcquired) {
            return null;
        } else {
            throw new LockFailedException(resource.getString());
        }
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
                    if (lockData.isOwnedBy(owner, tag)) {
                        return Optional.of(TaskData.deserialize(lockData.getTaskData()));
                    } else {
                        log.debug("Resource {} not owned by pair ({}, {})", resource.getString(), owner, tag);
                        return Optional.empty();
                    }
                }

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

    //    @Override
    //    public CompletableFuture<Void> removeChildren(String parent, List<TaggedResource> children, boolean deleteEmptyParent) {
    //        Preconditions.checkNotNull(parent);
    //        Preconditions.checkNotNull(children);
    //
    //        return CompletableFuture.supplyAsync(() -> {
    //            try {
    //
    //                for (TaggedResource child : children) {
    //                    client.delete()
    //                            .forPath(getHostPath(parent, child));
    //                }
    //
    //                if (deleteEmptyParent) {
    //                    // if there are no children for the parent, remove parent znode
    //                    Stat stat = new Stat();
    //                    client.getData()
    //                            .storingStatIn(stat)
    //                            .forPath(getHostPath(parent));
    //
    //                    if (stat.getNumChildren() == 0) {
    //                        client.delete()
    //                                .withVersion(stat.getVersion())
    //                                .forPath(getHostPath(parent));
    //                    }
    //                }
    //                return null;
    //            } catch (KeeperException.NoNodeException e) {
    //                log.debug("Node does not exist.", e);
    //                return null;
    //            } catch (Exception e) {
    //                throw new RuntimeException(e);
    //            }
    //        });
    //    }

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

    //    @Override
    //    public CompletableFuture<List<TaggedResource>> getChildren(String parent) {
    //        Preconditions.checkNotNull(parent);
    //
    //        return CompletableFuture.supplyAsync(() -> {
    //            try {
    //
    //                return client.getChildren().forPath(getHostPath(parent))
    //                        .stream()
    //                        .map(this::getTaggedResource)
    //                        .collect(Collectors.toList());
    //
    //            } catch (KeeperException.NoNodeException e) {
    //                log.debug("Node does not exist.", e);
    //                return Collections.emptyList();
    //            } catch (Exception e) {
    //                throw new RuntimeException(e);
    //            }
    //        });
    //    }

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

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<String> children = client.getChildren().forPath(hostRoot);
                return children.stream().collect(Collectors.toSet());
            } catch (KeeperException.NoNodeException e) {
                return Collections.emptySet();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    private String getTaskPath(final Resource resource) {
        return taskRoot + "/" + getNode(resource);
    }

    private String getHostPath(final String hostId, final TaggedResource resource) {
        return hostRoot + "/" + hostId + "/" + getNode(resource);
    }

    private String getHostPath(final String hostId) {
        return hostRoot + "/" + hostId;
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
}
