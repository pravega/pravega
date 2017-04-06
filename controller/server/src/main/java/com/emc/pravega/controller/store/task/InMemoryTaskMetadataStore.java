/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In-memory task metadata store.
 */
@Slf4j
class InMemoryTaskMetadataStore extends AbstractTaskMetadataStore {

    @GuardedBy("itself")
    private final Map<Resource, LockData> lockTable;

    @GuardedBy("itself")
    private final Map<String, Set<TaggedResource>> hostTable;

    InMemoryTaskMetadataStore(ScheduledExecutorService executor) {
        super(executor);
        lockTable = new HashMap<>();
        hostTable = new HashMap<>();
    }

    @Override
    Integer acquireLock(Resource resource, LockType type, TaskData taskData, String owner, String threadId) {
        // todo: We will implement this once ZK based implementation in ZKTaskMetadataStore is reviewed.
        throw new NotImplementedException();
    }

    @Override
    Integer transferLock(Resource resource, LockType type, String owner, String threadId, int seqNumber, String oldOwner, String oldThreadId) {
        throw new NotImplementedException();
    }

    @Override
    Void removeLock(Resource resource, LockType type, int seqNumber, String owner, String tag) {
        throw new NotImplementedException();
    }

    Void acquireLock(final Resource resource,
                             final TaskData taskData,
                             final String owner,
                             final String threadId) {
        // test and set implementation
        // for fresh lock, create the node and write its data.
        // if the node successfully got created, locking has succeeded,
        // else locking has failed.
        synchronized (lockTable) {
            if (!lockTable.containsKey(resource)) {
                LockData lockData = new LockData(owner, threadId, taskData.serialize());
                lockTable.put(resource, lockData);
                return null;
            } else {
                throw new LockFailedException(resource.getString());
            }
        }
    }

    Void transferLock(final Resource resource,
                              final String owner,
                              final String threadId,
                              final String oldOwner,
                              final String oldThreadId) {

        // test and set implementation
        // Read the existing lock data, if it is owned by (oldOwner, oldThreadId) change it and transfer lock.
        // Otherwise fail..
        synchronized (lockTable) {
            LockData lockData = lockTable.get(resource);

            if (lockData != null && lockData.isOwnedBy(oldOwner, oldThreadId)) {
                LockData newLockData = new LockData(owner, threadId, lockData.getTaskData());
                lockTable.put(resource, newLockData);
                return null;
            } else {
                throw new LockFailedException(resource.getString());
            }
        }
    }

    Void removeLock(final Resource resource, final String owner, final String tag) {

        // test and set implementation
        // Read the existing lock data, if it is owned by (oldOwner, oldThreadId) change it and transfer lock.
        // Otherwise fail.
        synchronized (lockTable) {
            LockData lockData = lockTable.get(resource);

            if (lockData != null && lockData.isOwnedBy(owner, tag)) {
                lockTable.remove(resource);
                return null;
            } else {
                throw new UnlockFailedException(resource.getString());
            }
        }
    }

    public synchronized CompletableFuture<Optional<Pair<TaskData, Integer>>> getTask(final Resource resource,
                                                         final String owner,
                                                         final String tag) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(resource);
                Preconditions.checkNotNull(owner);
                Preconditions.checkNotNull(tag);

                LockData lockData = lockTable.get(resource);

                if (lockData == null) {
                    return Optional.empty();
                } else {
                    if (lockData.isOwnedBy(owner, tag)) {
                        return Optional.of(new ImmutablePair<>(TaskData.deserialize(lockData.getTaskData()), 10));
                    } else {
                        log.debug(String.format("Resource %s not owned by (%s, %s)", resource.getString(), owner, tag));
                        return Optional.empty();
                    }
                }
            }, executor);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> putChild(final String parent, final TaggedResource child) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);
                Preconditions.checkNotNull(child);

                if (hostTable.containsKey(parent)) {
                    hostTable.get(parent).add(child);
                } else {
                    Set<TaggedResource> taggedResources = new HashSet<>();
                    taggedResources.add(child);
                    hostTable.put(parent, taggedResources);
                }
                return null;
            }, executor);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> removeChild(final String parent,
                                               final TaggedResource child,
                                               final boolean deleteEmptyParent) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);
                Preconditions.checkNotNull(child);

                if (hostTable.containsKey(parent)) {
                    Set<TaggedResource> taggedResources = hostTable.get(parent);
                    if (taggedResources.contains(child)) {
                        if (deleteEmptyParent && taggedResources.size() == 1) {
                            hostTable.remove(parent);
                        } else {
                            taggedResources.remove(child);
                        }
                    }
                }
                return null;
            }, executor);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> removeNode(final String parent) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);

                hostTable.remove(parent);
                return null;

            }, executor);
        }
    }

    @Override
    public synchronized CompletableFuture<Optional<TaggedResource>> getRandomChild(final String parent) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);

                Set<TaggedResource> taggedResources = hostTable.get(parent);
                if (taggedResources == null) {
                    return Optional.empty();
                } else {
                    return taggedResources.stream().findAny();
                }

            }, executor);
        }
    }
}
