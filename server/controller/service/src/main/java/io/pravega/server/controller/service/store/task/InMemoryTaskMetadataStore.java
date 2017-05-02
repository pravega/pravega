/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.task;

import io.pravega.server.controller.service.task.TaskData;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.Collections;
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

    @Override
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

    @Override
    Void removeLock(final Resource resource, final String owner, final String tag) {

        // test and set implementation
        // Read the existing lock data, if it is owned by (oldOwner, oldThreadId) change it and transfer lock.
        // Otherwise fail.
        synchronized (lockTable) {
            if (!lockTable.containsKey(resource)) {
                log.debug("removeLock on {} completed; resource was not locked", resource);
                return null;
            }
            LockData lockData = lockTable.get(resource);

            if (lockData != null && lockData.isOwnedBy(owner, tag)) {
                lockTable.remove(resource);
                return null;
            } else {
                throw new UnlockFailedException(resource.getString());
            }
        }
    }

    @Override
    public synchronized CompletableFuture<Optional<TaskData>> getTask(final Resource resource,
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
                        return Optional.of(TaskData.deserialize(lockData.getTaskData()));
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

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return CompletableFuture.completedFuture(Collections.unmodifiableSet(hostTable.keySet()));
    }
}
