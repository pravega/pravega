/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.task;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-memory task metadata store.
 */
@Slf4j
class InMemoryTaskMetadataStore extends AbstractTaskMetadataStore {

    private static class Directory {
        private int nextSeqNumnber = 0;
        private final Map<String, LockData> lockNodes = new HashMap<>();
        private final List<CompletableFuture<Void>> listeners = new ArrayList<>();

        public int add(LockType type, LockData lockData) {
            synchronized (lockNodes) {
                String lockNodeName = getLockNodeName(type, nextSeqNumnber);
                this.lockNodes.put(lockNodeName, lockData);
                nextSeqNumnber++;
                return nextSeqNumnber - 1;
            }
        }

        public void remove(String lockNodeName) {
            synchronized (lockNodes) {
                lockNodes.remove(lockNodeName);
                listeners.forEach(listener -> {
                    if (!listener.isDone()) {
                        listener.complete(null);
                    }
                });
                listeners.clear();
            }
        }

        public void put(String lockNodeName, LockData lockData) {
            synchronized (lockNodes) {
                lockNodes.put(lockNodeName, lockData);
            }
        }

        public LockData get(String lockNodeName) {
            return lockNodes.get(lockNodeName);
        }

        public Collection<String> getLockNodes(CompletableFuture<Void> listener) {
            synchronized (lockNodes) {
                listeners.add(listener);
                return this.lockNodes.keySet();
            }
        }

        public boolean isLockHeld(LockType type, int seqNumber) {
            return isLockAcquired(type, seqNumber, lockNodes.keySet());
        }

        public Optional<Pair<TaskData, Integer>> getTask(LockOwner lockOwner) {
            for (Map.Entry<String, LockData> entry : lockNodes.entrySet()) {
                LockData lockData = entry.getValue();
                if (lockData.isOwnedBy(lockOwner)) {
                    TaskData taskData = TaskData.deserialize(lockData.getTaskData());
                    return Optional.of(new ImmutablePair<>(taskData, getSeqNumber(entry.getKey())));
                }
            }
            return Optional.empty();
        }
    }

    @GuardedBy("itself")
    private final Map<Resource, Directory> lockTable;

    @GuardedBy("itself")
    private final Map<String, Set<TaggedResource>> hostTable;

    InMemoryTaskMetadataStore(ScheduledExecutorService executor) {
        super(executor);
        lockTable = new ConcurrentHashMap<>();
        hostTable = new HashMap<>();
    }

    @Override
    CompletableFuture<Integer> createLockNode(Resource resource, LockType type, LockOwner lockOwner, TaskData taskData) {
        synchronized (lockTable) {
            LockData lockData = new LockData(lockOwner, taskData);
            Directory directory;
            if (lockTable.containsKey(resource)) {
                directory = lockTable.get(resource);
            } else {
                directory = new Directory();
                lockTable.put(resource, directory);
            }
            int seqNumber = directory.add(type, lockData);
            return CompletableFuture.completedFuture(seqNumber);
        }
    }

    @Override
    CompletableFuture<Void> transferLockNode(Resource resource, LockType type, int seqNumber, LockOwner lockOwner,
                                             LockOwner oldLockOwner) {
        synchronized (lockTable) {
            if (!lockTable.containsKey(resource)) {
                return FutureHelpers.failedFuture(new LockFailedException(resource.getString()));
            }

            String lockNodeName = getLockNodeName(type, seqNumber);
            LockData lockData = lockTable.get(resource).get(lockNodeName);

            if (lockData == null || !lockData.isOwnedBy(oldLockOwner)) {
                return FutureHelpers.failedFuture(new LockFailedException(resource.getString()));
            }

            LockData newLockData = new LockData(lockOwner, lockData.getTaskData());
            lockTable.get(resource).put(lockNodeName, newLockData);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    CompletableFuture<Void> checkIfLockAcquired(Resource resource, LockType type, int seqNumber, AtomicReference<Boolean> lockAcquired) {
        CompletableFuture<Void> resourceChanged = new CompletableFuture<>();
        Collection<String> children = lockTable.get(resource).getLockNodes(resourceChanged);
        if (isLockAcquired(type, seqNumber, children)) {
            lockAcquired.set(true);
            resourceChanged.complete(null);
        }
        return resourceChanged;
    }

    @Override
    Void removeLock(Resource resource, LockType type, int seqNumber, LockOwner lockOwner) {
        synchronized (lockTable) {
            Directory directory = lockTable.get(resource);
            if (!directory.isLockHeld(type, seqNumber)) {
                String errorMsg = String.format("Lock node %s does not hold the lock on resource %s",
                        getLockNodeName(type, seqNumber), resource.getString());
                log.warn(errorMsg);
                throw new UnlockFailedException(errorMsg);
            }

            String lockNodeName = getLockNodeName(type, seqNumber);
            LockData lockData = lockTable.get(resource).get(lockNodeName);

            if (lockData != null && lockData.isOwnedBy(lockOwner)) {
                directory.remove(lockNodeName);
            } else {
                String errorMsg = String.format("Lock %s on resource %s not owned by owner %s: tag %s",
                        getLockNodeName(type, seqNumber), resource.getString(), lockOwner.getHost(), lockOwner.getTag());
                log.warn(errorMsg);
                throw new UnlockFailedException(errorMsg);
            }
            return null;
        }
    }

    public synchronized CompletableFuture<Optional<Pair<TaskData, Integer>>> getTask(final Resource resource,
                                                                                     final LockOwner lockOwner) {
        synchronized (lockTable) {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(lockOwner);

            Directory directory = lockTable.get(resource);

            if (directory == null) {
                return CompletableFuture.completedFuture(Optional.empty());
            } else {
                return CompletableFuture.completedFuture(directory.getTask(lockOwner));
            }
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
