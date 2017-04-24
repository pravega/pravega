/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.task;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract task metadata store.
 */
public abstract class AbstractTaskMetadataStore implements TaskMetadataStore {

    private final static String WRITE_PREFIX = LockType.WRITE.toString();
    protected final ScheduledExecutorService executor;

    AbstractTaskMetadataStore(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Integer> lock(final Resource resource,
                                           final LockType type,
                                           final TaskData taskData,
                                           final LockOwner lockOwner,
                                           final Optional<Integer> seqNumber,
                                           final Optional<LockOwner> oldLockOwner) {
        Preconditions.checkNotNull(resource);
        Preconditions.checkNotNull(taskData);
        Preconditions.checkNotNull(lockOwner);
        // oldLockOwner.isPresent <==> seqNumber.isPresent
        Preconditions.checkArgument((oldLockOwner.isPresent() && seqNumber.isPresent()) ||
                        (!seqNumber.isPresent() && !oldLockOwner.isPresent()),
                "seqNumber should be present iff oldOwner is present");

        if (!oldLockOwner.isPresent()) {
            return acquireLock(resource, type, taskData, lockOwner);
        } else {
            return transferLock(resource, type, lockOwner, seqNumber.get(), oldLockOwner.get())
                    .thenApply(ignore -> seqNumber.get());
        }
    }

    @Override
    public CompletableFuture<Void> unlock(final Resource resource,
                                          final LockType type,
                                          final int seqNumber,
                                          final LockOwner lockOwner) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(lockOwner);
            return removeLock(resource, type, seqNumber, lockOwner);
        }, executor);
    }

    private CompletableFuture<Integer> acquireLock(final Resource resource,
                                                   final LockType type,
                                                   final TaskData taskData,
                                                   final LockOwner lockOwner) {
        // Create a sequential persistent node with LockData(owner, tagId, taskData) as data
        return createLockNode(resource, type, lockOwner, taskData).thenComposeAsync(seqNumber ->
                // Wait until seqNumber is the smallest numbered node among children of resource node
                waitUntilLockAcquired(resource, type, seqNumber).thenApply(ignore -> seqNumber), this.executor);
    }

    private CompletableFuture<Void> transferLock(final Resource resource,
                                                 final LockType type,
                                                 final LockOwner lockOwner,
                                                 final int seqNumber,
                                                 final LockOwner oldLockOwner) {
        // Transfer the lock first.
        return transferLockNode(resource, type, seqNumber, lockOwner, oldLockOwner).thenComposeAsync(x ->
                // Wait until seqNumber is the smallest numbered node among children of resource node
                waitUntilLockAcquired(resource, type, seqNumber), this.executor);
    }

    /**
     * Waits until the lock represented by the specified sequence number and specified type is acquired.
     *
     * @param resource  resource.
     * @param type      lock type.
     * @param seqNumber sequence number.
     */
    private CompletableFuture<Void> waitUntilLockAcquired(final Resource resource,
                                                          final LockType type,
                                                          final int seqNumber) {
        AtomicReference<Boolean> lockAcquired = new AtomicReference<>(false);
        return FutureHelpers.loop(
                () -> !lockAcquired.get(),
                () -> checkIfLockAcquired(resource, type, seqNumber, lockAcquired),
                this.executor);

    }

    abstract CompletableFuture<Integer> createLockNode(final Resource resource,
                                                       final LockType type,
                                                       final LockOwner lockOwner,
                                                       final TaskData taskData);

    abstract CompletableFuture<Void> transferLockNode(final Resource resource,
                                                      final LockType type,
                                                      final int seqNumber,
                                                      final LockOwner lockOwner,
                                                      final LockOwner oldLockOwner);

    /**
     * Fetch the children of resourcePath and check whether the lock of given type and
     * having given sequence number is acquired.
     * If it is acquired, it sets lockAcquired to true and returns a completed future.
     * Otherwise, it returns a future that completes when the resourcePath node changes.
     *
     * @param resource     resource.
     * @param type         lock type.
     * @param seqNumber    sequence number.
     * @param lockAcquired atomic reference set to true when lock is acquired.
     * @return if lock is acquired, returns a completed future, otherwise returns a
     * future that completes when node resourcePath changes.
     */
    abstract CompletableFuture<Void> checkIfLockAcquired(final Resource resource,
                                                         final LockType type,
                                                         final int seqNumber,
                                                         final AtomicReference<Boolean> lockAcquired);

    abstract Void removeLock(final Resource resource,
                             final LockType type,
                             final int seqNumber,
                             final LockOwner lockOwner);

    /**
     * For write lock:
     * if the given seqNumber is the smallest among sequence numbers of children return true, else false.
     * For read lock:
     * if the none of the write lock children have sequence number smaller than the given sequence number return
     * true, else false.
     *
     * @param children  list of children.
     * @param seqNumber sequence number.
     * @return boolean indicating whether the lock has been acquired.
     */
    static boolean isLockAcquired(LockType type, int seqNumber, Collection<String> children) {
        boolean lockNodePresent = children.contains(getLockNodeName(type, seqNumber));
        if (type == LockType.READ) {
            return lockNodePresent &&
                    children.stream().allMatch(child -> !isWriteLock(child) || getSeqNumber(child) > seqNumber);
        } else {
            return lockNodePresent &&
                    children.stream().allMatch(child -> getSeqNumber(child) >= seqNumber);
        }
    }

    static String getLockNodeName(LockType type, int seqNumber) {
        return String.format("%s%010d", type.toString(), seqNumber);
    }

    static boolean isWriteLock(String lockNode) {
        return lockNode.startsWith(WRITE_PREFIX);
    }

    static int getSeqNumber(String lockNode) {
        return Integer.parseInt(lockNode.substring(lockNode.length() - 10));
    }
}
