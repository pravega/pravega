/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract task metadata store.
 */
public abstract class AbstractTaskMetadataStore implements TaskMetadataStore {

    protected final ScheduledExecutorService executor;

    AbstractTaskMetadataStore(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Integer> lock(final Resource resource,
                                           final LockType type,
                                           final TaskData taskData,
                                           final String owner,
                                           final String tag,
                                           final Optional<Integer> seqNumber,
                                           final String oldOwner,
                                           final String oldTag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(taskData);
            Preconditions.checkNotNull(owner);
            Preconditions.checkArgument(!owner.isEmpty());
            Preconditions.checkNotNull(tag);
            Preconditions.checkArgument(!tag.isEmpty());
            Preconditions.checkArgument((oldOwner == null && oldTag == null) || (oldOwner != null && oldTag != null));
            Preconditions.checkArgument(oldOwner == null || !oldOwner.isEmpty());
            Preconditions.checkArgument(oldTag == null || !oldTag.isEmpty());
            // oldOwner != null ==> seqNumber.isPresent
            Preconditions.checkArgument(oldOwner == null || seqNumber.isPresent(),
                    "seqNumber should be present if oldOwner is not null");

            if (oldOwner == null) {
                return acquireLock(resource, type, taskData, owner, tag);
            } else {
                return transferLock(resource, type, owner, tag, seqNumber.get(), oldOwner, oldTag);
            }

        }, executor);

    }

    @Override
    public CompletableFuture<Void> unlock(final Resource resource,
                                          final LockType type,
                                          final int seqNumber,
                                          final String owner,
                                          final String tag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(owner);
            Preconditions.checkNotNull(tag);

            return removeLock(resource, type, seqNumber, owner, tag);

        }, executor);
    }

    abstract Integer acquireLock(final Resource resource,
                                 final LockType type,
                                 final TaskData taskData,
                                 final String owner,
                                 final String threadId);

    abstract Integer transferLock(final Resource resource,
                                  final LockType type,
                                  final String owner,
                                  final String threadId,
                                  final int seqNumber,
                                  final String oldOwner,
                                  final String oldThreadId);

    abstract Void removeLock(final Resource resource, final LockType type, final int seqNumber, final String owner, final String tag);
}
