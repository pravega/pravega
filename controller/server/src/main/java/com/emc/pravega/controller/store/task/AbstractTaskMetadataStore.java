/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;

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
    public CompletableFuture<Void> lock(final Resource resource,
                                                     final TaskData taskData,
                                                     final String owner,
                                                     final String tag,
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

            if (oldOwner == null) {
                return acquireLock(resource, taskData, owner, tag);
            } else {
                return transferLock(resource, owner, tag, oldOwner, oldTag);
            }

        }, executor);

    }

    @Override
    public CompletableFuture<Void> unlock(final Resource resource,
                                                       final String owner,
                                                       final String tag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(owner);
            Preconditions.checkNotNull(tag);

            return removeLock(resource, owner, tag);

        }, executor);
    }

    abstract Void acquireLock(final Resource resource,
                              final TaskData taskData,
                              final String owner,
                              final String threadId);

    abstract Void transferLock(final Resource resource,
                              final String owner,
                              final String threadId,
                              final String oldOwner,
                              final String oldThreadId);

    abstract Void removeLock(final Resource resource, final String owner, final String tag);
}
