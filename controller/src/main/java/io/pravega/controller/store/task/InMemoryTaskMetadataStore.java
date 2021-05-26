/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.task;

import io.pravega.controller.store.index.InMemoryHostIndex;
import io.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In-memory task metadata store.
 */
@Slf4j
class InMemoryTaskMetadataStore extends AbstractTaskMetadataStore {

    @GuardedBy("lockTable")
    private final Map<Resource, LockData> lockTable;

    InMemoryTaskMetadataStore(ScheduledExecutorService executor) {
        super(new InMemoryHostIndex(), executor);
        lockTable = new HashMap<>();
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
    public CompletableFuture<Optional<TaskData>> getTask(final Resource resource,
                                                         final String owner,
                                                         final String tag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(owner);
            Preconditions.checkNotNull(tag);

            synchronized (lockTable) {
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
            }
        }, executor);
    }
}
