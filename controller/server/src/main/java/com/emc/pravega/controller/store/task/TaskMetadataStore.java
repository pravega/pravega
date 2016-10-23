/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.task;

import com.emc.pravega.controller.task.TaskData;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Task metadata store.
 */
public interface TaskMetadataStore {

    /**
     * Locks a resource for update.
     * If (oldOwner, oldThreadId) are specified then it revokes old owner's lock and itself acquires it.
     * This is non-reentrant lock, i.e., a process/thread cannot lock the same resource twice.
     * If oldOwner is null then
     *     atomically create the key value pair resource -> (owner, threadId, taskData) if it does not exist.
     * If oldOwner is non-null
     *     then atomically replace the key value pair resource -> (oldOwner, oldThreadId, taskData) with the pair
     * resource -> (owner, threadId, taskData).
     * @param resource resource identifier.
     * @param taskData details of update task on the resource.
     * @param owner    owner of the task.
     * @param oldOwner host that had previously locked the resource.
     * @return void if the operation succeeds, otherwise throws LockFailedException.
     */
    CompletableFuture<Void> lock(Resource resource, TaskData taskData, String owner, String threadId, String oldOwner, String oldThreadId);

    /**
     * Unlocks a resource if it is owned by the specified owner.
     * Delete the key value pair resource -> (x, taskData) iff x == owner.
     * @param resource resource identifier.
     * @param owner    owner of the lock.
     * @return void if successful, otherwise throws UnlockFailedException.
     */
    CompletableFuture<Void> unlock(Resource resource, String owner, String threadId);

    /**
     * Fetch details of task associated with the specified resource and locked/owned by specified owner and threadId.
     * @param resource resource.
     * @param owner owner.
     * @param threadId threadId.
     * @return TaskData if owner and threadId hold a lock on the specified resource otherwise Optional.empty().
     */
    CompletableFuture<Optional<TaskData>> getTask(Resource resource, String owner, String threadId);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * This is idempotent operation.
     * @param parent Parent node.
     * @param child TaggedResource node to be added as child of parent.
     * @return void in future.
     */
    CompletableFuture<Void> putChild(String parent, TaggedResource child);

    /**
     * Removes the specified child node from the specified parent node.
     * This is idempotent operation.
     * If deleteEmptyParent is true and parent has no child after deletion of given child then parent is also deleted.
     * @param parent node whose child is to be removed.
     * @param child child TaggedResource node to remove.
     * @return void in future.
     */
    CompletableFuture<Void> removeChild(String parent, TaggedResource child, boolean deleteEmptyParent);

    /**
     * Remove a parent node if it is empty.
     * This is idempotent operation.
     * @param parent parent node.
     * @return void in future.
     */
    CompletableFuture<Void> removeNode(String parent);

    /**
     * Returns a random child from among the children of specified parent.
     * @param parent parent node.
     * @return A randomly selected child if parent has children, otherwise Optional.empty().
     */
    CompletableFuture<Optional<TaggedResource>> getRandomChild(String parent);
}
