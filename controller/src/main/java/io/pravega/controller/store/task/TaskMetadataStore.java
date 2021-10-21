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

import io.pravega.controller.task.TaskData;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Task metadata store.
 */
public interface TaskMetadataStore {

    /**
     * Locks a resource for update.
     * If (oldOwner, oldTag) are specified then it revokes old owner's lock and itself acquires it.
     * This is non-reentrant lock, i.e., a process/thread cannot lock the same resource twice.
     * If oldOwner is null then
     * atomically create the key value pair resource -{@literal >} (owner, tag, taskData) if it does not exist.
     * If oldOwner is non-null
     * then atomically replace the key value pair resource -{@literal >} (oldOwner, oldTag, taskData) with the pair
     * resource -{@literal >} (owner, tag, taskData).
     *
     * @param resource    resource identifier.
     * @param taskData    details of update task on the resource.
     * @param owner       owner of the task.
     * @param tag         tag.
     * @param oldOwner    host that had previously locked the resource.
     * @param oldTag tag that took the lock
     * @return void if the operation succeeds, otherwise throws LockFailedException.
     */
    CompletableFuture<Void> lock(final Resource resource,
                                 final TaskData taskData,
                                 final String owner,
                                 final String tag,
                                 final String oldOwner,
                                 final String oldTag);

    /**
     * Unlocks a resource if it is owned by the specified owner.
     * Delete the key value pair resource -{@literal >} (x, taskData) iff x == owner.
     *
     * @param resource resource identifier.
     * @param owner    owner of the lock.
     * @param tag tag.
     * @return void if successful, otherwise throws UnlockFailedException.
     */
    CompletableFuture<Void> unlock(final Resource resource, final String owner, final String tag);

    /**
     * Fetch details of task associated with the specified resource and locked/owned by specified owner and tag.
     *
     * @param resource resource.
     * @param owner    owner.
     * @param tag tag.
     * @return TaskData if owner and tag hold a lock on the specified resource otherwise Optional.empty().
     */
    CompletableFuture<Optional<TaskData>> getTask(final Resource resource, final String owner, final String tag);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * This is idempotent operation.
     *
     * @param parent Parent node.
     * @param child  TaggedResource node to be added as child of parent.
     * @return void in future.
     */
    CompletableFuture<Void> putChild(final String parent, final TaggedResource child);

    /**
     * Removes the specified child node from the specified parent node.
     * This is idempotent operation.
     * If deleteEmptyParent is true and parent has no child after deletion of given child then parent is also deleted.
     *
     * @param parent            node whose child is to be removed.
     * @param child             child TaggedResource node to remove.
     * @param deleteEmptyParent to delete or not to delete.
     * @return void in future.
     */
    CompletableFuture<Void> removeChild(final String parent, final TaggedResource child, final boolean deleteEmptyParent);

    /**
     * Remove a parent node if it is empty.
     * This is idempotent operation.
     *
     * @param parent parent node.
     * @return void in future.
     */
    CompletableFuture<Void> removeNode(final String parent);

    /**
     * Returns a random child from among the children of specified parent.
     *
     * @param parent parent node.
     * @return A randomly selected child if parent has children, otherwise Optional.empty().
     */
    CompletableFuture<Optional<TaggedResource>> getRandomChild(final String parent);

    /**
     * Returns the list of hosts performing some task. This list is obtained from the hostIndex.
     *
     * @return the list of hosts performing some task.
     */
    CompletableFuture<Set<String>> getHosts();
}
