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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Task metadata store.
 */
public interface TaskMetadataStore {

    /**
     * Locks a resource for update. If old owner is specified, it revokes old owner's lock and itself acquires it.
     * This is non-reentrant lock, i.e., a process cannot lock the same resource twice.
     * If oldOwner is null atomically create the key value pair resource -> (owner, taskData) if it does not exist.
     * if oldOwner is non-null atomically replace the key value pair resource -> (oldOwner, taskData) with the pair
     * resource -> (owner, taskData).
     * @param resource resource identifier.
     * @param taskData details of update task on the resource.
     * @param owner    owner of the task.
     * @param oldOwner host that had previously locked the resource.
     * @return void if the operation succeeds, otherwise throws LockFailedException.
     */
    CompletableFuture<Void> lock(String resource, TaskData taskData, String owner, String oldOwner);

    /**
     * Unlocks a resource if it is owned by the specified owner.
     * Delete the key value pair resource -> (x, taskData) iff x == owner.
     * @param resource resource identifier.
     * @param owner    owner of the lock.
     * @return void if successful, otherwise throws UnlockFailedException.
     */
    CompletableFuture<Void> unlock(String resource, String owner);

    /**
     * Fetch details of task, including its current owner, associated with the specified resource.
     * @param resource node.
     * @return byte array in future.
     */
    CompletableFuture<byte[]> get(String resource);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * @param parent parent node.
     * @param child child noe.
     * @return void in future.
     */
    CompletableFuture<Void> putChild(String parent, String child);

    /**
     * Removes the specified child node from the specified parent node.
     * @param parent node whose child is to be removed.
     * @param child child node to remove.
     * @return void in future.
     */
    CompletableFuture<Void> removeChild(String parent, String child, boolean deleteEmptyParent);

    /**
     * Remove a parent node if it is empty.
     * @param parent parent node.
     * @return void in future.
     */
    CompletableFuture<Void> removeNode(String parent);

    /**
     * Returns all children of a given parent node.
     * @param parent host id.
     * @return children list.
     */
    CompletableFuture<List<String>> getChildren(String parent);
}
