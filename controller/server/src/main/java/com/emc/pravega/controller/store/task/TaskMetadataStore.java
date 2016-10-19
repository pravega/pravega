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
     * @param resource resource identifier.
     * @param oldOwner host that had previously locked the resource.
     * @return void if the operation succeeds, otherwise throws LockFailedException.
     */
    CompletableFuture<Void> lock(String resource, TaskData taskData, String oldOwner);

    /**
     * Unlocks a resource if it is owned by the current process.
     * @param resource resource identifier.
     * @return void if successful, otherwise throws UnlockFailedException.
     */
    CompletableFuture<Void> unlock(String resource);

    /**
     * Fetch data associated with the specified resource.
     * @param resource resource.
     * @return byte array in future.
     */
    CompletableFuture<byte[]> get(String resource);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * @param resource resource.
     * @return void in future.
     */
    CompletableFuture<Void> putChild(String resource);

    /**
     * Removes the specified child of current host's hostId node.
     * @param resource child node to remove.
     * @return void in future.
     */
    CompletableFuture<Void> removeChild(String resource);

    /**
     * Removes specified child of the specified failed host.
     * It also removes the host's node if it is left with no children as a result.
     * @param failedHostId failed host id.
     * @param resource child node to remove.
     * @return void in future.
     */
    CompletableFuture<Void> removeChild(String failedHostId, String resource);

    /**
     * Returns all children of a given hostId node.
     * @param hostId host id.
     * @return children list.
     */
    CompletableFuture<List<String>> getChildren(String hostId);
}
