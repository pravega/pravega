/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.index;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Host Index.
 */
public interface HostIndex {
    /**
     * Adds specified resource as a child of current host's hostId node.
     * This is idempotent operation.
     *
     * @param parent Parent node.
     * @param child  Node to be added as child of parent.
     * @return void in future.
     */
    CompletableFuture<Void> putChild(final String parent, final String child);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * This is idempotent operation.
     *
     * @param parent Parent node.
     * @param child  Node to be added as child of parent.
     * @param data   Child node's data.
     * @return void in future.
     */
    CompletableFuture<Void> putChild(final String parent, final String child, final byte[] data);

    /**
     * Removes the specified child node from the specified parent node.
     * This is idempotent operation.
     * If deleteEmptyParent is true and parent has no child after deletion of given child then parent is also deleted.
     *
     * @param parent            node whose child is to be removed.
     * @param child             child node to remove.
     * @param deleteEmptyParent to delete or not to delete.
     * @return void in future.
     */
    CompletableFuture<Void> removeChild(final String parent, final String child, final boolean deleteEmptyParent);

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
    CompletableFuture<Optional<String>> getRandomChild(final String parent);

    /**
     * Returns the list of hosts performing some task. This list is obtained from the hostIndex.
     *
     * @return the list of hosts performing some task.
     */
    CompletableFuture<Set<String>> getHosts();
}
