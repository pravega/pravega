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
package io.pravega.controller.store.index;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Host Index provides a mechanism for tracking a bunch of entities managed by hosts in the cluster. Entities can
 * be added and subsequently removed from the host's index. On host failure, ownership of orphaned entities can
 * be transferred to a new host.
 *
 * Host Index is a map of host to the set of entities it manages. Entities can have additional data
 * associated with them.
 */
public interface HostIndex {
    /**
     * Adds specified entity to the set of entities being managed by the specified host.
     * This is an idempotent operation.
     *
     * @param hostId Host.
     * @param entity Entity to be added to the host.
     * @return void in future.
     */
    CompletableFuture<Void> addEntity(final String hostId, final String entity);

    /**
     * Adds specified entity to the set of entities being managed by the specified host.
     * This is an idempotent operation.
     *
     * @param hostId     Host.
     * @param entity     Entity to be added to the host.
     * @param entityData Child node's data.
     * @return void in future.
     */
    CompletableFuture<Void> addEntity(final String hostId, final String entity, final byte[] entityData);

    /**
     * Fetches data for specified entity stored under the specified host in the index.
     *
     * @param hostId  Host.
     * @param entity  Entity whose data is to be retrieved.
     * @return data for specified entity stored under the specified host in the index.
     */
    CompletableFuture<byte[]> getEntityData(final String hostId, final String entity);

    /**
     * Removes the specified entity from the set of entities being managed by the specified host.
     * This is an idempotent operation. If deleteEmptyHost is true and the host has not more entities
     * to manage after removal of specified entity, then the host is removed from the index.
     *
     * @param hostId          Host to remove an entity from.
     * @param entity          Entity to remove.
     * @param deleteEmptyHost whether to delete host node if it has no children.
     * @return void in future.
     */
    CompletableFuture<Void> removeEntity(final String hostId, final String entity, final boolean deleteEmptyHost);

    /**
     * Remove an empty host. This is idempotent operation.
     *
     * @param hostId Host.
     * @return void in future.
     */
    CompletableFuture<Void> removeHost(final String hostId);

    /**
     * Returns a random entity from among the entities being managed by te specified host.
     *
     * @param hostId Host.
     * @return A randomly selected entity if the host is managing any, otherwise Optional.empty().
     */
    CompletableFuture<List<String>> getEntities(final String hostId);

    /**
     * Returns the list of hosts present in the index.
     *
     * @return the list of hosts present in the index.
     */
    CompletableFuture<Set<String>> getHosts();
}
