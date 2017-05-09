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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * In-memory implementation of HostIndex.
 */
public class InMemoryHostIndex implements HostIndex {
    private final ConcurrentHashMap<String, ConcurrentSkipListSet<Pair<String, byte[]>>> hostTable;

    public InMemoryHostIndex() {
        hostTable = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Void> addEntity(final String hostId, final String entity) {
        return addEntity(hostId, entity, new byte[0]);
    }

    @Override
    public CompletableFuture<Void> addEntity(String hostId, String entity, byte[] entityData) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        if (hostTable.containsKey(hostId)) {
            hostTable.get(hostId).add(new ImmutablePair<>(entity, entityData));
        } else {
            ConcurrentSkipListSet<Pair<String, byte[]>> children = new ConcurrentSkipListSet<>();
            children.add(new ImmutablePair<>(entity, entityData));
            hostTable.put(hostId, children);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeEntity(final String hostId,
                                                final String entity,
                                                final boolean deleteEmptyHost) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        if (hostTable.containsKey(hostId)) {
            Set<Pair<String, byte[]>> taggedResources = hostTable.get(hostId);
            Optional<Pair<String, byte[]>> resource =
                    taggedResources.stream().filter(pair -> pair.getKey().equals(entity)).findFirst();
            if (resource.isPresent()) {
                if (deleteEmptyHost && taggedResources.size() == 1) {
                    hostTable.remove(hostId);
                } else {
                    taggedResources.remove(resource.get());
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeHost(final String hostId) {
        Preconditions.checkNotNull(hostId);
        hostTable.remove(hostId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Optional<String>> getRandomEntity(final String hostId) {
        Preconditions.checkNotNull(hostId);
        Set<Pair<String, byte[]>> children = hostTable.get(hostId);
        if (children == null) {
            return CompletableFuture.completedFuture(Optional.empty());
        } else {
            return CompletableFuture.completedFuture(children.stream().findAny().map(Pair::getKey));
        }
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return CompletableFuture.completedFuture(Collections.unmodifiableSet(hostTable.keySet()));
    }
}
