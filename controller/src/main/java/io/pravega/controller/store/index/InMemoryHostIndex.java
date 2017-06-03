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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * In-memory implementation of HostIndex.
 */
public class InMemoryHostIndex implements HostIndex {
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<String, byte[]>> hostTable;

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
            hostTable.get(hostId).put(entity, entityData);
        } else {
            ConcurrentSkipListMap<String, byte[]> children = new ConcurrentSkipListMap<>();
            children.put(entity, entityData);
            hostTable.put(hostId, children);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<byte[]> getEntityData(String hostId, String entity) {
        ConcurrentSkipListMap<String, byte[]> value = hostTable.get(hostId);
        if (value != null) {
            return CompletableFuture.completedFuture(value.get(entity));
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
            ConcurrentSkipListMap<String, byte[]> value = hostTable.get(hostId);
            value.remove(entity);
            if (deleteEmptyHost && value.size() == 0) {
                hostTable.remove(hostId);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeHost(final String hostId) {
        Preconditions.checkNotNull(hostId);
        hostTable.remove(hostId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<String>> getEntities(final String hostId) {
        Preconditions.checkNotNull(hostId);
        ConcurrentSkipListMap<String, byte[]> children = hostTable.get(hostId);
        if (children == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            return CompletableFuture.completedFuture(children.keySet().stream().collect(Collectors.toList()));
        }
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return CompletableFuture.completedFuture(Collections.unmodifiableSet(hostTable.keySet()));
    }
}
