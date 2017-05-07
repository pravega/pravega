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

import javax.annotation.concurrent.GuardedBy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * In-memory implementation of HostIndex.
 */
public class InMemoryHostIndex implements HostIndex {
    @GuardedBy("itself")
    private final Map<String, Set<Pair<String, byte[]>>> hostTable;
    private Executor executor;

    public InMemoryHostIndex(Executor executor) {
        hostTable = new HashMap<>();
        this.executor = executor;
    }

    @Override
    public synchronized CompletableFuture<Void> putChild(final String parent, final String child) {
        return putChild(parent, child, new byte[0]);
    }

    @Override
    public CompletableFuture<Void> putChild(String parent, String child, byte[] data) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);
                Preconditions.checkNotNull(child);

                if (hostTable.containsKey(parent)) {
                    hostTable.get(parent).add(new ImmutablePair<>(child, data));
                } else {
                    Set<Pair<String, byte[]>> taggedResources = new HashSet<>();
                    taggedResources.add(new ImmutablePair<>(child, data));
                    hostTable.put(parent, taggedResources);
                }
                return null;
            }, executor);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> removeChild(final String parent,
                                                            final String child,
                                                            final boolean deleteEmptyParent) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);
                Preconditions.checkNotNull(child);

                if (hostTable.containsKey(parent)) {
                    Set<Pair<String, byte[]>> taggedResources = hostTable.get(parent);
                    Optional<Pair<String, byte[]>> resource =
                            taggedResources.stream().filter(pair -> pair.getKey().equals(child)).findFirst();
                    if (resource.isPresent()) {
                        if (deleteEmptyParent && taggedResources.size() == 1) {
                            hostTable.remove(parent);
                        } else {
                            taggedResources.remove(resource.get());
                        }
                    }
                }
                return null;
            }, executor);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> removeNode(final String parent) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);

                hostTable.remove(parent);
                return null;

            }, executor);
        }
    }

    @Override
    public synchronized CompletableFuture<Optional<String>> getRandomChild(final String parent) {
        synchronized (hostTable) {
            return CompletableFuture.supplyAsync(() -> {
                Preconditions.checkNotNull(parent);

                Set<Pair<String, byte[]>> children = hostTable.get(parent);
                if (children == null) {
                    return Optional.empty();
                } else {
                    return children.stream().findAny().map(Pair::getKey);
                }

            }, executor);
        }
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return CompletableFuture.completedFuture(Collections.unmodifiableSet(hostTable.keySet()));
    }
}
