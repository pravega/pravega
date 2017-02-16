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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.Cache;
import com.emc.pravega.controller.store.stream.tables.Data;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ZKScope implements Scope {

    private static final String SCOPE_PATH = "/store/%s";
    private static CuratorFramework client;
    private final String scopePath;
    private final String scopeName;
    private final Cache<Integer> cache;

    protected ZKScope(final String scopeName) {
        this.scopeName = scopeName;
        scopePath = String.format(SCOPE_PATH, scopeName);
        cache = new Cache<>(ZKScope::getData);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Boolean> createScope() {
        return addNode(scopePath);
    }

    @Override
    public CompletableFuture<Boolean> deleteScope() {
        return deleteNode(scopePath);
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        return getChildren(scopePath);
    }

    @Override
    public void refresh() {
        cache.invalidateAll();
    }

    public static void initialize(final CuratorFramework cf) {
        client = cf;
    }

    private static CompletableFuture<Data<Integer>> getData(final String path) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x) {
                        try {
                            Stat stat = new Stat();
                            return new Data<>(client.getData().storingStatIn(stat).forPath(path), stat.getVersion());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        throw new DataNotFoundException(path);
                    }
                });
    }

    private static CompletableFuture<Boolean> addNode(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                client.create().creatingParentsIfNeeded().forPath(path);
            } catch (KeeperException.NodeExistsException e) {
                throw new StoreException(StoreException.Type.NODE_EXISTS, e);
            } catch (Exception e) {
                throw new StoreException(StoreException.Type.UNKNOWN, e);
            }
            return true;
        });
    }

    private static CompletableFuture<Boolean> deleteNode(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                client.delete().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                throw new StoreException(StoreException.Type.NODE_NOT_FOUND, e);
            } catch (KeeperException.NotEmptyException e) {
                throw new StoreException(StoreException.Type.NODE_NOT_EMPTY, e);
            } catch (Exception e) {
                throw new StoreException(StoreException.Type.UNKNOWN, e);
            }
            return true;
        });
    }

    private static CompletableFuture<List<String>> getChildren(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.getChildren().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                throw new StoreException(StoreException.Type.NODE_NOT_FOUND, e);
            } catch (Exception e) {
                throw new StoreException(StoreException.Type.UNKNOWN, e);
            }
        });
    }

    private static CompletableFuture<Boolean> checkExists(final String path) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return client.checkExists().forPath(path);
                    } catch (Exception e) {
                        throw new StoreException(StoreException.Type.UNKNOWN, e);
                    }
                })
                .thenApply(x -> x != null);
    }
}
