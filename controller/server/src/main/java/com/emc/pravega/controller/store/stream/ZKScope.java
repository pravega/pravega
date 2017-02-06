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
import com.emc.pravega.controller.store.stream.tables.Create;
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
    public CompletableFuture<Boolean> createScope(String scopeName) {
        return checkScopeExists(scopeName)
                .thenCompose(x -> createZNodeIfNotExist(scopePath))
                .thenApply(x -> true);
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scope) {
        return null;
    }

    @Override
    public CompletableFuture<List<Stream>> listStreamsInScope(String scope) {
        return null;
    }

    @Override
    public void refresh() {
        cache.invalidateAll();
    }

    CompletableFuture<Void> checkScopeExists(String scopeName) throws ScopeAlreadyExistsException {
        return checkExists(scopePath)
                .thenApply(x -> {
                    if (x) {
                        throw new ScopeAlreadyExistsException(scopeName);
                    }
                    return null;
                });
    }

    private static CompletableFuture<Boolean> checkExists(final String path) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return client.checkExists().forPath(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .thenApply(x -> x != null);
    }

    public static void initialize(final CuratorFramework cf) {
        client = cf;
    }

    private static CompletableFuture<Data<Integer>> getData(final String path) throws DataNotFoundException {
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

    CompletableFuture<Void> createScopePath(final Create create) {
        return createZNodeIfNotExist(scopePath);
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.create().creatingParentsIfNeeded().forPath(path);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> null);
    }
}
