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
package io.pravega.controller.store;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.store.stream.Cache;
import io.pravega.controller.store.stream.StoreException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

@SuppressWarnings("deprecation")
@Slf4j
public class ZKStoreHelper {
    @Getter(AccessLevel.PUBLIC)
    private final CuratorFramework client;
    private final Executor executor;
    @VisibleForTesting
    @Getter(AccessLevel.PUBLIC)
    private final Cache cache;
    private final AtomicBoolean isZKConnected = new AtomicBoolean(false);

    public ZKStoreHelper(final CuratorFramework cf, Executor executor) {
        client = cf;
        this.executor = executor;
        this.cache = new Cache();
        this.isZKConnected.set(client.getZookeeperClient().isConnected());
        //Listen for any zookeeper connection state changes
        client.getConnectionStateListenable().addListener(
                (curatorClient, newState) -> {
                    this.isZKConnected.set(newState.isConnected());
                });
    }

    public boolean isZKConnected() {
        return isZKConnected.get();
    }

    /**
     * List Scopes in the cluster.
     *
     * @return A list of scopes.
     */
    public CompletableFuture<List<String>> listScopes() {
        return getChildren("/store");
    }

    CompletableFuture<Void> addNode(final String path) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.create().creatingParentsIfNeeded().inBackground(
                    callback(x -> result.complete(null), result::completeExceptionally, path), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }
        return result;
    }

    public CompletableFuture<Void> deleteNode(final String path) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.delete().inBackground(
                    callback(x -> result.complete(null), result::completeExceptionally, path), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }
        return result;
    }

    public CompletableFuture<Void> deleteNode(final String path, final Version version) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.delete().withVersion(version.asIntVersion().getIntValue()).inBackground(
                    callback(x -> result.complete(null),
                            e -> {
                                if (e instanceof StoreException.DataNotFoundException) { // deleted already
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }, path), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }
        return result;
    }

    // region curator client store access

    public CompletableFuture<Void> deletePath(final String path, final boolean deleteEmptyContainer) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final CompletableFuture<Void> deleteNode = new CompletableFuture<>();

        try {
            client.delete().inBackground(
                    callback(event -> deleteNode.complete(null),
                            e -> {
                                if (e instanceof StoreException.DataNotFoundException) { // deleted already
                                    deleteNode.complete(null);
                                } else {
                                    deleteNode.completeExceptionally(e);
                                }
                            }, path), executor).forPath(path);
        } catch (Exception e) {
            deleteNode.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        deleteNode.whenComplete((res, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else if (deleteEmptyContainer) {
                final String container = ZKPaths.getPathAndNode(path).getPath();
                try {
                    client.delete().inBackground(
                            callback(event -> result.complete(null),
                                    e -> {
                                        if (e instanceof StoreException.DataNotFoundException) { // deleted already
                                            result.complete(null);
                                        } else if (e instanceof StoreException.DataNotEmptyException) { // non empty dir
                                            result.complete(null);
                                        } else {
                                            result.completeExceptionally(e);
                                        }
                                    }, path), executor).forPath(container);
                } catch (Exception e) {
                    result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
                }
            } else {
                result.complete(null);
            }
        });
        return result;
    }

    public CompletableFuture<Void> deleteTree(final String path) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.delete()
                    .deletingChildrenIfNeeded()
                    .inBackground(callback(event -> result.complete(null), result::completeExceptionally, path),
                            executor)
                    .forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }
        return result;
    }

    /**
     * Method to retrieve an entity from zookeeper and then deserialize it using the supplied `fromBytes` function. 
     * @param path Zk path where entity is stored
     * @param fromBytes Deserialization function for creating object of type T
     * @param <T> Type of Object to retrieve. 
     * @return CompletableFuture which when completed will have the object of type T retrieved from path and deserialized 
     * using fromBytes function. 
     */
    public <T> CompletableFuture<VersionedMetadata<T>> getData(final String path, Function<byte[], T> fromBytes) {
        final CompletableFuture<VersionedMetadata<T>> result = new CompletableFuture<>();

        try {
            client.getData().inBackground(
                    callback(event -> {
                                try {
                                    T deserialized = fromBytes.apply(event.getData());
                                    result.complete(new VersionedMetadata<>(deserialized, new Version.IntVersion(event.getStat().getVersion())));
                                } catch (Exception e) {
                                    log.error("Exception thrown while deserializing the data", e);
                                    result.completeExceptionally(e);
                                }
                            },
                            result::completeExceptionally, path), executor)
                    .forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    public CompletableFuture<List<String>> getChildren(final String path) {
        return getChildren(path, true);
    }

    CompletableFuture<List<String>> getChildren(final String path, boolean ignoreDataNotFound) {
        final CompletableFuture<List<String>> result = new CompletableFuture<>();

        try {
            client.getChildren().inBackground(
                    callback(event -> result.complete(event.getChildren()),
                            e -> {
                                if (ignoreDataNotFound && e instanceof StoreException.DataNotFoundException) {
                                    result.complete(Collections.emptyList());
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }, path), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    public CompletableFuture<Integer> setData(final String path, final byte[] data, final Version version) {
        final CompletableFuture<Integer> result = new CompletableFuture<>();
        try {
            if (version == null) {
                client.setData().inBackground(
                        callback(event -> result.complete(event.getStat().getVersion()), result::completeExceptionally, path), executor)
                        .forPath(path, data);
            } else {
                client.setData().withVersion(version.asIntVersion().getIntValue()).inBackground(
                        callback(event -> result.complete(event.getStat().getVersion()), result::completeExceptionally, path), executor)
                        .forPath(path, data);
            }
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }
        return result;
    }

    public CompletableFuture<Integer> createZNode(final String path, final byte[] data) {
        final CompletableFuture<Integer> result = new CompletableFuture<>();
        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(x.getStat().getVersion()),
                    e -> result.completeExceptionally(e), path);
            createBuilder.creatingParentsIfNeeded().inBackground(callback, executor).forPath(path, data);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    public CompletableFuture<Integer> createZNodeIfNotExist(final String path, final byte[] data) {
        return createZNodeIfNotExist(path, data, true);
    }

    public CompletableFuture<Integer> createZNodeIfNotExist(final String path, final byte[] data, final boolean createParent) {
        final CompletableFuture<Integer> result = new CompletableFuture<>();
        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(x.getStat().getVersion()),
                    e -> {
                        if (e instanceof StoreException.DataExistsException) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(e);
                        }
                    }, path);
            if (createParent) {
                createBuilder.creatingParentsIfNeeded().inBackground(callback, executor).forPath(path, data);
            } else {
                createBuilder.inBackground(callback, executor).forPath(path, data);
            }
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    public CompletableFuture<Integer> createZNodeIfNotExist(final String path) {
        return createZNodeIfNotExist(path, true);
    }

    private CompletableFuture<Integer> createZNodeIfNotExist(final String path, final boolean createParent) {
        final CompletableFuture<Integer> result = new CompletableFuture<>();

        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(x.getStat().getVersion()),
                    e -> {
                        if (e instanceof StoreException.DataExistsException) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(e);
                        }
                    }, path);
            if (createParent) {
                createBuilder.creatingParentsIfNeeded().inBackground(callback, executor).forPath(path);
            } else {
                createBuilder.inBackground(callback, executor).forPath(path);
            }
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    public CompletableFuture<Boolean> createEphemeralZNode(final String path, byte[] data) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(true),
                    e -> {
                        if (e instanceof StoreException.DataExistsException) {
                            result.complete(false);
                        } else {
                            result.completeExceptionally(e);
                        }
                    }, path);
                createBuilder.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                        .inBackground(callback, executor).forPath(path, data);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    CompletableFuture<String> createEphemeralSequentialZNode(final String path) {
        final CompletableFuture<String> result = new CompletableFuture<>();

        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(x.getName()),
                    result::completeExceptionally, path);
            createBuilder.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                         .inBackground(callback, executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }
    
    public CompletableFuture<String> createPersistentSequentialZNode(final String path, final byte[] data) {
        final CompletableFuture<String> result = new CompletableFuture<>();

        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(x.getName()),
                    result::completeExceptionally, path);
            createBuilder.creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                         .inBackground(callback, executor).forPath(path, data);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }
    
    public CompletableFuture<Void> sync(final String path) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            BackgroundCallback callback = callback(x -> result.complete(null),
                    result::completeExceptionally, path);
            client.sync().inBackground(callback, executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }
    
    public CompletableFuture<Boolean> checkExists(final String path) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        try {
            client.checkExists().inBackground(
                    callback(x -> result.complete(x.getStat() != null),
                            ex -> {
                                if (ex instanceof StoreException.DataNotFoundException) {
                                    result.complete(false);
                                } else {
                                    result.completeExceptionally(ex);
                                }
                            }, path), executor).forPath(path);

        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    private BackgroundCallback callback(Consumer<CuratorEvent> result, Consumer<Throwable> exception, String path) {
        return (client, event) -> {
            if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                result.accept(event);
            } else if (event.getResultCode() == KeeperException.Code.CONNECTIONLOSS.intValue() ||
                    event.getResultCode() == KeeperException.Code.SESSIONEXPIRED.intValue() ||
                    event.getResultCode() == KeeperException.Code.SESSIONMOVED.intValue() ||
                    event.getResultCode() == KeeperException.Code.OPERATIONTIMEOUT.intValue()) {
                exception.accept(StoreException.create(StoreException.Type.CONNECTION_ERROR, path));
            } else if (event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue()) {
                exception.accept(StoreException.create(StoreException.Type.DATA_EXISTS, path));
            } else if (event.getResultCode() == KeeperException.Code.BADVERSION.intValue()) {
                exception.accept(StoreException.create(StoreException.Type.WRITE_CONFLICT, path));
            } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                exception.accept(StoreException.create(StoreException.Type.DATA_NOT_FOUND, path));
            } else if (event.getResultCode() == KeeperException.Code.NOTEMPTY.intValue()) {
                exception.accept(StoreException.create(StoreException.Type.DATA_CONTAINS_ELEMENTS, path));
            } else {
                exception.accept(StoreException.create(StoreException.Type.UNKNOWN,
                        KeeperException.create(KeeperException.Code.get(event.getResultCode()), path)));
            }
        };
    }
    // endregion
    
    public PathChildrenCache getPathChildrenCache(String path, boolean cacheData) {
        return new PathChildrenCache(client, path, cacheData);
    }

    public NodeCache getNodeCache(String path, boolean cacheData) {
        return new NodeCache(client, path, cacheData);
    }

    public <T> CompletableFuture<VersionedMetadata<T>> getCachedData(String path, String id, Function<byte[], T> fromBytes) {
        ZkCacheKey<T> cacheKey = new ZkCacheKey<>(path, id, fromBytes);
        VersionedMetadata<?> cached = cache.getCachedData(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(getVersionedMetadata(cached));
        } else {
            long time = System.currentTimeMillis();
            return getData(path, fromBytes)
                .thenApply(v -> {
                    VersionedMetadata<T> record = new VersionedMetadata<>(v.getObject(), v.getVersion());
                    cache.put(cacheKey, record, time);
                    return record;
                });
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T> VersionedMetadata<T> getVersionedMetadata(VersionedMetadata v) {
        // Since cache is untyped and holds all types of deserialized objects, we typecast it to the requested object type
        // based on the type in caller's supplied Deserialization function. 
        return new VersionedMetadata<>((T) v.getObject(), v.getVersion());
    }

    public void invalidateCache(String path, String id) {
        cache.invalidateCache(new ZkCacheKey<>(path, id, x -> null));
    }

    /**
     * Cache key used to load and retrieve entities from the cache. 
     * The cache key also provides a deserialization function which is used after loading the value from the zookeeper.
     * The cache key is comprised of three parts - zk path, id and deserialization function.
     * Only Id and ZkPath are used in equals and hashcode in the cache key. 
     */
    @Data
    public static class ZkCacheKey<T> implements Cache.CacheKey {
        // ZkPath is the path at which the entity is stored in zookeeper.
        private final String path;
        // Id is the unique id that callers can use for entities that are deleted and recreated. Using a unique id upon recreation 
        // of entity ensures that the previously cached value against older id becomes stale.
        private final String id;
        // FromBytesFunction is the deserialization function which takes byes and deserializes it into object of type T. 
        // Refer to ZkStoreHelper constructor for cache loader.  
        private final Function<byte[], T> fromBytesFunc;

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + path.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public boolean equals(Object obj) {
            return obj instanceof ZkCacheKey 
                    && path.equals(((ZkCacheKey) obj).path)
                    && id.equals(((ZkCacheKey) obj).id);
        }
    }

}
