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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.controller.store.stream.StoreException;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Zookeeper based host index.
 */
@Slf4j
public class ZKHostIndex implements HostIndex {
    private final CuratorFramework client;
    private final Executor executor;
    private final String hostRoot;

    public ZKHostIndex(CuratorFramework client, String hostRoot, Executor executor) {
        this.client = client;
        this.executor = executor;
        this.hostRoot = hostRoot;
    }

    @Override
    public CompletableFuture<Void> addEntity(final String hostId, final String entity) {
        return addEntity(hostId, entity, new byte[0]);
    }

    @Override
    public CompletableFuture<Void> addEntity(String hostId, String entity, byte[] entityData) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        return createNode(CreateMode.PERSISTENT, true, getHostPath(hostId, entity), entityData);
    }

    @Override
    public CompletableFuture<byte[]> getEntityData(String hostId, String entity) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        return readNode(getHostPath(hostId, entity));
    }

    @Override
    public CompletableFuture<Void> removeEntity(final String hostId, final String entity, final boolean deleteEmptyHost) {
        Preconditions.checkNotNull(hostId);
        Preconditions.checkNotNull(entity);
        return deleteNode(getHostPath(hostId, entity)).thenCompose(ignore -> {
            if (deleteEmptyHost) {
                return deleteNode(getHostPath(hostId)).exceptionally(ex -> {
                    if (ex instanceof StoreException.DataNotEmptyException) {
                        return null;
                    } else {
                        throw (StoreException) ex;
                    }
                });
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    @Override
    public CompletableFuture<Void> removeHost(final String hostId) {
        Preconditions.checkNotNull(hostId);
        return deleteNode(getHostPath(hostId));
    }

    @Override
    public CompletableFuture<List<String>> getEntities(final String hostId) {
        Preconditions.checkNotNull(hostId);
        String hostPath = getHostPath(hostId);
        return sync(hostPath).thenCompose(v -> getChildren(hostPath));
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return sync(hostRoot).thenCompose(v -> getChildren(hostRoot)).thenApply(list -> list.stream().collect(Collectors.toSet()));
    }

    private CompletableFuture<Void> createNode(CreateMode createMode, boolean createParents, String path, byte[] data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            BackgroundCallback callback = (cli, event) -> {
                if (event.getResultCode() == KeeperException.Code.OK.intValue() ||
                        event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(translateErrorCode(path, event));
                }
            };
            if (createParents) {
                client.create().creatingParentsIfNeeded().withMode(createMode).inBackground(callback, executor)
                        .forPath(path, data);
            } else {
                client.create().withMode(createMode).inBackground(callback, executor).forPath(path, data);
            }
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e));
        }
        return result;
    }

    private CompletableFuture<byte[]> readNode(String path) {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        try {
            client.getData().inBackground((cli, event) -> {
                if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                    result.complete(event.getData());
                } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                    log.debug("Node {} does not exist.", path);
                    result.complete(null);
                } else {
                    result.completeExceptionally(translateErrorCode(path, event));
                }
            }, executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e));
        }
        return result;
    }

    private CompletableFuture<Void> deleteNode(String path) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.delete().inBackground((cli, event) -> {
                if (event.getResultCode() == KeeperException.Code.OK.intValue() ||
                        event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(translateErrorCode(path, event));
                }
            }, executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e));
        }
        return result;
    }

    @VisibleForTesting
    CompletableFuture<Void> sync(final String path) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            client.sync().inBackground((cli, event) -> {
                if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(translateErrorCode(path, event));
                }
                }, executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    private CompletableFuture<List<String>> getChildren(String path) {
        CompletableFuture<List<String>> result = new CompletableFuture<>();
        try {
            client.getChildren().inBackground((cli, event) -> {
                if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                    result.complete(event.getChildren());
                } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                    result.complete(Collections.emptyList());
                } else {
                    result.completeExceptionally(translateErrorCode(path, event));
                }
            }, executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e));
        }
        return result;
    }

    private StoreException translateErrorCode(String path, CuratorEvent event) {
        StoreException ex;
        if (event.getResultCode() == KeeperException.Code.CONNECTIONLOSS.intValue() ||
                event.getResultCode() == KeeperException.Code.SESSIONEXPIRED.intValue() ||
                event.getResultCode() == KeeperException.Code.SESSIONMOVED.intValue() ||
                event.getResultCode() == KeeperException.Code.OPERATIONTIMEOUT.intValue()) {
            ex = StoreException.create(StoreException.Type.CONNECTION_ERROR, path);
        } else if (event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue()) {
            ex = StoreException.create(StoreException.Type.DATA_EXISTS, path);
        } else if (event.getResultCode() == KeeperException.Code.BADVERSION.intValue()) {
            ex = StoreException.create(StoreException.Type.WRITE_CONFLICT, path);
        } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
            ex = StoreException.create(StoreException.Type.DATA_NOT_FOUND, path);
        } else if (event.getResultCode() == KeeperException.Code.NOTEMPTY.intValue()) {
            ex = StoreException.create(StoreException.Type.DATA_CONTAINS_ELEMENTS, path);
        } else {
            ex = StoreException.create(StoreException.Type.UNKNOWN,
                    KeeperException.create(KeeperException.Code.get(event.getResultCode()), path));
        }
        return ex;
    }

    private String getHostPath(final String hostId, final String child) {
        return hostRoot + "/" + hostId + "/" + child;
    }

    private String getHostPath(final String hostId) {
        return hostRoot + "/" + hostId;
    }
}
