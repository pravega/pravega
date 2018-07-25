/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.store.stream.tables.Data;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

@Slf4j
public class ZKStoreHelper {
    static final String DELETED_STREAMS_PATH = "/lastActiveStreamSegment/%s";

    @Getter(AccessLevel.PACKAGE)
    private final CuratorFramework client;
    private final Executor executor;
    public ZKStoreHelper(final CuratorFramework cf, Executor executor) {
        client = cf;
        this.executor = executor;
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

    CompletableFuture<Void> deleteNode(final String path) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.delete().inBackground(
                    callback(x -> result.complete(null), result::completeExceptionally, path), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }
        return result;
    }

    // region curator client store access

    CompletableFuture<Void> deletePath(final String path, final boolean deleteEmptyContainer) {
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

    CompletableFuture<Data<Integer>> getData(final String path) {
        final CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

        try {
            client.getData().inBackground(
                    callback(event -> result.complete(new Data<>(event.getData(), event.getStat()
                                    .getVersion())),
                            result::completeExceptionally, path), executor)
                    .forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    CompletableFuture<List<String>> getChildren(final String path) {
        final CompletableFuture<List<String>> result = new CompletableFuture<>();

        try {
            client.getChildren().inBackground(
                    callback(event -> result.complete(event.getChildren()),
                            e -> {
                                if (e instanceof StoreException.DataNotFoundException) {
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

    CompletableFuture<Void> setData(final String path, final Data<Integer> data) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            if (data.getVersion() == null) {
                client.setData().inBackground(
                        callback(event -> result.complete(null), result::completeExceptionally, path), executor)
                        .forPath(path, data.getData());
            } else {
                client.setData().withVersion(data.getVersion()).inBackground(
                        callback(event -> result.complete(null), result::completeExceptionally, path), executor)
                        .forPath(path, data.getData());
            }
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }
        return result;
    }

    CompletableFuture<Void> createZNode(final String path, final byte[] data) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(null),
                    e -> result.completeExceptionally(e), path);
            createBuilder.creatingParentsIfNeeded().inBackground(callback, executor).forPath(path, data);
        } catch (Exception e) {
            result.completeExceptionally(StoreException.create(StoreException.Type.UNKNOWN, e, path));
        }

        return result;
    }

    CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data) {
        return createZNodeIfNotExist(path, data, true);
    }

    CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data, final boolean createParent) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(null),
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

    CompletableFuture<Void> createZNodeIfNotExist(final String path) {
        return createZNodeIfNotExist(path, true);
    }

    CompletableFuture<Void> createZNodeIfNotExist(final String path, final boolean createParent) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            CreateBuilder createBuilder = client.create();
            BackgroundCallback callback = callback(x -> result.complete(null),
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

    CompletableFuture<Boolean> createEphemeralZNode(final String path, byte[] data) {
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

    CompletableFuture<Boolean> checkExists(final String path) {
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
}
