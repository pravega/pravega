/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.stream.tables.Data;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

@Slf4j
public class ZKStoreHelper {
    
    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    static final String ACTIVE_TX_PATH = ACTIVE_TX_ROOT_PATH + "/%s";
    private static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    static final String COMPLETED_TX_PATH = COMPLETED_TX_ROOT_PATH + "/%s";
    
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
        return CompletableFuture.runAsync(() -> {
            try {
                client.create().creatingParentsIfNeeded().forPath(path);
            } catch (KeeperException.NodeExistsException e) {
                throw StoreException.create(StoreException.Type.NODE_EXISTS, path);
            } catch (Exception e) {
                throw StoreException.create(StoreException.Type.UNKNOWN, path);
            }
        });
    }

    CompletableFuture<Void> deleteNode(final String path) {
        return CompletableFuture.runAsync(() -> {
            try {
                client.delete().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                throw StoreException.create(StoreException.Type.NODE_NOT_FOUND, path);
            } catch (KeeperException.NotEmptyException e) {
                throw StoreException.create(StoreException.Type.NODE_NOT_EMPTY, path);
            } catch (Exception e) {
                throw StoreException.create(StoreException.Type.UNKNOWN, path);
            }
        });
    }

    CompletableFuture<List<String>> getStreamsInPath(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.getChildren().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                FutureHelpers.failedFuture(StoreException.create(StoreException.Type.NODE_NOT_FOUND, path));
            } catch (Exception e) {
                FutureHelpers.failedFuture(StoreException.create(StoreException.Type.UNKNOWN, path));
            }
            return null;
        });
    }

    CompletableFuture<Void> checkPoint(String readerGroup, String readerId, byte[] checkpointBlob) {

        String path = ZKPaths.makePath("", readerGroup, readerId);
        Data<Integer> data = new Data<>(checkpointBlob, null);
        return setData(path, data);
    }

    CompletableFuture<ByteBuffer> readCheckPoint(String readerGroup, String readerId) {
        String path = ZKPaths.makePath("", readerGroup, readerId);
        return getData(path).thenApply(data -> ByteBuffer.wrap(data.getData()));
    }

    // region curator client store access

    CompletableFuture<Void> deletePath(final String path, final boolean deleteEmptyContainer) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final CompletableFuture<Void> deleteNode = new CompletableFuture<>();

        try {
            client.delete().inBackground(
                    callback(event -> deleteNode.complete(null),
                            e -> {
                                if (e instanceof DataNotFoundException) { // deleted already
                                    deleteNode.complete(null);
                                } else {
                                    deleteNode.completeExceptionally(e);
                                }
                            }), executor).forPath(path);
        } catch (Exception e) {
            deleteNode.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
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
                                        if (e instanceof DataNotFoundException) { // deleted already
                                            result.complete(null);
                                        } else if (e instanceof DataExistsException) { // non empty dir
                                            result.complete(null);
                                        } else {
                                            result.completeExceptionally(e);
                                        }
                                    }), executor).forPath(container);
                } catch (Exception e) {
                    result.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
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
            client.delete().deletingChildrenIfNeeded().inBackground(
                    callback(event -> result.complete(null),
                            e -> {
                                if (e instanceof DataNotFoundException) {
                                    result.completeExceptionally(new StoreException(StoreException.Type.NODE_NOT_FOUND));
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
        }

        return result;
    }

    CompletableFuture<Data<Integer>> getData(final String path) throws DataNotFoundException {
        final CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

        checkExists(path)
                .whenComplete((exists, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else if (exists) {
                        try {
                            client.getData().inBackground(
                                    callback(event -> result.complete(new Data<>(event.getData(), event.getStat().getVersion())),
                                            result::completeExceptionally), executor).forPath(path);
                        } catch (Exception e) {
                            result.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
                        }
                    } else {
                        result.completeExceptionally(new DataNotFoundException(path));
                    }
                });

        return result;
    }

    CompletableFuture<Void> updateTxnData(final String path, final byte[] data) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                client.setData().forPath(path, data);
                return null;
            } catch (KeeperException.NoNodeException nne) {
                throw new DataNotFoundException(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<List<String>> getChildrenPath(final String rootPath) {
        return getChildren(rootPath)
                .thenApply(children -> children.stream().map(x -> ZKPaths.makePath(rootPath, x)).collect(Collectors.toList()));
    }

    CompletableFuture<List<String>> getChildren(final String path) {
        final CompletableFuture<List<String>> result = new CompletableFuture<>();

        try {
            client.getChildren().inBackground(
                    callback(event -> result.complete(event.getChildren()),
                            e -> {
                                if (e instanceof DataNotFoundException) {
                                    result.complete(Collections.emptyList());
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
        }

        return result;
    }

    CompletableFuture<Void> setData(final String path, final Data<Integer> data) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        checkExists(path)
                .whenComplete((exists, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else if (exists) {
                        try {
                            if (data.getVersion() == null) {
                                client.setData().inBackground(
                                        callback(event -> result.complete(null), result::completeExceptionally), executor)
                                        .forPath(path, data.getData());
                            } else {
                                client.setData().withVersion(data.getVersion()).inBackground(
                                        callback(event -> result.complete(null), result::completeExceptionally), executor)
                                        .forPath(path, data.getData());
                            }
                        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
                            result.completeExceptionally(new StoreConnectionException(e));
                        } catch (Exception e) {
                            result.completeExceptionally(e);
                        }
                    } else {
                        log.error("Failed to write data. path {}", path);
                        result.completeExceptionally(new DataNotFoundException(path));
                    }
                });

        return result;
    }

    CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.create().creatingParentsIfNeeded().inBackground(
                    callback(
                            x -> result.complete(null),
                            e -> {
                                if (e instanceof DataExistsException) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }), executor).forPath(path, data);
        } catch (Exception e) {
            result.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
        }

        return result;
    }

    CompletableFuture<Void> createZNodeIfNotExist(final String path) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            client.create().creatingParentsIfNeeded().inBackground(
                    callback(x -> result.complete(null),
                            e -> {
                                if (e instanceof DataExistsException) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }), executor).forPath(path);
        } catch (Exception e) {
            result.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
        }

        return result;
    }

    CompletableFuture<Boolean> checkExists(final String path) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        try {
            client.checkExists().inBackground(
                    callback(x -> result.complete(x.getStat() != null),
                            ex -> {
                                if (ex instanceof DataNotFoundException) {
                                    result.complete(false);
                                } else {
                                    result.completeExceptionally(ex);
                                }
                            }), executor).forPath(path);

        } catch (Exception e) {
            result.completeExceptionally(new StoreException(StoreException.Type.UNKNOWN));
        }

        return result;
    }

    BackgroundCallback callback(Consumer<CuratorEvent> result, Consumer<Throwable> exception) {
        return (client, event) -> {
            if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                result.accept(event);
            } else if (event.getResultCode() == KeeperException.Code.CONNECTIONLOSS.intValue() ||
                    event.getResultCode() == KeeperException.Code.SESSIONEXPIRED.intValue() ||
                    event.getResultCode() == KeeperException.Code.SESSIONMOVED.intValue() ||
                    event.getResultCode() == KeeperException.Code.OPERATIONTIMEOUT.intValue()) {
                exception.accept(new StoreConnectionException("" + event.getResultCode()));
            } else if (event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue()) {
                exception.accept(new DataExistsException(event.getPath()));
            } else if (event.getResultCode() == KeeperException.Code.BADVERSION.intValue()) {
                exception.accept(new WriteConflictException(event.getPath()));
            } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                exception.accept(new DataNotFoundException(event.getPath()));
            } else if (event.getResultCode() == KeeperException.Code.NOTEMPTY.intValue()) {
                exception.accept(new DataExistsException(event.getPath()));
            } else {
                exception.accept(new RuntimeException("Curator background task errorCode:" + event.getResultCode()));
            }
        };
    }
    // endregion
}
