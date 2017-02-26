/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.store.stream.tables.Data;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

@Slf4j
public class ZKStoreHelper {
    
    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    static final String ACTIVE_TX_PATH = ACTIVE_TX_ROOT_PATH + "/%s";
    private static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    static final String COMPLETED_TX_PATH = COMPLETED_TX_ROOT_PATH + "/%s";
    
    private CuratorFramework client;

    public ZKStoreHelper(final CuratorFramework cf) {
        client = cf;
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
                StoreException.create(StoreException.Type.NODE_EXISTS, path);
            } catch (Exception e) {
                StoreException.create(StoreException.Type.UNKNOWN, path);
            }
        });
    }

    CompletableFuture<Void> deleteNode(final String path) {
        return CompletableFuture.runAsync(() -> {
            try {
                client.delete().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                 StoreException.create(StoreException.Type.NODE_NOT_FOUND, path);
            } catch (KeeperException.NotEmptyException e) {
                 StoreException.create(StoreException.Type.NODE_NOT_EMPTY, path);
            } catch (Exception e) {
                StoreException.create(StoreException.Type.UNKNOWN, path);
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
    
    CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
        return getAllTransactionData(ACTIVE_TX_ROOT_PATH)
                .thenApply(x -> x.entrySet().stream()
                                 .map(z -> {
                                     ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(z.getKey());
                                     String node = pathAndNode.getNode();
                                     final String stream = ZKPaths.getNodeFromPath(pathAndNode.getPath());
                                     final UUID txId = UUID.fromString(node);
                                     return new ActiveTxRecordWithStream(stream, stream, txId, ActiveTxRecord.parse(z.getValue().getData()));
                                 })
                                 .collect(Collectors.toList()));
    }

    CompletableFuture<Map<String, Data<Integer>>> getAllCompletedTx() {
        return getAllTransactionData(COMPLETED_TX_ROOT_PATH)
                .thenApply(x -> x.entrySet().stream()
                                 .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
    
    public CompletableFuture<Void> deletePath(final String path, final boolean deleteEmptyContainer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.delete().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                // already deleted, ignore
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> {
            if (deleteEmptyContainer) {
                final String container = ZKPaths.getPathAndNode(path).getPath();
                try {
                    client.delete().forPath(container);
                } catch (KeeperException.NotEmptyException | KeeperException.NoNodeException e) {
                    // log and ignore;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        });
    }

    CompletableFuture<Data<Integer>> getData(final String path) throws DataNotFoundException {
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


    CompletableFuture<Void> updateTxData(final String path, final byte[] data) {
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
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.getChildren().forPath(path);
            } catch (KeeperException.NoNodeException nne) {
                return Collections.emptyList();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<Map<String, Data<Integer>>> getAllTransactionData(final String rootPath) {
        return getChildrenPath(rootPath) // list of all streams for either active or completed tx based on root path
                                         .thenApply(x -> x.stream()
                                                          .map(this::getChildrenPath) // get all transactions on the stream
                                                          .collect(Collectors.toList()))
                                         .thenCompose(FutureHelpers::allOfWithResults)
                                         .thenApply(z -> z.stream().flatMap(Collection::stream).collect(Collectors.toList())) // flatten list<list> to list
                                         .thenApply(x -> x.stream()
                                                          .collect(Collectors.toMap(z -> z, this::getData)))
                                         .thenCompose(FutureHelpers::allOfWithResults); // convert Map<string, future<Data>> to future<map<String, Data>>
    }

    CompletableFuture<Void> setData(final String path, final Data<Integer> data) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x) {
                        try {
                            if (data.getVersion() == null) {
                                return client.setData().forPath(path, data.getData());
                            } else {
                                return client.setData().withVersion(data.getVersion()).forPath(path, data.getData());
                            }
                        } catch (KeeperException.BadVersionException e) {
                            throw new WriteConflictException(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        //path does not exist indicates Stream is not present
                        log.error("Failed to write data. path {}", path);
                        throw new StreamNotFoundException(extractStreamName(path));
                    }
                }) // load into cache after writing the data
                .thenApply(x -> null);
    }

    private String extractStreamName(final String path) {
        Preconditions.checkNotNull(path, "path");
        String[] result = path.split("/");
        if (result.length > 2) {
            return result[2];
        } else {
            return path;
        }
    }

    CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.create().creatingParentsIfNeeded().forPath(path, data);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> null);
    }

    CompletableFuture<Void> createZNodeIfNotExist(final String path) {
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

    CompletableFuture<Boolean> checkExists(final String path) {
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
}
