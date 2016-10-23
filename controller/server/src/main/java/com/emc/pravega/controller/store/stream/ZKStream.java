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

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Utilities;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxStatus;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 * <p>
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
class ZKStream extends PersistentStreamBase {
    private static final String STREAM_PATH = "/streams/%s";
    private static final String CREATION_TIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configuration";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String HISTORY_PATH = STREAM_PATH + "/history";
    private static final String INDEX_PATH = STREAM_PATH + "/index";

    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    private static final String ACTIVE_TX_PATH = ACTIVE_TX_ROOT_PATH + "/%s";
    private static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    private static final String COMPLETED_TX_PATH = COMPLETED_TX_ROOT_PATH + "/%s";

    private static CuratorFramework client;

    private final TreeCache streamCache;
    private final TreeCache activeTxCache;
    private final String creationPath;
    private final String configurationPath;
    private final String segmentPath;
    private final String segmentChunkPathTemplate;
    private final String historyPath;
    private final String indexPath;
    private final String activeTxPath;
    private final String completedTxPath;
    private final LoadingCache<String, CompletableFuture<byte[]>> zkNodes;

    public ZKStream(final String name) {
        super(name);

        creationPath = String.format(CREATION_TIME_PATH, name);
        configurationPath = String.format(CONFIGURATION_PATH, name);
        segmentPath = String.format(SEGMENT_PATH, name);
        segmentChunkPathTemplate = segmentPath + "/%s";
        historyPath = String.format(HISTORY_PATH, name);
        indexPath = String.format(INDEX_PATH, name);
        activeTxPath = String.format(ACTIVE_TX_PATH, name);
        completedTxPath = String.format(COMPLETED_TX_PATH, name);

        streamCache = TreeCache.newBuilder(client, String.format(STREAM_PATH, name)).build();
        activeTxCache = TreeCache.newBuilder(client, activeTxPath).setCacheData(true).build();

        zkNodes = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .refreshAfterWrite(2, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, CompletableFuture<byte[]>>() {
                            @ParametersAreNonnullByDefault
                            public CompletableFuture<byte[]> load(String path) {
                                try {
                                    // TODO: if path is indexTable, or history table, check if
                                    // existing data (if any) in the cache is consistent
                                    if (path.equals(indexPath) || path.equals(historyPath)) {
                                        zkNodes.invalidateAll();
                                    }
                                    return getData(path);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
    }

    public void init() {
        zkNodes.invalidateAll();
        try {
            streamCache.start();
            addListener(streamCache);
            activeTxCache.start();
            addListener(activeTxCache);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void tearDown() {
        zkNodes.cleanUp();
        streamCache.close();
        activeTxCache.close();
    }

    @Override
    public CompletableFuture<Boolean> createStream(Create create) throws StreamAlreadyExistsException {

        return createZNodeIfNotExist(activeTxPath)
                .thenApply(x -> invalidateCache(activeTxPath))
                .thenCompose(x -> checkExists(creationPath))
                .thenCompose(x -> {
                    if (x) {
                        return getData(creationPath)
                                .thenApply(creationTime -> Utilities.toLong(creationTime) == create.getEventTime());
                    } else
                        return createZnode(creationPath, Utilities.toByteArray(create.getEventTime()))
                                .thenApply(z -> invalidateCache(creationPath))
                                .thenApply(z -> true);
                })
                .thenApply(x -> {
                    if (!x)
                        throw new StreamAlreadyExistsException(getName());

                    return true;
                });
    }

    @Override
    public CompletableFuture<Void> createConfiguration(final Create create) {
        return createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(create.getConfiguration()))
                .thenApply(x -> invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return createZNodeIfNotExist(segmentPath).thenApply(x -> invalidateCache(segmentPath));
    }

    @Override
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final byte[] data) {
        String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkNumber);
        return createZNodeIfNotExist(segmentChunkPath, data)
                .thenApply(x -> invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<Void> createIndexTable(final Create create) {
        final byte[] indexTable = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
        return createZNodeIfNotExist(indexPath, indexTable)
                .thenApply(x -> invalidateCache(indexPath));
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Create create) {
        final int numSegments = create.getConfiguration().getScalingingPolicy().getMinNumSegments();
        final byte[] historyTable = TableHelper.updateHistoryTable(new byte[0],
                create.getEventTime(),
                IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

        return createZNodeIfNotExist(historyPath, historyTable)
                .thenApply(x -> invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final byte[] updated) {
        return setData(historyPath, updated).thenApply(x -> invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> createSegmentFile(final Create create) {
        final int numSegments = create.getConfiguration().getScalingingPolicy().getMinNumSegments();
        final int chunkFileName = 0;
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        final int toCreate = newRanges.size();

        final byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                new byte[0],
                toCreate,
                newRanges,
                create.getEventTime()
        );

        String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkFileName);
        return createZNodeIfNotExist(segmentChunkPath, segmentTable)
                .thenApply(x -> invalidateCache(segmentChunkPath));
    }

    @Override
    CompletableFuture<Void> createNewTransaction(UUID txId, long timestamp) {
        final String activePath = getActiveTxPath(txId.toString());
        return createZNodeIfNotExist(activePath,
                new ActiveTxRecord(timestamp, TxStatus.OPEN).toByteArray())
                .thenApply(x -> invalidateCache(activePath));
    }

    @Override
    CompletableFuture<ActiveTxRecord> getActiveTx(UUID txId) {
        final String activeTxPath = getActiveTxPath(txId.toString());
        invalidateCache(activeTxPath);

        return getCachedData(activeTxPath)
                .thenApply(ActiveTxRecord::parse);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(UUID txId) {
        final String activePath = getActiveTxPath(txId.toString());

        return getActiveTx(txId)
                .thenCompose(x -> setData(activePath,
                        new ActiveTxRecord(x.getTxCreationTimestamp(), TxStatus.SEALED).toByteArray()))
                .thenApply(x -> invalidateCache(activePath));
    }

    @Override
    CompletableFuture<CompletedTxRecord> getCompletedTx(UUID txId) {
        return getCachedData(getCompletedTxPath(txId.toString()))
                .thenApply(CompletedTxRecord::parse);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(UUID txId) {
        return deletePath(getActiveTxPath(txId.toString()));
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, TxStatus complete, long timestamp) {
        final String completedTxPath = getCompletedTxPath(txId.toString());
        return createZNodeIfNotExist(completedTxPath,
                new CompletedTxRecord(timestamp, complete).toByteArray())
                .thenApply(x -> invalidateCache(completedTxPath));
    }

    @Override
    public CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration) {
        return setData(configurationPath, SerializationUtils.serialize(configuration))
                .thenApply(x -> invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        return getCachedData(configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x));
    }

    @Override
    public CompletableFuture<Segment> getSegmentRow(final int number) {
        // compute the file name based on segment number
        final int chunkNumber = number / SegmentRecord.SEGMENT_CHUNK_SIZE;

        return getSegmentTableChunk(chunkNumber)
                .thenApply(x -> TableHelper.getSegment(number, x));
    }

    @Override
    public CompletableFuture<byte[]> getSegmentTableChunk(final int chunkNumber) {
        return getCachedData(String.format(segmentChunkPathTemplate, chunkNumber));
    }

    @Override
    CompletableFuture<Void> setSegmentTableChunk(final int chunkNumber, final byte[] data) {
        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkNumber);
        return setData(segmentChunkPath, data)
                .thenApply(x -> invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<List<String>> getSegmentChunks() {
        return getChildren(segmentPath);
    }

    @Override
    public CompletableFuture<byte[]> getHistoryTable() {
        return getCachedData(historyPath);
    }

    @Override
    public CompletableFuture<byte[]> getIndexTable() {
        return getCachedData(indexPath);
    }

    @Override
    CompletableFuture<Void> updateIndexTable(final byte[] updated) {
        return setData(indexPath, updated).thenApply(x -> invalidateCache(indexPath));
    }

    private void addListener(final TreeCache cache) {
        TreeCacheListener listener = (client1, event) -> {
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                case NODE_REMOVED:
                    invalidateCache(event.getData().getPath());
                    break;
                default:
            }
        };

        cache.getListenable().addListener(listener);
    }

    private String getActiveTxPath(String txId) {
        return activeTxPath + "/" + txId;
    }

    private String getCompletedTxPath(String txId) {
        return completedTxPath + "/" + txId;
    }

    static CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
        return getAllTransactionData(ACTIVE_TX_ROOT_PATH)
                .thenApply(x -> x.entrySet().stream()
                        .map(z -> {
                            final String[] pathTokens = z.getKey().replace(ACTIVE_TX_ROOT_PATH, "").split("/");
                            final String stream = pathTokens[1];
                            final UUID txId = UUID.fromString(pathTokens[2]);
                            return new ActiveTxRecordWithStream(stream, stream, txId, ActiveTxRecord.parse(z.getValue()));
                        })
                        .collect(Collectors.toList()));
    }

    static CompletableFuture<Map<String, CompletedTxRecord>> getAllCompletedTx() {
        return getAllTransactionData(COMPLETED_TX_ROOT_PATH)
                .thenApply(x -> x.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, z -> CompletedTxRecord.parse(z.getValue()))));
    }

    static CompletableFuture<Void> deletePath(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.delete().forPath(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<byte[]> getCachedData(String path) {
        try {
            return zkNodes.get(path);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Void invalidateCache(String path) {
        zkNodes.invalidate(path);
        return null;
    }

    private static CompletableFuture<byte[]> getData(final String path) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x) {
                        try {
                            return client.getData().forPath(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else throw new DataNotFoundException(path);
                });
    }

    private static CompletableFuture<List<String>> getChildrenPath(final String rootPath) {
        return getChildren(rootPath)
                .thenApply(children -> children.stream().map(x -> rootPath + "/" + x).collect(Collectors.toList()));
    }

    private static CompletableFuture<List<String>> getChildren(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.getChildren().forPath(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static CompletableFuture<Map<String, byte[]>> getAllTransactionData(final String rootPath) {
        return getChildrenPath(rootPath)
                .thenApply(x -> x.stream()
                        .map(z -> getChildrenPath(z))
                        .collect(Collectors.toList()))
                .thenCompose(FutureCollectionHelper::sequence)
                .thenApply(z -> z.stream().flatMap(Collection::stream).collect(Collectors.toList()))
                .thenApply(x -> x.stream()
                        .collect(Collectors.toMap(z -> z, ZKStream::getData)))
                .thenCompose(FutureCollectionHelper::sequenceMap);
    }

    private static CompletableFuture<Void> setData(final String path, final byte[] data) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x) {
                        try {
                            return client.setData().forPath(path, data);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else throw new RuntimeException(String.format("path %s not Found", path));
                })
                        // load into cache after writing the data
                .thenApply(x -> null);
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path) {
        return checkExists(path)
                .thenCompose(x -> {
                    if (!x) return createZnode(path);
                    else return CompletableFuture.completedFuture(null);
                });
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data) {

        return checkExists(path)
                .thenCompose(x -> {
                    if (!x) return createZnode(path, data);
                    else return CompletableFuture.completedFuture(null);
                });
    }

    private static CompletableFuture<Void> createZnode(final String path, final byte[] data) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.create().creatingParentsIfNeeded().forPath(path, data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> null);
    }

    private static CompletableFuture<Void> createZnode(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.create().creatingParentsIfNeeded().forPath(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> null);
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

    public static void initialize(String connectionString) {
        client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
        client.start();
    }
}
