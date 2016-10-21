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
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Utilities;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxStatus;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
    private static final String ROOT_PATH = "/streams";
    private static final String TRANSACTION_PATH = ROOT_PATH + "/transactions";

    private static final String STREAM_PATH = ROOT_PATH + "/%s";
    private static final String CREATIONTIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configurationPath";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segmentPath";
    private static final String HISTORY_PATH = STREAM_PATH + "/historyPath";
    private static final String INDEX_PATH = STREAM_PATH + "/indexPath";
    private static final String ACTIVE_TX_PATH = TRANSACTION_PATH + "/activeTxPath";
    private static final String COMPLETED_TX_PATH = TRANSACTION_PATH + "/completedTxPath";
    private static PathChildrenCache activeTxCache;
    private static PathChildrenCache completedTxCache;
    private static CuratorFramework client;

    private final NodeCache configurationCache;
    private final PathChildrenCache segmentCache;
    private final NodeCache indexCache;
    private final NodeCache historyCache;

    private final String creationPath;
    private final String configurationPath;
    private final String segmentPath;
    private final String segmentChunkPathTemplate;
    private final String historyPath;
    private final String indexPath;

    public ZKStream(final String name) {
        super(name);

        creationPath = String.format(CREATIONTIME_PATH, name);
        configurationPath = String.format(CONFIGURATION_PATH, name);
        segmentPath = String.format(SEGMENT_PATH, name);
        segmentChunkPathTemplate = segmentPath + "/%s";
        historyPath = String.format(HISTORY_PATH, name);
        indexPath = String.format(INDEX_PATH, name);

        segmentCache = new PathChildrenCache(client, segmentPath, true);
        indexCache = new NodeCache(client, indexPath);
        historyCache = new NodeCache(client, historyPath);
        configurationCache = new NodeCache(client, configurationPath);
    }

    public void init() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        futures.add(CompletableFuture.supplyAsync(() -> start(configurationCache)));
        futures.add(CompletableFuture.supplyAsync(() -> start(segmentCache)));
        futures.add(CompletableFuture.supplyAsync(() -> start(historyCache)));
        futures.add(CompletableFuture.supplyAsync(() -> start(indexCache)));

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void tearDown() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        futures.add(CompletableFuture.supplyAsync(() -> close(configurationCache)));
        futures.add(CompletableFuture.supplyAsync(() -> close(segmentCache)));
        futures.add(CompletableFuture.supplyAsync(() -> close(historyCache)));
        futures.add(CompletableFuture.supplyAsync(() -> close(indexCache)));

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        client.close();
    }

    @Override
    public CompletableFuture<Boolean> createStream(Create create) throws StreamAlreadyExistsException {
        return checkExists(creationPath)
                .thenCompose(x -> {
                    if (x) {
                        return getData(creationPath).thenApply(y -> Utilities.toLong(y) == create.getEventTime());
                    } else
                        return createZnode(creationPath, Utilities.toByteArray(create.getEventTime())).thenApply(z -> true);
                })
                .thenApply(x -> {
                    if (!x)
                        throw new StreamAlreadyExistsException(getName());

                    return true;
                });
    }

    @Override
    public CompletableFuture<Void> createConfiguration(final Create create) {
        return createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(create.getConfiguration()));
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return createZNodeIfNotExist(segmentPath);
    }

    @Override
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final byte[] data) {
        return createZNodeIfNotExist(String.format(segmentChunkPathTemplate, chunkNumber), data);
    }

    @Override
    public CompletableFuture<Void> createIndexTable(final Create create) {
        final byte[] indexTable = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
        return createZNodeIfNotExist(indexPath, indexTable);
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Create create) {
        final int numSegments = create.getConfiguration().getScalingingPolicy().getMinNumSegments();
        final byte[] historyTable = TableHelper.updateHistoryTable(new byte[0],
                create.getEventTime(),
                IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

        return createZNodeIfNotExist(historyPath, historyTable);
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final byte[] updated) {
        return setData(historyPath, updated).thenApply(x -> null);
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

        return createZNodeIfNotExist(String.format(segmentChunkPathTemplate, chunkFileName), segmentTable);
    }

    @Override
    CompletableFuture<Void> createNewTransaction(UUID txId, long timestamp) {
        return createZNodeIfNotExist(createActiveTxPath(txId.toString()),
                new ActiveTxRecord(timestamp, TxStatus.OPEN).toByteArray());
    }

    @Override
    CompletableFuture<ActiveTxRecord> getActiveTx(UUID txId) {
        return getData(activeTxCache, createActiveTxPath(txId.toString()))
                .thenApply(ActiveTxRecord::parse);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(UUID txId) {
        return getActiveTx(txId)
                .thenCompose(x -> setData(createActiveTxPath(txId.toString()),
                        new ActiveTxRecord(x.getTxCreationTimestamp(), TxStatus.SEALED).toByteArray()))
                .thenApply(x -> null);
    }

    @Override
    CompletableFuture<CompletedTxRecord> getCompletedTx(UUID txId) {
        return getData(completedTxCache, createCompletedTxPath(txId.toString()))
                .thenApply(CompletedTxRecord::parse);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(UUID txId) {
        return deletePath(createActiveTxPath(txId.toString()));
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, TxStatus complete, long timestamp) {
        return createZNodeIfNotExist(createCompletedTxPath(txId.toString()),
                new CompletedTxRecord(timestamp, complete).toByteArray());
    }

    @Override
    public CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration) {
        return setData(configurationPath, SerializationUtils.serialize(configuration))
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        return getData(configurationCache, configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x));
    }

    @Override
    public CompletableFuture<Segment> getSegmentRow(final int number) {
        // compute the file name based on segment number
        final int znodeName = number / SegmentRecord.SEGMENT_CHUNK_SIZE;

        return getData(segmentCache, String.format(segmentChunkPathTemplate, znodeName))
                .thenApply(x -> TableHelper.getSegment(number, x));
    }

    @Override
    public CompletableFuture<byte[]> getSegmentTableChunk(final int chunkNumber) {
        return getData(segmentCache, String.format(segmentChunkPathTemplate, chunkNumber));
    }

    @Override
    CompletableFuture<Void> setSegmentTableChunk(final int chunkNumber, final byte[] data) {
        return setData(String.format(segmentChunkPathTemplate, chunkNumber), data).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<List<String>> getSegmentChunks() {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return client.getChildren().forPath(segmentPath);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public CompletableFuture<byte[]> getHistoryTable() {
        return getData(historyCache, historyPath);
    }

    @Override
    public CompletableFuture<byte[]> getIndexTable() {
        return getData(indexCache, indexPath);
    }

    @Override
    CompletableFuture<Void> updateIndexTable(final byte[] updated) {
        return setData(indexPath, updated).thenApply(x -> null);
    }

    private Void start(final NodeCache cache) {
        try {
            cache.start();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void start(final PathChildrenCache cache) {
        try {
            cache.start();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void close(final NodeCache cache) {
        try {
            cache.close();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void close(final PathChildrenCache cache) {
        try {
            cache.close();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String createActiveTxPath(String txId) {
        return ACTIVE_TX_PATH + "/" + getName() + "_" + txId;
    }

    private String createCompletedTxPath(String txId) {
        return ACTIVE_TX_PATH + "/" + getName() + "_" + txId;
    }

    static CompletableFuture<Map<String, ActiveTxRecord>> getAllActiveTx() {
        return getChildrenData(activeTxCache, ACTIVE_TX_PATH)
                .thenApply(x -> x.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, z -> ActiveTxRecord.parse(z.getValue()))));
    }

    static CompletableFuture<Map<String, CompletedTxRecord>> getAllCompletedTx() {
        return getChildrenData(completedTxCache, COMPLETED_TX_PATH)
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

    private static CompletableFuture<byte[]> getData(final NodeCache cache, final String path) {
        if (cache.getCurrentData() != null)
            return CompletableFuture.completedFuture(cache.getCurrentData().getData());
        else return getData(path);
    }

    private static CompletableFuture<byte[]> getData(final PathChildrenCache cache, final String path) {
        if (cache.getCurrentData(path) != null)
            return CompletableFuture.completedFuture(cache.getCurrentData(path).getData());
        else return getData(path);
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

    private static CompletableFuture<Map<String, byte[]>> getChildrenData(final PathChildrenCache cache, final String path) {
        if (cache.getCurrentData() != null)
            return CompletableFuture.completedFuture(cache.getCurrentData()
                    .stream()
                    .collect(Collectors.toMap(ChildData::getPath, ChildData::getData)));
        else {
            try {
                Map<String, CompletableFuture<byte[]>> z = client.getChildren().forPath(path)
                        .stream()
                        .collect(Collectors.toMap(x -> x, ZKStream::getData));
                return FutureCollectionHelper.sequenceMap(z);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static CompletableFuture<Stat> setData(final String path, final byte[] data) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x) {
                        try {
                            return client.setData().forPath(path, data);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else throw new RuntimeException(String.format("path %s not Found", path));
                });
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path) {
        return checkExists(path)
                .thenCompose(x -> {
                    if (!x) {
                        try {
                            return createZnode(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else return CompletableFuture.completedFuture(null);
                });
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data) {

        return checkExists(path)
                .thenCompose(
                        x -> {
                            if (!x) {
                                try {
                                    return createZnode(path, data);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            } else return CompletableFuture.completedFuture(null);
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
        activeTxCache = new PathChildrenCache(client, ACTIVE_TX_PATH, true);
        completedTxCache = new PathChildrenCache(client, COMPLETED_TX_PATH, true);

        try {
            activeTxCache.start();
            completedTxCache.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
