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
import com.emc.pravega.controller.store.stream.tables.Cache;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.Data;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Utilities;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
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
@Slf4j
class ZKStream extends PersistentStreamBase<Integer> {
    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAM_PATH = SCOPE_PATH + "/%s";
    private static final String CREATION_TIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configuration";
    private static final String STATE_PATH = STREAM_PATH + "/state";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String HISTORY_PATH = STREAM_PATH + "/history";
    private static final String INDEX_PATH = STREAM_PATH + "/index";

    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    private static final String ACTIVE_TX_PATH = ACTIVE_TX_ROOT_PATH + "/%s";
    private static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    private static final String COMPLETED_TX_PATH = COMPLETED_TX_ROOT_PATH + "/%s";

    private static CuratorFramework client;

    private final String creationPath;
    private final String configurationPath;
    private final String statePath;
    private final String segmentPath;
    private final String segmentChunkPathTemplate;
    private final String historyPath;
    private final String indexPath;
    private final String activeTxPath;
    private final String completedTxPath;
    private final String scopePath;
    private final Cache<Integer> cache;


    public ZKStream(final String scopeName, final String streamName) {
        super(scopeName, streamName);

        scopePath = String.format(SCOPE_PATH, scopeName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, streamName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, streamName);
        statePath = String.format(STATE_PATH, scopeName, streamName);
        segmentPath = String.format(SEGMENT_PATH, scopeName, streamName);
        segmentChunkPathTemplate = segmentPath + "/%s";
        historyPath = String.format(HISTORY_PATH, scopeName, streamName);
        indexPath = String.format(INDEX_PATH, scopeName, streamName);
        activeTxPath = String.format(ACTIVE_TX_PATH, scopeName, streamName);
        completedTxPath = String.format(COMPLETED_TX_PATH, scopeName, streamName);

        cache = new Cache<>(ZKStream::getData);
    }

    @Override
    public CompletableFuture<Boolean> isTransactionOngoing() {
        return getChildren(activeTxPath).thenApply(list -> list != null && !list.isEmpty());
    }

    @Override
    public void refresh() {
        cache.invalidateAll();
    }

    @Override
    public CompletableFuture<Void> checkStreamExists(final Create create) throws StreamAlreadyExistsException {

        return checkExists(creationPath)
                .thenCompose(x -> {
                    if (x) {
                        return cache.getCachedData(creationPath)
                                .thenApply(creationTime -> Utilities.toLong(creationTime.getData()) != create.getEventTime());
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                })
                .thenApply(x -> {
                    if (x) {
                        throw new StreamAlreadyExistsException(getName());
                    }
                    return null;
                });
    }

    public CompletableFuture<Void> checkScopeExists() {
        return checkExists(scopePath)
                .thenApply(x -> {
                    if (x) {
                        return null;
                    } else {
                        log.debug("Scope with name {} not found.", getScopeName());
                        throw new ScopeNotFoundException(getScopeName());
                    }
                });
    }

    @Override
    CompletableFuture<Void> storeCreationTime(final Create create) {
        return createZNodeIfNotExist(creationPath, Utilities.toByteArray(create.getEventTime()));
    }

    @Override
    public CompletableFuture<Void> createConfiguration(final Create create) {
        return createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(create.getConfiguration()))
                .thenApply(x -> cache.invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<Void> createState(final State state) {
        return createZNodeIfNotExist(statePath, SerializationUtils.serialize(state))
                .thenApply(x -> cache.invalidateCache(statePath));
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return createZNodeIfNotExist(segmentPath).thenApply(x -> cache.invalidateCache(segmentPath));
    }

    @Override
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final Data<Integer> data) {
        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkNumber);
        return createZNodeIfNotExist(segmentChunkPath, data.getData())
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<Void> createIndexTable(final Create create) {
        final byte[] indexTable = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
        return createZNodeIfNotExist(indexPath, indexTable)
                .thenApply(x -> cache.invalidateCache(indexPath));
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final byte[] historyTable = TableHelper.updateHistoryTable(new byte[0],
                create.getEventTime(),
                IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

        return createZNodeIfNotExist(historyPath, historyTable)
                .thenApply(x -> cache.invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final Data<Integer> updated) {
        return setData(historyPath, updated).thenApply(x -> cache.invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> createSegmentFile(final Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
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

        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkFileName);
        return createZNodeIfNotExist(segmentChunkPath, segmentTable)
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    CompletableFuture<Void> createNewTransaction(final UUID txId, final long timestamp) {
        final String activePath = getActiveTxPath(txId.toString());
        return createZNodeIfNotExist(activePath,
                new ActiveTxRecord(timestamp, TxnStatus.OPEN).toByteArray())
                .thenApply(x -> cache.invalidateCache(activePath));
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final UUID txId) {
        final String activeTxPath = getActiveTxPath(txId.toString());

        return getData(activeTxPath);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(final UUID txId) {
        final String activePath = getActiveTxPath(txId.toString());

        return getActiveTx(txId)
                .thenCompose(x -> {
                    ActiveTxRecord previous = ActiveTxRecord.parse(x.getData());
                    ActiveTxRecord updated = new ActiveTxRecord(previous.getTxCreationTimestamp(), TxnStatus.SEALED);
                    return setData(activePath, new Data<>(updated.toByteArray(), x.getVersion()));
                })
                .thenApply(x -> cache.invalidateCache(activePath));
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId) {
        return cache.getCachedData(getCompletedTxPath(txId.toString()));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final UUID txId) {
        final String activePath = getActiveTxPath(txId.toString());
        return checkExists(activePath)
                .thenCompose(x -> {
                    if (x) {
                        return deletePath(activePath, true);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp) {
        final String completedTxPath = getCompletedTxPath(txId.toString());
        return createZNodeIfNotExist(completedTxPath,
                new CompletedTxRecord(timestamp, complete).toByteArray())
                .thenApply(x -> cache.invalidateCache(completedTxPath));
    }

    @Override
    public CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration) {
        return setData(configurationPath, new Data<>(SerializationUtils.serialize(configuration), null))
                .thenApply(x -> cache.invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        return cache.getCachedData(configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x.getData()));
    }

    @Override
    CompletableFuture<Void> setStateData(final State state) {
        return setData(statePath, new Data<>(SerializationUtils.serialize(state), null))
                .thenApply(x -> cache.invalidateCache(statePath));
    }

    @Override
    CompletableFuture<State> getStateData() {
        return cache.getCachedData(statePath)
                .thenApply(x -> (State) SerializationUtils.deserialize(x.getData()));
    }

    @Override
    public CompletableFuture<Segment> getSegmentRow(final int number) {
        // compute the file name based on segment number
        final int chunkNumber = number / SegmentRecord.SEGMENT_CHUNK_SIZE;

        return getSegmentTableChunk(chunkNumber)
                .thenApply(x -> TableHelper.getSegment(number, x.getData()));
    }

    @Override
    public CompletableFuture<Data<Integer>> getSegmentTableChunk(final int chunkNumber) {
        return cache.getCachedData(String.format(segmentChunkPathTemplate, chunkNumber));
    }

    @Override
    CompletableFuture<Void> setSegmentTableChunk(final int chunkNumber, final Data<Integer> data) {
        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkNumber);
        return setData(segmentChunkPath, data)
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<List<String>> getSegmentChunks() {
        return getChildren(segmentPath);
    }

    @Override
    public CompletableFuture<Data<Integer>> getHistoryTable() {
        return cache.getCachedData(historyPath);
    }

    @Override
    public CompletableFuture<Data<Integer>> getIndexTable() {
        return cache.getCachedData(indexPath);
    }

    @Override
    CompletableFuture<Void> updateIndexTable(final Data<Integer> updated) {
        return setData(indexPath, updated).thenApply(x -> cache.invalidateCache(indexPath));
    }

    private String getActiveTxPath(final String txId) {
        return ZKPaths.makePath(activeTxPath, txId);
    }

    private String getCompletedTxPath(final String txId) {
        return ZKPaths.makePath(completedTxPath, txId);
    }

    static CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
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

    static CompletableFuture<Map<String, Data<Integer>>> getAllCompletedTx() {
        return getAllTransactionData(COMPLETED_TX_ROOT_PATH)
                .thenApply(x -> x.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public static void initialize(final CuratorFramework cf) {
        client = cf;
    }

    public static CompletableFuture<Void> deletePath(final String path, final boolean deleteEmptyContainer) {
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

    private static CompletableFuture<List<String>> getChildrenPath(final String rootPath) {
        return getChildren(rootPath)
                .thenApply(children -> children.stream().map(x -> ZKPaths.makePath(rootPath, x)).collect(Collectors.toList()));
    }

    private static CompletableFuture<List<String>> getChildren(final String path) {
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

    private static CompletableFuture<Map<String, Data<Integer>>> getAllTransactionData(final String rootPath) {
        return getChildrenPath(rootPath) // list of all streams for either active or completed tx based on root path
                .thenApply(x -> x.stream()
                        .map(ZKStream::getChildrenPath) // get all transactions on the stream
                        .collect(Collectors.toList()))
                .thenCompose(FutureCollectionHelper::sequence)
                .thenApply(z -> z.stream().flatMap(Collection::stream).collect(Collectors.toList())) // flatten list<list> to list
                .thenApply(x -> x.stream()
                        .collect(Collectors.toMap(z -> z, ZKStream::getData)))
                .thenCompose(FutureCollectionHelper::sequenceMap); // convert Map<string, future<Data>> to future<map<String, Data>>
    }

    private static CompletableFuture<Void> setData(final String path, final Data<Integer> data) {
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

    private static String extractStreamName(final String path) {
        Preconditions.checkNotNull(path, "path");
        String[] result = path.split("/");
        if (result.length > 2) {
            return result[2];
        } else {
            return path;
        }
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data) {
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
}
