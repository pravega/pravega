/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
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

import java.util.AbstractMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.utils.ZKPaths;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 * <p>
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
class ZKStream extends PersistentStreamBase<Integer> {
    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAM_PATH = SCOPE_PATH + "/%s";
    private static final String CREATION_TIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configuration";
    private static final String STATE_PATH = STREAM_PATH + "/state";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String HISTORY_PATH = STREAM_PATH + "/history";
    private static final String INDEX_PATH = STREAM_PATH + "/index";

    private final ZKStreamStoreHelper store;
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

    public ZKStream(final String scopeName, final String streamName, ZKStreamStoreHelper storeHelper) {
        super(scopeName, streamName);
        store = storeHelper;
        scopePath = String.format(SCOPE_PATH, scopeName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, streamName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, streamName);
        statePath = String.format(STATE_PATH, scopeName, streamName);
        segmentPath = String.format(SEGMENT_PATH, scopeName, streamName);
        segmentChunkPathTemplate = segmentPath + "/%s";
        historyPath = String.format(HISTORY_PATH, scopeName, streamName);
        indexPath = String.format(INDEX_PATH, scopeName, streamName);
        activeTxPath = String.format(ZKStreamStoreHelper.ACTIVE_TX_PATH, streamName);
        completedTxPath = String.format(ZKStreamStoreHelper.COMPLETED_TX_PATH, streamName);

        cache = new Cache<>(store::getData);
    }

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        return store.getChildren(activeTxPath).thenApply(list -> {
            return list == null ? 0 : list.size();
        });
    }

    @Override
    public void refresh() {
        cache.invalidateAll();
    }

    @Override
    public CompletableFuture<Void> checkStreamExists(final Create create) throws StreamAlreadyExistsException {

        return store.checkExists(creationPath)
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
                        StoreException.create(StoreException.Type.NODE_EXISTS, creationPath);
                    }
                    return null;
                });
    }

    /**
     * Method to check whether a scope exists before creating a stream under that scope.
     *
     * @return A future either returning a result or an exception.
     */
    public CompletableFuture<Void> checkScopeExists() {
        return store.checkExists(scopePath)
                .thenAccept(x -> {
                    if (x) {
                        return;
                    } else {
                        StoreException.create(StoreException.Type.NODE_NOT_FOUND);
                    }
                });
    }

    @Override
    CompletableFuture<Void> storeCreationTime(final Create create) {
        return store.createZNodeIfNotExist(creationPath, Utilities.toByteArray(create.getEventTime()));
    }

    @Override
    public CompletableFuture<Void> createConfiguration(final Create create) {
        return store.createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(create.getConfiguration()))
                .thenApply(x -> cache.invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<Void> createState(final State state) {
        return store.createZNodeIfNotExist(statePath, SerializationUtils.serialize(state))
                .thenApply(x -> cache.invalidateCache(statePath));
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return store.createZNodeIfNotExist(segmentPath).thenApply(x -> cache.invalidateCache(segmentPath));
    }

    @Override
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final Data<Integer> data) {
        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkNumber);
        return store.createZNodeIfNotExist(segmentChunkPath, data.getData())
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<Void> createIndexTable(final Create create) {
        final byte[] indexTable = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
        return store.createZNodeIfNotExist(indexPath, indexTable)
                .thenApply(x -> cache.invalidateCache(indexPath));
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final byte[] historyTable = TableHelper.updateHistoryTable(new byte[0],
                create.getEventTime(),
                IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

        return store.createZNodeIfNotExist(historyPath, historyTable)
                .thenApply(x -> cache.invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final Data<Integer> updated) {
        return store.setData(historyPath, updated).thenApply(x -> cache.invalidateCache(historyPath));
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
        return store.createZNodeIfNotExist(segmentChunkPath, segmentTable)
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    CompletableFuture<Void> createNewTransaction(final UUID txId, final long timestamp) {
        final String activePath = getActiveTxPath(txId.toString());
        return store.createZNodeIfNotExist(activePath,
                new ActiveTxRecord(timestamp, TxnStatus.OPEN).toByteArray())
                .thenApply(x -> cache.invalidateCache(activePath));
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final UUID txId) {
        final String activeTxPath = getActiveTxPath(txId.toString());

        return store.getData(activeTxPath);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(final UUID txId) {
        final String activePath = getActiveTxPath(txId.toString());

        return getActiveTx(txId)
                .thenCompose(x -> {
                    ActiveTxRecord previous = ActiveTxRecord.parse(x.getData());
                    ActiveTxRecord updated = new ActiveTxRecord(previous.getTxCreationTimestamp(), TxnStatus.SEALED);
                    return store.setData(activePath, new Data<>(updated.toByteArray(), x.getVersion()));
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
        return store.checkExists(activePath)
                .thenCompose(x -> {
                    if (x) {
                        return store.deletePath(activePath, true);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp) {
        final String completedTxPath = getCompletedTxPath(txId.toString());
        return store.createZNodeIfNotExist(completedTxPath,
                new CompletedTxRecord(timestamp, complete).toByteArray())
                .thenApply(x -> cache.invalidateCache(completedTxPath));
    }

    @Override
    public CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration) {
        return store.setData(configurationPath, new Data<>(SerializationUtils.serialize(configuration), null))
                .thenApply(x -> cache.invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        return cache.getCachedData(configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x.getData()));
    }

    @Override
    CompletableFuture<Void> setStateData(final State state) {
        return store.setData(statePath, new Data<>(SerializationUtils.serialize(state), null))
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
        return store.setData(segmentChunkPath, data)
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<List<String>> getSegmentChunks() {
        return store.getChildren(segmentPath);
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
        return store.setData(indexPath, updated).thenApply(x -> cache.invalidateCache(indexPath));
    }

    private String getActiveTxPath(final String txId) {
        return ZKPaths.makePath(activeTxPath, txId);
    }

    private String getCompletedTxPath(final String txId) {
        return ZKPaths.makePath(completedTxPath, txId);
    }

}
