/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.Cache;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Create;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.SegmentRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.utils.ZKPaths;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
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
class ZKStream extends PersistentStreamBase<Integer> {
    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAM_PATH = SCOPE_PATH + "/%s";
    private static final String CREATION_TIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configuration";
    private static final String STATE_PATH = STREAM_PATH + "/state";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String HISTORY_PATH = STREAM_PATH + "/history";
    private static final String INDEX_PATH = STREAM_PATH + "/index";

    private static final String MARKER_PATH = STREAM_PATH + "/markers";

    private final ZKStoreHelper store;
    private final String creationPath;
    private final String configurationPath;
    private final String statePath;
    private final String segmentPath;
    private final String segmentChunkPathTemplate;
    private final String historyPath;
    private final String indexPath;
    private final String activeTxRoot;
    private final String completedTxPath;
    private final String markerPath;
    private final String scopePath;
    private final String streamPath;

    private final Cache<Integer> cache;

    public ZKStream(final String scopeName, final String streamName, ZKStoreHelper storeHelper) {
        super(scopeName, streamName);
        store = storeHelper;
        scopePath = String.format(SCOPE_PATH, scopeName);
        streamPath = String.format(STREAM_PATH, scopeName, streamName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, streamName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, streamName);
        statePath = String.format(STATE_PATH, scopeName, streamName);
        segmentPath = String.format(SEGMENT_PATH, scopeName, streamName);
        segmentChunkPathTemplate = segmentPath + "/%s";
        historyPath = String.format(HISTORY_PATH, scopeName, streamName);
        indexPath = String.format(INDEX_PATH, scopeName, streamName);
        activeTxRoot = String.format(ZKStoreHelper.STREAM_TX_ROOT, streamName);
        completedTxPath = String.format(ZKStoreHelper.COMPLETED_TX_PATH, streamName);

        markerPath = String.format(MARKER_PATH, scopeName, streamName);

        cache = new Cache<>(store::getData);
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        return store.getChildren(activeTxRoot).thenApply(list -> list == null ? 0 : list.size());
    }

    @Override
    public void refresh() {
        cache.invalidateAll();
    }

    @Override
    public CompletableFuture<Void> deleteStream() {
        return store.deleteTree(streamPath);
    }

    @Override
    public CompletableFuture<Void> checkStreamExists(final Create create) throws StreamAlreadyExistsException {
        return store.checkExists(creationPath)
                .thenCompose(x -> {
                    if (x) {
                        return cache.getCachedData(creationPath)
                                .thenApply(creationTime -> BitConverter.readLong(creationTime.getData(), 0) != create.getCreationTime());
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                })
                .thenAccept(x -> {
                    if (x) {
                        throw StoreException.create(StoreException.Type.NODE_EXISTS, creationPath);
                    }
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
                    if (!x) {
                        throw StoreException.create(StoreException.Type.NODE_NOT_FOUND);
                    }
                });
    }

    @Override
    CompletableFuture<Void> storeCreationTime(final Create create) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, create.getCreationTime());

        return store.createZNodeIfNotExist(creationPath, b);
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
    public CompletableFuture<Void> createIndexTable(final Data<Integer> indexTable) {
        return store.createZNodeIfNotExist(indexPath, indexTable.getData())
                .thenApply(x -> cache.invalidateCache(indexPath));
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Data<Integer> historyTable) {
        return store.createZNodeIfNotExist(historyPath, historyTable.getData())
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

        final byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                new byte[0],
                newRanges,
                create.getCreationTime()
        );

        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkFileName);
        return store.createZNodeIfNotExist(segmentChunkPath, segmentTable)
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return store.createZNodeIfNotExist(path, b)
                .thenAccept(x -> cache.invalidateCache(markerPath));
    }

    @Override
    CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<Integer> data) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));

        return store.setData(path, data)
                .thenAccept(x -> cache.invalidateCache(path));
    }

    @Override
    CompletableFuture<Data<Integer>> getMarkerData(int segmentNumber) {
        final CompletableFuture<Data<Integer>> result = new CompletableFuture<>();
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));
        cache.getCachedData(path)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ExceptionHelpers.getRealException(ex);
                        if (cause instanceof DataNotFoundException) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(cause);
                        }
                    } else {
                        result.complete(res);
                    }
                });

        return result;
    }

    @Override
    CompletableFuture<Void> removeMarkerData(int segmentNumber) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));

        return store.deletePath(path, false)
                .thenAccept(x -> cache.invalidateCache(path));
    }

    @Override
    public CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        CompletableFuture<Integer> epochFuture = getActiveEpoch().thenApply(Pair::getKey);
        return epochFuture.thenCompose(epoch -> store.getChildren(getEpochPath(epoch)))
                .thenCompose(txIds -> FutureHelpers.allOfWithResults(txIds.stream().collect(
                        Collectors.toMap(txId -> txId, txId -> cache.getCachedData(getActiveTxPath(epochFuture.join(), txId)))
                )));
    }

    @Override
    CompletableFuture<Integer> createNewTransaction(final UUID txId,
                                                 final long timestamp,
                                                 final long leaseExpiryTime,
                                                 final long maxExecutionExpiryTime,
                                                 final long scaleGracePeriod) {
        // TODO: retry is yet to be applied here
        return getLatestEpoch().thenCompose(pair -> {
            final String activePath = getActiveTxPath(pair.getKey(), txId.toString());
            final byte[] txnRecord = new ActiveTxnRecord(timestamp, leaseExpiryTime, maxExecutionExpiryTime,
                    scaleGracePeriod, TxnStatus.OPEN).toByteArray();
            return store.createZNodeIfNotExist(activePath, txnRecord)
                    .thenApply(x -> cache.invalidateCache(activePath))
                    .thenApply(y -> pair.getKey());
            // TODO: replace the previous create version with `createZNodeIfParentExists` method, once scale operation
            // creates epoch nodes
            //  return store.createZNodeIfParentExists(activePath, txnRecord)
            //          .thenApply(x -> cache.invalidateCache(activePath))
            //          .thenApply(y -> pair.getKey());
        });
    }

    @Override
    CompletableFuture<Integer> getTransactionEpoch(UUID txId) {
        return store.getChildren(activeTxRoot).thenCompose(list -> {
            Map<String, CompletableFuture<Boolean>> map = new HashMap<>();
            for (String str : list) {
                int epoch = Integer.parseInt(str);
                String activeTxnPath = getActiveTxPath(epoch, txId.toString());
                map.put(str, store.checkExists(activeTxnPath));
            }
            return FutureHelpers.allOfWithResults(map);
        }).thenApply(map -> {
            Optional<Map.Entry<String, Boolean>> opt = map.entrySet().stream().filter(Map.Entry::getValue).findFirst();
            if (opt.isPresent()) {
                return Integer.parseInt(opt.get().getKey());
            } else {
                return null;
            }
        });
    }

    @Override
    CompletableFuture<Pair<Integer, List<Integer>>> getLatestEpoch() {
        // TODO: this implementation needs to change once we do epoch change in history table
        return getActiveSegments().thenApply(list -> new ImmutablePair<>(0, list));
    }

    @Override
    CompletableFuture<Pair<Integer, List<Integer>>> getActiveEpoch() {
        return getActiveSegments().thenApply(list -> new ImmutablePair<>(0, list));
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final int epoch, final UUID txId) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return store.getData(activeTxPath);
    }

    @Override
    CompletableFuture<Void> updateActiveTx(final int epoch, final UUID txId, final byte[] data) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return store.updateTxnData(activeTxPath, data);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(final int epoch, final UUID txId, final boolean commit, final Optional<Integer> version) {
        final String activePath = getActiveTxPath(epoch, txId.toString());

        return getActiveTx(epoch, txId)
                .thenCompose(x -> {
                    if (version.isPresent() && version.get().intValue() != x.getVersion()) {
                        throw new WriteConflictException(txId.toString());
                    }
                    ActiveTxnRecord previous = ActiveTxnRecord.parse(x.getData());
                    ActiveTxnRecord updated = new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                            previous.getLeaseExpiryTime(),
                            previous.getMaxExecutionExpiryTime(),
                            previous.getScaleGracePeriod(),
                            commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING);
                    return store.setData(activePath, new Data<>(updated.toByteArray(), x.getVersion()));
                })
                .thenApply(x -> cache.invalidateCache(activePath));
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId) {
        return cache.getCachedData(getCompletedTxPath(txId.toString()));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        final String activePath = getActiveTxPath(epoch, txId.toString());
        // TODO: no need to check existence, just delete the path, if the path is already deleted, will get error
        return store.checkExists(activePath)
                .thenCompose(x -> {
                    if (x) {
                        return store.deletePath(activePath, true)
                                .thenAccept(y -> cache.invalidateCache(activePath));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp) {
        final String completedTxPath = getCompletedTxPath(txId.toString());
        return store.createZNodeIfNotExist(completedTxPath,
                new CompletedTxnRecord(timestamp, complete).toByteArray())
                .thenAccept(x -> cache.invalidateCache(completedTxPath));
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

    // endregion

    // region private helpers
    private String getActiveTxPath(final int epoch, final String txId) {
        return ZKPaths.makePath(ZKPaths.makePath(activeTxRoot, Integer.toString(epoch)), txId);
    }

    private String getEpochPath(final int epoch) {
        return ZKPaths.makePath(activeTxRoot, Integer.toString(epoch));
    }

    private String getCompletedTxPath(final String txId) {
        return ZKPaths.makePath(completedTxPath, txId);
    }
}
