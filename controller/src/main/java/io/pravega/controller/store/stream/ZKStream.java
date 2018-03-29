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

import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.Cache;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
    private static final String TRUNCATION_PATH = STREAM_PATH + "/truncation";
    private static final String STATE_PATH = STREAM_PATH + "/state";
    private static final String SEGMENT_INDEX_PATH = STREAM_PATH + "/segmentIndex";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String HISTORY_PATH = STREAM_PATH + "/history";
    private static final String HISTORY_INDEX_PATH = STREAM_PATH + "/index";
    private static final String EPOCH_TRANSITION_PATH = STREAM_PATH + "/epochTransition";
    private static final String RETENTION_PATH = STREAM_PATH + "/retention";
    private static final String SEALED_SEGMENTS_PATH = STREAM_PATH + "/sealedSegments";
    private static final String MARKER_PATH = STREAM_PATH + "/markers";

    private final ZKStoreHelper store;
    private final String creationPath;
    private final String configurationPath;
    private final String truncationPath;
    private final String statePath;
    private final String segmentIndexPath;
    private final String segmentPath;
    private final String historyPath;
    private final String historyIndexPath;
    private final String retentionPath;
    private final String epochTransitionPath;
    private final String sealedSegmentsPath;
    private final String activeTxRoot;
    private final String completedTxPath;
    private final String markerPath;
    private final String scopePath;
    @Getter(AccessLevel.PACKAGE)
    private final String streamPath;

    private final Cache<Integer> cache;

    ZKStream(final String scopeName, final String streamName, ZKStoreHelper storeHelper) {
        super(scopeName, streamName);
        store = storeHelper;
        scopePath = String.format(SCOPE_PATH, scopeName);
        streamPath = String.format(STREAM_PATH, scopeName, streamName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, streamName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, streamName);
        truncationPath = String.format(TRUNCATION_PATH, scopeName, streamName);
        statePath = String.format(STATE_PATH, scopeName, streamName);
        segmentPath = String.format(SEGMENT_PATH, scopeName, streamName);
        segmentIndexPath = String.format(SEGMENT_INDEX_PATH, scopeName, streamName);
        historyPath = String.format(HISTORY_PATH, scopeName, streamName);
        historyIndexPath = String.format(HISTORY_INDEX_PATH, scopeName, streamName);
        retentionPath = String.format(RETENTION_PATH, scopeName, streamName);
        epochTransitionPath = String.format(EPOCH_TRANSITION_PATH, scopeName, streamName);
        sealedSegmentsPath = String.format(SEALED_SEGMENTS_PATH, scopeName, streamName);
        activeTxRoot = String.format(ZKStoreHelper.STREAM_TX_ROOT, scopeName, streamName);
        completedTxPath = String.format(ZKStoreHelper.COMPLETED_TX_PATH, scopeName, streamName);
        markerPath = String.format(MARKER_PATH, scopeName, streamName);

        cache = new Cache<>(store::getData);
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        return store.getChildren(activeTxRoot).thenCompose(list ->
                Futures.allOfWithResults(list.stream().map(epoch ->
                        getNumberOfOngoingTransactions(Integer.parseInt(epoch))).collect(Collectors.toList())))
                .thenApply(list -> list.stream().reduce(0, Integer::sum));
    }

    private CompletableFuture<Integer> getNumberOfOngoingTransactions(int epoch) {
        return store.getChildren(getEpochPath(epoch)).thenApply(List::size);
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
    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime) {
        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier,
        return store.checkExists(creationPath).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                        configuration, creationTime));
            }

            return getCreationTime().thenCompose(storedCreationTime ->
                    store.checkExists(configurationPath).thenCompose(configExists -> {
                        if (configExists) {
                            return handleConfigExists(storedCreationTime, storedCreationTime == creationTime);
                        } else {
                            return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                    configuration, storedCreationTime));
                        }
                    }));
        });
    }

    private CompletableFuture<CreateStreamResponse> handleConfigExists(long creationTime, boolean creationTimeMatched) {
        CreateStreamResponse.CreateStatus status = creationTimeMatched ?
                CreateStreamResponse.CreateStatus.NEW : CreateStreamResponse.CreateStatus.EXISTS_CREATING;

        return getConfiguration().thenCompose(config -> store.checkExists(statePath)
                .thenCompose(stateExists -> {
                    if (!stateExists) {
                        return CompletableFuture.completedFuture(new CreateStreamResponse(status, config, creationTime));
                    }

                    return getState(false).thenApply(state -> {
                        if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                            return new CreateStreamResponse(status, config, creationTime);
                        } else {
                            return new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE,
                                    config, creationTime);
                        }
                    });
                }));
    }

    private CompletableFuture<Long> getCreationTime() {
        return cache.getCachedData(creationPath)
                .thenApply(data -> BitConverter.readLong(data.getData(), 0));
    }

    /**
     * Method to check whether a scope exists before creating a stream under that scope.
     *
     * @return A future either returning a result or an exception.
     */
    @Override
    public CompletableFuture<Void> checkScopeExists() {
        return store.checkExists(scopePath)
                .thenAccept(x -> {
                    if (!x) {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopePath);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createSealedSegmentsRecord(byte[] sealedSegmentsRecord) {
        return store.createZNodeIfNotExist(sealedSegmentsPath, sealedSegmentsRecord);
    }

    @Override
    CompletableFuture<Data<Integer>> getSealedSegmentsRecord() {
        return store.getData(sealedSegmentsPath);
    }

    @Override
    CompletableFuture<Void> updateSealedSegmentsRecord(Data<Integer> update) {
        return store.setData(sealedSegmentsPath, update);
    }

    @Override
    CompletableFuture<Void> createRetentionSet(byte[] retention) {
        return store.createZNodeIfNotExist(retentionPath, retention);
    }

    @Override
    CompletableFuture<Data<Integer>> getRetentionSet() {
        return store.getData(retentionPath);
    }

    @Override
    CompletableFuture<Void> updateRetentionSet(Data<Integer> retention) {
        return store.setData(retentionPath, retention);
    }

    @Override
    CompletableFuture<Void> createEpochTransitionNode(byte[] epochTransition) {
        return store.createZNode(epochTransitionPath, epochTransition)
                .thenApply(x -> cache.invalidateCache(epochTransitionPath));
    }

    @Override
    CompletableFuture<Data<Integer>> getEpochTransitionNode() {
        cache.invalidateCache(epochTransitionPath);
        return cache.getCachedData(epochTransitionPath);
    }

    @Override
    CompletableFuture<Void> deleteEpochTransitionNode() {
        return store.deleteNode(epochTransitionPath);
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return store.createZNodeIfNotExist(creationPath, b)
            .thenApply(x -> cache.invalidateCache(creationPath));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final StreamProperty<StreamConfiguration> configuration) {
        return store.createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(configuration))
                .thenApply(x -> cache.invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final State state) {
        return store.createZNodeIfNotExist(statePath, SerializationUtils.serialize(state))
                .thenApply(x -> cache.invalidateCache(statePath));
    }

    @Override
    public CompletableFuture<Void> createSegmentTableIfAbsent(final Data<Integer> segmentTable) {

        return store.createZNodeIfNotExist(segmentPath, segmentTable.getData())
                .thenApply(x -> cache.invalidateCache(segmentPath));
    }

    @Override
    public CompletableFuture<Void> createHistoryIndexIfAbsent(final Data<Integer> indexTable) {
        return store.createZNodeIfNotExist(historyIndexPath, indexTable.getData())
                .thenApply(x -> cache.invalidateCache(historyIndexPath));
    }

    @Override
    public CompletableFuture<Void> createHistoryTableIfAbsent(final Data<Integer> historyTable) {
        return store.createZNodeIfNotExist(historyPath, historyTable.getData())
                .thenApply(x -> cache.invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final Data<Integer> updated) {
        return store.setData(historyPath, updated)
                .whenComplete((r, e) -> cache.invalidateCache(historyPath));
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
                .whenComplete((r, e) -> cache.invalidateCache(path));
    }

    @Override
    CompletableFuture<Data<Integer>> getMarkerData(int segmentNumber) {
        final CompletableFuture<Data<Integer>> result = new CompletableFuture<>();
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));
        cache.getCachedData(path)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = Exceptions.unwrap(ex);
                        if (cause instanceof StoreException.DataNotFoundException) {
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
                .whenComplete((r, e) -> cache.invalidateCache(path));
    }

    @Override
    public CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        return getActiveEpoch(false)
                .thenCompose(epoch -> store.getChildren(getEpochPath(epoch.getKey()))
                        .thenCompose(txIds -> Futures.allOfWithResults(txIds.stream().collect(
                                Collectors.toMap(txId -> txId, txId -> cache.getCachedData(getActiveTxPath(epoch.getKey(), txId))))
                        )));
    }

    @Override
    CompletableFuture<Integer> createNewTransaction(final UUID txId,
                                                    final long timestamp,
                                                    final long leaseExpiryTime,
                                                    final long maxExecutionExpiryTime,
                                                    final long scaleGracePeriod) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        createNewTransactionNode(txId, timestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod)
                .whenComplete((value, ex) -> {
                    if (ex != null) {
                        if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                            Futures.completeAfter(() -> createNewTransactionNode(txId, timestamp, leaseExpiryTime,
                                    maxExecutionExpiryTime, scaleGracePeriod), future);
                        } else {
                            future.completeExceptionally(ex);
                        }
                    } else {
                        future.complete(value);
                    }
                });
        return future;
    }

    private CompletableFuture<Integer> createNewTransactionNode(final UUID txId,
                                                                final long timestamp,
                                                                final long leaseExpiryTime,
                                                                final long maxExecutionExpiryTime,
                                                                final long scaleGracePeriod) {
        return getLatestEpoch().thenCompose(pair -> {
            final String activePath = getActiveTxPath(pair.getKey(), txId.toString());
            final byte[] txnRecord = new ActiveTxnRecord(timestamp, leaseExpiryTime, maxExecutionExpiryTime,
                    scaleGracePeriod, TxnStatus.OPEN).toByteArray();
            return store.createZNodeIfNotExist(activePath, txnRecord, false)
                    .thenApply(x -> cache.invalidateCache(activePath))
                    .thenApply(y -> pair.getKey());
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
            return Futures.allOfWithResults(map);
        }).thenApply(map -> {
            Optional<Map.Entry<String, Boolean>> opt = map.entrySet().stream().filter(Map.Entry::getValue).findFirst();
            if (opt.isPresent()) {
                return Integer.parseInt(opt.get().getKey());
            } else {
                throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString());
            }
        });
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final int epoch, final UUID txId) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return store.getData(activeTxPath);
    }

    @Override
    CompletableFuture<Void> updateActiveTx(final int epoch, final UUID txId, final Data<Integer> data) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return store.setData(activeTxPath, data).whenComplete((r, e) -> cache.invalidateCache(activeTxPath));
    }

    @Override
    CompletableFuture<Void> sealActiveTx(final int epoch, final UUID txId, final boolean commit,
                                         final ActiveTxnRecord previous, final int version) {
        final String activePath = getActiveTxPath(epoch, txId.toString());
        final ActiveTxnRecord updated = new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                            previous.getLeaseExpiryTime(),
                            previous.getMaxExecutionExpiryTime(),
                            previous.getScaleGracePeriod(),
                            commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING);
        final Data<Integer> data = new Data<>(updated.toByteArray(), version);
        return store.setData(activePath, data).thenApply(x -> cache.invalidateCache(activePath))
                            .whenComplete((r, e) -> cache.invalidateCache(activePath));
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
                        return store.deletePath(activePath, false)
                                .whenComplete((r, e) -> cache.invalidateCache(activePath));
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
                .whenComplete((r, e) -> cache.invalidateCache(completedTxPath));
    }

    @Override
    public CompletableFuture<Void> createTruncationDataIfAbsent(final StreamProperty<StreamTruncationRecord> truncationRecord) {
        return store.createZNodeIfNotExist(truncationPath, SerializationUtils.serialize(truncationRecord))
                .thenApply(x -> cache.invalidateCache(truncationPath));
    }

    @Override
    CompletableFuture<Void> setTruncationData(final Data<Integer> truncationRecord) {
        return store.setData(truncationPath, truncationRecord)
                .whenComplete((r, e) -> cache.invalidateCache(truncationPath));
    }

    @Override
    CompletableFuture<Data<Integer>> getTruncationData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(truncationPath);
        }

        return cache.getCachedData(truncationPath);
    }

    @Override
    CompletableFuture<Void> setConfigurationData(final Data<Integer> configuration) {
        return store.setData(configurationPath, configuration)
                .whenComplete((r, e) -> cache.invalidateCache(configurationPath));
    }

    @Override
    CompletableFuture<Data<Integer>> getConfigurationData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(configurationPath);
        }

        return cache.getCachedData(configurationPath);
    }

    @Override
    CompletableFuture<Void> setStateData(final Data<Integer> state) {
        return store.setData(statePath, state)
                .whenComplete((r, e) -> cache.invalidateCache(statePath));
    }

    @Override
    CompletableFuture<Data<Integer>> getStateData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(statePath);
        }

        return cache.getCachedData(statePath);
    }

    @Override
    CompletableFuture<Void> createSegmentIndexIfAbsent(Data<Integer> data) {
        // what if index was created in an earlier attempt but segment table was not.
        // if segment table exists, index should definitely exist. if segment table does not exist,
        // delete any existing index and create again so that it is guaranteed to match new segment table we are about to create.
        return store.checkExists(segmentPath)
                .thenCompose(exists -> {
                    if (!exists) {
                        return store.deletePath(segmentIndexPath, false)
                            .thenCompose(v -> store.createZNodeIfNotExist(segmentIndexPath, data.getData()));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenApply(x -> cache.invalidateCache(segmentIndexPath));
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentIndex() {
        return cache.getCachedData(segmentIndexPath);
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentIndexFromStore() {
        cache.invalidateCache(segmentIndexPath);
        return getSegmentIndex();
    }

    @Override
    CompletableFuture<Void> updateSegmentIndex(Data<Integer> data) {
        return store.setData(segmentIndexPath, data)
                .whenComplete((r, e) -> cache.invalidateCache(segmentIndexPath));
    }

    @Override
    public CompletableFuture<Segment> getSegmentRow(final int number) {
        return getSegmentIndex()
            .thenCompose(segmentIndex -> getSegmentTable()
                .thenApply(segmentTable -> TableHelper.getSegment(number, segmentIndex.getData(), segmentTable.getData())));
    }

    @Override
    public CompletableFuture<Data<Integer>> getSegmentTable() {
        return cache.getCachedData(segmentPath);
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentTableFromStore() {
        cache.invalidateCache(segmentPath);
        return getSegmentTable();
    }

    @Override
    CompletableFuture<Void> updateSegmentTable(final Data<Integer> data) {
        return store.setData(segmentPath, data)
                .whenComplete((r, e) -> cache.invalidateCache(segmentPath));
    }

    @Override
    public CompletableFuture<Data<Integer>> getHistoryTable() {
        return cache.getCachedData(historyPath);
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTableFromStore() {
        cache.invalidateCache(historyPath);
        return getHistoryTable();
    }

    @Override
    CompletableFuture<Void> createEpochNodeIfAbsent(int epoch) {
        return store.createZNodeIfNotExist(getEpochPath(epoch));
    }

    @Override
    CompletableFuture<Void> deleteEpochNode(int epoch) {
        String epochPath = getEpochPath(epoch);
        return store.deletePath(epochPath, false).thenAccept(x -> cache.invalidateCache(epochPath));
    }

    @Override
    public CompletableFuture<Data<Integer>> getHistoryIndex() {
        return cache.getCachedData(historyIndexPath);
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryIndexFromStore() {
        cache.invalidateCache(historyIndexPath);
        return getHistoryIndex();
    }

    @Override
    CompletableFuture<Void> updateHistoryIndex(final Data<Integer> updated) {
        return store.setData(historyIndexPath, updated)
                .whenComplete((r, e) -> cache.invalidateCache(historyIndexPath));
    }

    // endregion

    // region private helpers
    private String getActiveTxPath(final long epoch, final String txId) {
        return ZKPaths.makePath(ZKPaths.makePath(activeTxRoot, Long.toString(epoch)), txId);
    }

    private String getEpochPath(final long epoch) {
        return ZKPaths.makePath(activeTxRoot, Long.toString(epoch));
    }

    private String getCompletedTxPath(final String txId) {
        return ZKPaths.makePath(completedTxPath, txId);
    }
}
