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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.utils.ZKPaths;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata storeHelper.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 * <p>
 * It may cache files read from the storeHelper for its lifetime.
 * This shall reduce storeHelper round trips for answering queries, thus making them efficient.
 */
@Slf4j
class PravegaTablesStream extends PersistentStreamBase {
    // <scoped-stream-name>_streamMetadata
    // 1. Key: creation time  // never deleted
    // 2. Key: configuration record // never deleted
    // 3. key: truncation record // never deleted
    // 4. key: state record // never deleted
    // 5. key: epoch transition record // never deleted
    // 6. key: committing-transaction-record // never deleted
    // 7. key: current epoch record // never deleted
    // 8. key: retention set record // never deleted
    // 9. key: epoch record <epoch number>  // never deleted
    // 10. key: history time series <chunk> // never deleted O(number of epochs / 1000)
    // 11. key: segment sealed size map <shard-number> // never deleted O(number of epochs / 1000)
    // 12. key: <segment number> // never deleted O(number of segments)
    // 13. key: markers_<segment-number> // deletable entries
    // 14. key: stream-cut-<reference> // deletable entries
    // 15. key: waitingRequestProcessor // deletable entry
    // <scoped-stream-name>_transactions_in_epochs
    // 1. key: epoch Value: null
    
    // <scoped-stream-name>_transactions_<epoch>
    // key: txnId value: active txn record
    
    
    // <scoped-stream-name>_completed_transactions_<batch>
    // key: txnId 
    
    /*
        create transaction: 
        0. add transaction key in `transactions-epochs-table`
        if (table does not exist) --> 
        1. add epoch key in transactions-epochs-table --> create epoch specific table
        try step 0 again
        
        complete txn:
        1. write completed txn record to new batch.
        2. remove transaction key from the table
        3. delete empty table --> delete epoch from transactions-epochs-table
        
        
        list transactions in epoch:
        1. list all entries in epoch specific transaction table. (this can also throw table not exists which would be transalated to DataNotFoundException)
        
        
        list all transactions:
        1. get all epochs with transactions from `transactions-epochs-table`
        2. call list transactions in epoch
     */
    
    private static final String STREAM_TABLE_PREFIX = "%s/%s"; // scoped stream name
    private static final String METADATA_TABLE = STREAM_TABLE_PREFIX + "/metadata"; 
    private static final String EPOCHS_WITH_TRANSACTIONS_TABLE = STREAM_TABLE_PREFIX + "/epochsWithTransactions"; 
    private static final String EPOCH_TRANSACTIONS_TABLE_FORMAT = STREAM_TABLE_PREFIX + "/transactionsInEpoch-%d";
    private static final String COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT = STREAM_TABLE_PREFIX + "/completedTransactionsBatch-%d";
    
    // metadata keys
    private static final String CREATION_TIME_KEY = "creationTime";
    private static final String CONFIGURATION_KEY = "configuration";
    private static final String TRUNCATION_KEY = "truncation";
    private static final String STATE_KEY = "state";
    private static final String EPOCH_TRANSITION_KEY = "epochTransition";
    private static final String RETENTION_SET_KEY = "retention";
    private static final String RETENTION_STREAM_CUT_RECORD_KEY_FORMAT = "retentionCuts-%s"; // stream cut reference
    private static final String CURRENT_EPOCH_KEY = "currentEpochRecord";
    private static final String EPOCH_RECORD_KEY_FORMAT = "epochRecord-%d";
    private static final String HISTORY_TIMESERES_CHUNK_FORMAT = "historyTimeSeriesChunk-%d";
    private static final String SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT = "segmentsSealedSizeMapShard-%d";
    private static final String SEGMENT_SEALED_EPOCH_KEY_FORMAT = "segmentSealedEpochPath-%d"; // segment id
    private static final String COMMITTING_TRANSACTIONS_RECORD_KEY = "committingTxns";
    private static final String SEGMENT_MARKER_PATH_FORMAT = "markers-%d";
    private static final String WAITING_REQUEST_PROCESSOR_PATH = "waitingRequestProcessor";
    
    private static final String EPOCHS_WITH_TRANSACTIONS_KEY = "epoch-%d";
    private static final String STREAM_COMPLETED_TX_BATCH_KEY_FORMAT = STREAM_TABLE_PREFIX + "%s"; // stream name + transaction id

    private static final Data EMPTY_DATA = new Data(null, new Version.IntVersion(Integer.MIN_VALUE));

    private final PravegaTablesStoreHelper storeHelper;
    private final String streamTablePrefix;
    private final String metadataTableName;
    private final String epochsWithTransactionsTableName;

    private final Cache cache;
    private final Supplier<Integer> currentBatchSupplier;

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, SegmentHelper segmentHelper) {
        this(scopeName, streamName, segmentHelper, () -> 0);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, SegmentHelper segmentHelper, int chunkSize, int shardSize) {
        this(scopeName, streamName, segmentHelper, () -> 0, chunkSize, shardSize);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, SegmentHelper segmentHelper, Supplier<Integer> currentBatchSupplier) {
        this(scopeName, streamName, segmentHelper, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE, SealedSegmentsMapShard.SHARD_SIZE);
    }
    
    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, SegmentHelper segmentHelper, Supplier<Integer> currentBatchSupplier,
                        int chunkSize, int shardSize) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper);

        cache = new Cache(key -> this.storeHelper.getData(scopeName, streamName, key));
        this.currentBatchSupplier = currentBatchSupplier;
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        // TODO: once we get list api we should be able to implement this. 
//        return storeHelper.readTable(activeTxRoot).thenCompose(list ->
//                Futures.allOfWithResults(list.stream().map(epoch ->
//                        getNumberOfOngoingTransactions(Integer.parseInt(epoch))).collect(Collectors.toList())))
//                    .thenApply(list -> list.stream().reduce(0, Integer::sum));
    }

    private CompletableFuture<Integer> getNumberOfOngoingTransactions(int epoch) {
        return storeHelper.getChildren(getEpochPath(epoch)).thenApply(List::size);
    }

    @Override
    public CompletableFuture<Void> deleteStream() {
        // delete all tables for this stream even if they are not empty!
        // 1. read epoch table 
        // delete all epoch txn specific tables
        // delete epoch txn base table
        // delete metadata table
        
        // delete stream in scope 
        return storeHelper.deleteTree(streamPath);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime, final int startingSegmentNumber) {
        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier.
        
        // 1. check metadata table exists
        // 2. 
        return storeHelper.checkExists(creationPath).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                        configuration, creationTime, startingSegmentNumber));
            }

            return getCreationTime().thenCompose(storedCreationTime ->
                    storeHelper.checkExists(configurationPath).thenCompose(configExists -> {
                        if (configExists) {
                            return handleConfigExists(storedCreationTime, startingSegmentNumber, storedCreationTime == creationTime);
                        } else {
                            return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                    configuration, storedCreationTime, startingSegmentNumber));
                        }
                    }));
        });
    }

    private CompletableFuture<CreateStreamResponse> handleConfigExists(long creationTime, int startingSegmentNumber, boolean creationTimeMatched) {
        CreateStreamResponse.CreateStatus status = creationTimeMatched ?
                CreateStreamResponse.CreateStatus.NEW : CreateStreamResponse.CreateStatus.EXISTS_CREATING;

        return getConfiguration().thenCompose(config -> storeHelper.checkExists(statePath)
                                                                   .thenCompose(stateExists -> {
                                                                 if (!stateExists) {
                                                                     return CompletableFuture.completedFuture(new CreateStreamResponse(status, config, creationTime, startingSegmentNumber));
                                                                 }

                                                                 return getState(false).thenApply(state -> {
                                                                     if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                                                                         return new CreateStreamResponse(status, config, creationTime, startingSegmentNumber);
                                                                     } else {
                                                                         return new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE,
                                                                                 config, creationTime, startingSegmentNumber);
                                                                     }
                                                                 });
                                                             }));
    }

    @Override
    public CompletableFuture<Long> getCreationTime() {
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
        // try creating a key called stream name under `scope` table
        
        return storeHelper.checkExists(scopePath)
                          .thenAccept(x -> {
                        if (!x) {
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopePath);
                        }
                    });
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] data) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(retentionSetPath, data));
    }

    @Override
    CompletableFuture<Data> getRetentionSetData() {
        return storeHelper.getData(retentionSetPath);
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(Data retention) {
        return storeHelper.setData(retentionSetPath, retention)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, byte[] record) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(path, record));
    }

    @Override
    CompletableFuture<Data> getStreamCutRecordData(long recordingTime) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);
        return cache.getCachedData(path);
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);

        return storeHelper.deletePath(path, false)
                          .thenAccept(x -> cache.invalidateCache(path));
    }
    
    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data) {
        String path = String.format(historyTimeSeriesChunkPathFormat, chunkNumber);
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(path, data));
    }

    @Override
    CompletableFuture<Data> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        String path = String.format(historyTimeSeriesChunkPathFormat, chunkNumber);
        if (ignoreCached) {
            cache.invalidateCache(path);
        }
        return cache.getCachedData(path);
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, Data data) {
        String path = String.format(historyTimeSeriesChunkPathFormat, chunkNumber);
        return storeHelper.setData(path, data)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(currentEpochRecordPath, data));
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(Data data) {
        return storeHelper.setData(currentEpochRecordPath, data)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Data> getCurrentEpochRecordData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(currentEpochRecordPath);
        }
        return cache.getCachedData(currentEpochRecordPath);
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, byte[] data) {
        String path = String.format(epochRecordPathFormat, epoch);
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(path, data));
    }

    @Override
    CompletableFuture<Data> getEpochRecordData(int epoch) {
        String path = String.format(epochRecordPathFormat, epoch);
        return cache.getCachedData(path);
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, byte[] data) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(path, data));
    }

    @Override
    CompletableFuture<Data> getSealedSegmentSizesMapShardData(int shard) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return storeHelper.getData(path);
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, Data data) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return storeHelper.setData(path, data)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segmentToSeal, int epoch) {
        String path = String.format(segmentSealedEpochPathFormat, segmentToSeal);
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, epoch);
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(path, epochData));
    }

    @Override
    CompletableFuture<Data> getSegmentSealedRecordData(long segmentId) {
        String path = String.format(segmentSealedEpochPathFormat, segmentId);
        return cache.getCachedData(path);
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(byte[] epochTransition) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(epochTransitionPath, epochTransition));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(Data epochTransition) {
        return storeHelper.setData(epochTransitionPath, epochTransition)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Data> getEpochTransitionNode() {
        return storeHelper.getData(epochTransitionPath);
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return Futures.toVoid(storeHelper.createZNodeIfNotExist(creationPath, b));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final byte[] configuration) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(configurationPath, configuration));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final byte[] state) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(statePath, state));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return storeHelper.createZNodeIfNotExist(path, b)
                          .thenAccept(x -> cache.invalidateCache(markerPath));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, Data data) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));

        return storeHelper.setData(path, data).thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Data> getMarkerData(long segmentId) {
        final CompletableFuture<Data> result = new CompletableFuture<>();
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));
        storeHelper.getData(path)
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
    CompletableFuture<Void> removeMarkerData(long segmentId) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));

        return storeHelper.deletePath(path, false)
                          .whenComplete((r, e) -> cache.invalidateCache(path));
    }

    @Override
    public CompletableFuture<Map<String, Data>> getCurrentTxns() {
        return storeHelper.getChildren(activeTxRoot)
                          .thenCompose(children -> {
                        return Futures.allOfWithResults(children.stream().map(x -> getTxnInEpoch(Integer.parseInt(x))).collect(Collectors.toList()))
                                      .thenApply(list -> {
                                          Map<String, Data> map = new HashMap<>();
                                          list.forEach(map::putAll);
                                          return map;
                                      });
                    });
    }

    @Override
    public CompletableFuture<Map<String, Data>> getTxnInEpoch(int epoch) {
        return Futures.exceptionallyExpecting(storeHelper.getChildren(getEpochPath(epoch)),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, Collections.emptyList())
                      .thenCompose(txIds -> Futures.allOfWithResults(txIds.stream().collect(
                              Collectors.toMap(txId -> txId, txId -> Futures.exceptionallyExpecting(storeHelper.getData(getActiveTxPath(epoch, txId)),
                                      e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, EMPTY_DATA)))
                              ).thenApply(txnMap -> txnMap.entrySet().stream().filter(x -> !x.getValue().equals(EMPTY_DATA))
                                                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                      );
    }

    @Override
    CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final byte[] txnRecord) {
        final String activePath = getActiveTxPath(epoch, txId.toString());
        // we will always create parent if needed so that transactions are created successfully even if the epoch znode
        // previously found to be empty and deleted.
        // For this, send createParent flag = true
        return storeHelper.createZNodeIfNotExist(activePath, txnRecord, true)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Data> getActiveTx(final int epoch, final UUID txId) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return storeHelper.getData(activeTxPath);
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final Data data) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return storeHelper.setData(activeTxPath, data)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        final String activePath = getActiveTxPath(epoch, txId.toString());
        // attempt to delete empty epoch nodes by sending deleteEmptyContainer flag as true.
        return storeHelper.deletePath(activePath, true);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final byte[] complete) {
        String root = String.format(STREAM_COMPLETED_TX_BATCH_PATH, currentBatchSupplier.get(), getScope(), getName());
        String path = ZKPaths.makePath(root, txId.toString());

        return Futures.toVoid(storeHelper.createZNodeIfNotExist(path, complete));
    }


    @Override
    CompletableFuture<Data> getCompletedTx(final UUID txId) {
        return storeHelper.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH)
                          .thenCompose(children -> {
                        return Futures.allOfWithResults(children.stream().map(child -> {
                            String root = String.format(STREAM_COMPLETED_TX_BATCH_PATH, Long.parseLong(child), getScope(), getName());
                            String path = ZKPaths.makePath(root, txId.toString());

                            return cache.getCachedData(path)
                                        .exceptionally(e -> {
                                            if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                                                return null;
                                            } else {
                                                log.error("Exception while trying to fetch completed transaction status", e);
                                                throw new CompletionException(e);
                                            }
                                        });
                        }).collect(Collectors.toList()));
                    })
                          .thenCompose(result -> {
                        Optional<Data> any = result.stream().filter(Objects::nonNull).findFirst();
                        if (any.isPresent()) {
                            return CompletableFuture.completedFuture(any.get());
                        } else {
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "Completed Txn not found");
                        }
                    });
    }

    @Override
    public CompletableFuture<Void> createTruncationDataIfAbsent(final byte[] truncationRecord) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(truncationPath, truncationRecord));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final Data truncationRecord) {
        return storeHelper.setData(truncationPath, truncationRecord)
                          .thenApply(r -> {
                        cache.invalidateCache(truncationPath);
                        return new Version.IntVersion(r);
                    });
    }

    @Override
    CompletableFuture<Data> getTruncationData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(truncationPath);
        }

        return cache.getCachedData(truncationPath);
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final Data configuration) {
        return storeHelper.setData(configurationPath, configuration)
                          .thenApply(r -> {
                        cache.invalidateCache(configurationPath);
                        return new Version.IntVersion(r);
                    });
    }

    @Override
    CompletableFuture<Data> getConfigurationData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(configurationPath);
        }

        return cache.getCachedData(configurationPath);
    }

    @Override
    CompletableFuture<Version> setStateData(final Data state) {
        return storeHelper.setData(statePath, state)
                          .thenApply(r -> {
                        cache.invalidateCache(statePath);
                        return new Version.IntVersion(r);
                    });
    }

    @Override
    CompletableFuture<Data> getStateData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(statePath);
        }

        return cache.getCachedData(statePath);
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(byte[] committingTxns) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(committingTxnsPath, committingTxns));
    }

    @Override
    CompletableFuture<Data> getCommitTxnRecord() {
        return storeHelper.getData(committingTxnsPath);
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(Data update) {
        return storeHelper.setData(committingTxnsPath, update)
                          .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] waitingRequestProcessor) {
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(waitingRequestProcessorPath, waitingRequestProcessor));
    }

    @Override
    CompletableFuture<Data> getWaitingRequestNode() {
        return storeHelper.getData(waitingRequestProcessorPath);
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        return storeHelper.deletePath(waitingRequestProcessorPath, false);
    }

    @Override
    public void refresh() {
        // refresh all mutable records
        cache.invalidateAll();
    }
    // endregion

    // region private helpers
    @VisibleForTesting
    String getActiveTxPath(final int epoch, final String txId) {
        return ZKPaths.makePath(ZKPaths.makePath(activeTxRoot, Integer.toString(epoch)), txId);
    }

    private String getEpochPath(final int epoch) {
        return ZKPaths.makePath(activeTxRoot, Integer.toString(epoch));
    }
}
