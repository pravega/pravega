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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final String SYSTEM = "_system";
    private static final String STREAM_TABLE_PREFIX = "%s.#.%s"; // scoped stream name
    private static final String METADATA_TABLE = STREAM_TABLE_PREFIX + "metadata"; 
    private static final String EPOCHS_WITH_TRANSACTIONS_TABLE = STREAM_TABLE_PREFIX + "epochsWithTransactions"; 
    private static final String ACTIVE_TRANSACTIONS_IN_EPOCH_TABLE_FORMAT = STREAM_TABLE_PREFIX + "transactionsInEpoch-%d";
    private static final String COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT = "completedTransactionsBatch-%d";
    private static final String COMPLETED_TRANSACTIONS_KEY_FORMAT = "%s/%s/%s";
    
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
    private final String metadataTableName;
    private final String epochsWithTransactionsTableName;
    private final String activeTransactionsFormat;

    private final Cache cache;
    private final Supplier<Integer> currentBatchSupplier;

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper) {
        this(scopeName, streamName, storeHelper, () -> 0);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, int chunkSize, int shardSize) {
        this(scopeName, streamName, storeHelper, () -> 0, chunkSize, shardSize);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, Supplier<Integer> currentBatchSupplier) {
        this(scopeName, streamName, storeHelper, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE, SealedSegmentsMapShard.SHARD_SIZE);
    }
    
    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, Supplier<Integer> currentBatchSupplier,
                        int chunkSize, int shardSize) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;

        cache = new Cache(key -> this.storeHelper.getEntry(scopeName, streamName, key));
        this.currentBatchSupplier = currentBatchSupplier;
        this.metadataTableName = String.format(METADATA_TABLE, scopeName, streamName);
        this.epochsWithTransactionsTableName = String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, scopeName, streamName);
        this.activeTransactionsFormat = String.format(ACTIVE_TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, scopeName, streamName);
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        List<CompletableFuture<Integer>> futures = new LinkedList<>();
        // TODO: shivesh
        Executor executor = null;
        return storeHelper.getAllKeys(getScope(), epochsWithTransactionsTableName)
                   .forEachRemaining(x -> {
                       futures.add(getNumberOfOngoingTransactions(Integer.parseInt(x)));
                   }, executor)
        .thenCompose(v -> Futures.allOfWithResults(futures)
                            .thenApply(list -> list.stream().reduce(0, Integer::sum)));
    }

    private CompletableFuture<Integer> getNumberOfOngoingTransactions(int epoch) {
        String scope = getScope();
        String epochTableName = String.format(ACTIVE_TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, scope, getName(), epoch);
        AtomicInteger count = new AtomicInteger(0);
        // TODO: shivesh
        Executor executor = null;
        return storeHelper.getAllKeys(scope, epochTableName).forEachRemaining(x -> count.incrementAndGet(), executor)
                          .thenApply(x -> count.get());
    }

    @Override
    public CompletableFuture<Void> deleteStream() {
        // delete all tables for this stream even if they are not empty!
        // 1. read epoch table 
        // delete all epoch txn specific tables
        // delete epoch txn base table
        // delete metadata table
        
        // delete stream in scope 
        String scope = getScope();
        List<CompletableFuture<Boolean>> futures = new LinkedList<>();
        // TODO: shivesh
        Executor executor = null;
        return storeHelper.getAllKeys(getScope(), epochsWithTransactionsTableName)
                          .forEachRemaining(x -> {
                              String epochTableName = String.format(ACTIVE_TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, scope, getName(), Integer.parseInt(x));
                              futures.add(storeHelper.deleteTable(scope, epochTableName, false));
                          }, executor)
                          .thenCompose(x -> Futures.allOfWithResults(futures))
                          .thenCompose(x -> storeHelper.deleteTable(scope, epochsWithTransactionsTableName, false))
                          .thenCompose(deleted -> Futures.toVoid(storeHelper.deleteTable(scope, metadataTableName, false)));
    }

//    @Override
//    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime, final int startingSegmentNumber) {
//        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
//        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier.
//        
//        // 1. check metadata table exists
//        // 2. if table exists and has configuration which matches incoming configuration
//        // check state --> if state is unknown or creating --> return exists creating.. else return exists_active
//        return storeHelper.getEntry(getScope(), metadataTableName, CONFIGURATION_KEY)
//                          .exceptionally(e -> )
//                          .handle((r, e) -> {
//                                if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
//                                    return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
//                                            configuration, creationTime, startingSegmentNumber));
//                                }
//                                // if config exists, creation time should exist too. 
//                                return getCreationTime().thenCompose(storedCreationTime -> {
//                                    return getState(false).thenApply(state -> {
//                                    CreateStreamResponse.CreateStatus status = storedCreationTime == creationTime ?
//                                            CreateStreamResponse.CreateStatus.NEW : CreateStreamResponse.CreateStatus.EXISTS_CREATING;
//
//                                        if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
//                                            return new CreateStreamResponse(status, configuration, creationTime, startingSegmentNumber);
//                                        } else {
//                                            return new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE,
//                                                    configuration, creationTime, startingSegmentNumber);
//                                        }
//                                    });
//
//                                });
//                            });
//    }
    
    @Override
    public CompletableFuture<Long> getCreationTime() {
        return cache.getCachedData(CREATION_TIME_KEY)
                    .thenApply(data -> BitConverter.readLong(data.getData(), 0));
    }

//    /**
//     * Method to check whether a scope exists before creating a stream under that scope.
//     *
//     * @return A future either returning a result or an exception.
//     */
//    @Override
//    public CompletableFuture<Void> checkScopeExists() {
//        // try creating a key called stream name under `scope` table
//        return storeHelper.addNewEntry(scopePath)
//                          .thenAccept(x -> {
//                        if (!x) {
//                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopePath);
//                        }
//                    });
//    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] data) {
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, RETENTION_SET_KEY, data));
    }

    @Override
    CompletableFuture<Data> getRetentionSetData() {
        return storeHelper.getEntry(getScope(), metadataTableName, RETENTION_SET_KEY);
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(Data retention) {
        return storeHelper.updateEntry(getScope(), metadataTableName, RETENTION_SET_KEY, retention);
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, byte[] record) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, key, record));
    }

    @Override
    CompletableFuture<Data> getStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return cache.getCachedData(key);
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);

        return storeHelper.removeEntry(getScope(), metadataTableName, key)
                          .thenAccept(x -> cache.invalidateCache(key));
    }
    
    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, key, data));
    }

    @Override
    CompletableFuture<Data> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        if (ignoreCached) {
            cache.invalidateCache(key);
        }
        return cache.getCachedData(key);
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, Data data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return storeHelper.updateEntry(getScope(), metadataTableName, key, data);
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data) {
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, CURRENT_EPOCH_KEY, data));
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(Data data) {
        return storeHelper.updateEntry(getScope(), metadataTableName, CURRENT_EPOCH_KEY, data);
    }

    @Override
    CompletableFuture<Data> getCurrentEpochRecordData(boolean ignoreCached) {
        if (ignoreCached) {
            cache.invalidateCache(CURRENT_EPOCH_KEY);
        }
        return cache.getCachedData(CURRENT_EPOCH_KEY);
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, byte[] data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, key, data));
    }

    @Override
    CompletableFuture<Data> getEpochRecordData(int epoch) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return cache.getCachedData(key);
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, byte[] data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, key, data));
    }

    @Override
    CompletableFuture<Data> getSealedSegmentSizesMapShardData(int shard) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return storeHelper.getEntry(getScope(), metadataTableName, key);
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, Data data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return storeHelper.updateEntry(getScope(), metadataTableName, key, data);
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segmentToSeal, int epoch) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentToSeal);
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, epoch);
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, key, epochData));
    }

    @Override
    CompletableFuture<Data> getSegmentSealedRecordData(long segmentId) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentId);
        return cache.getCachedData(key);
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(byte[] epochTransition) {
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, EPOCH_TRANSITION_KEY, epochTransition));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(Data epochTransition) {
        return storeHelper.updateEntry(getScope(), metadataTableName, EPOCH_TRANSITION_KEY, epochTransition);
    }

    @Override
    CompletableFuture<Data> getEpochTransitionNode() {
        return storeHelper.getEntry(getScope(), metadataTableName, EPOCH_TRANSITION_KEY);
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, CREATION_TIME_KEY, b));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final byte[] configuration) {
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, CONFIGURATION_KEY, configuration));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final byte[] state) {
        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, STATE_KEY, state));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return Futures.toVoid(storeHelper.addNewEntry(getScope(), metadataTableName, key, b));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, Data data) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return storeHelper.updateEntry(getScope(), metadataTableName, key, data);
    }

    @Override
    CompletableFuture<Data> getMarkerData(long segmentId) {
        final CompletableFuture<Data> result = new CompletableFuture<>();
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        storeHelper.getEntry(getScope(), metadataTableName, key)
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
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return storeHelper.removeEntry(getScope(), metadataTableName, key);
    }

    @Override
    public CompletableFuture<Map<String, Data>> getCurrentTxns() {
        List<CompletableFuture<Map<String, Data>>> futures = new LinkedList<>();
        // TODO: shivesh
        Executor executor = null;
        
        return storeHelper.getAllKeys(getScope(), epochsWithTransactionsTableName)
                          .forEachRemaining(x -> {
                              futures.add(getTxnInEpoch(Integer.parseInt(x)));
                          }, executor)
                          .thenCompose(v -> {
                              return Futures.allOfWithResults(futures)
                                            .thenApply(list -> {
                                  Map<String, Data> map = new HashMap<>();
                                  list.forEach(map::putAll);
                                  return map;
                              });
                          });
    }

    @Override
    public CompletableFuture<Map<String, Data>> getTxnInEpoch(int epoch) {
        String scope = getScope();
        String epochTableName = String.format(ACTIVE_TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, scope, getName(), epoch);
        AtomicInteger count = new AtomicInteger(0);
        // TODO: shivesh
        Executor executor = null;
        Map<String, Data> result = new ConcurrentHashMap<>();
        return storeHelper.getAllEntries(scope, epochTableName)
                          .forEachRemaining(x -> {
                              result.put(x.getKey(), x.getValue());
                          }, executor)
                          .thenApply(x -> result);
    }

    @Override
    CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final byte[] txnRecord) {
        String tableName = String.format(activeTransactionsFormat, epoch);
        String scope = getScope();
        // TODO: shivesh
        // Note: this can fail with DataNotFoundException!!!
        return storeHelper.createTable(scope, tableName)
            .thenCompose(x -> storeHelper.addNewEntry(scope, tableName, txId.toString(), txnRecord));
    }

    @Override
    CompletableFuture<Data> getActiveTx(final int epoch, final UUID txId) {
        String tableName = String.format(activeTransactionsFormat, epoch);
        return storeHelper.getEntry(getScope(), tableName, txId.toString());
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final Data data) {
        String tableName = String.format(activeTransactionsFormat, epoch);
        return storeHelper.updateEntry(getScope(), tableName, txId.toString(), data);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        String tableName = String.format(activeTransactionsFormat, epoch);
        return storeHelper.removeEntry(getScope(), tableName, txId.toString())
                .thenCompose(v -> storeHelper.deleteTable(getScope(), tableName, true)
                        .thenCompose(deleted -> {
                            if (deleted) {
                                return storeHelper.removeEntry(getScope(), epochsWithTransactionsTableName, "" + epoch);     
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }));
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final byte[] complete) {
        String tableName = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, currentBatchSupplier.get());
        
        String key = String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, getScope(), getName(), txId.toString());

        return storeHelper.createTable(SYSTEM, tableName)
                .thenCompose(x -> Futures.toVoid(storeHelper.addNewEntry(SYSTEM, tableName, key, complete)));
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
    private String getEpochPath(final int epoch) {
        return ZKPaths.makePath(activeTxRoot, Integer.toString(epoch));
    }
}
