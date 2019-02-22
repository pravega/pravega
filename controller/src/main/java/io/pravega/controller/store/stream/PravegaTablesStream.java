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
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.*;

@Slf4j
class PravegaTablesStream extends PersistentStreamBase {
    private static final String STREAM_TABLE_PREFIX = "%s.#-#.%s.#-#."; // scoped stream name
    private static final String METADATA_TABLE = STREAM_TABLE_PREFIX + "metadata"; 
    private static final String EPOCHS_WITH_TRANSACTIONS_TABLE = STREAM_TABLE_PREFIX + "epochsWithTransactions"; 
    private static final String EPOCH_TRANSACTIONS_TABLE_FORMAT = "transactionsInEpoch-%d";
    
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

    // completed transactions key
    private static final String COMPLETED_TRANSACTIONS_KEY_FORMAT = STREAM_TABLE_PREFIX + "/%s";

    private static final String CACHE_KEY_DELIMITER = ".#cacheKey#.";

    private final PravegaTablesStoreHelper storeHelper;
    private final String metadataTableName;
    private final String epochsWithTransactionsTableName;
    private final String streamTablePrefix;

    private final Cache cache;
    private final Supplier<Integer> currentBatchSupplier;
    private final Executor executor;

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, 
                        Supplier<Integer> currentBatchSupplier, Executor executor) {
        this(scopeName, streamName, storeHelper, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE, 
                SealedSegmentsMapShard.SHARD_SIZE, executor);
    }
    
    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, 
                        Supplier<Integer> currentBatchSupplier, int chunkSize, int shardSize, Executor executor) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;

        cache = new Cache(entryKey -> {
            String[] splits = entryKey.split(CACHE_KEY_DELIMITER);
            String tableName = splits[0];
            String key = splits[1];
            return this.storeHelper.getEntry(scopeName, tableName, key);
        });
        this.currentBatchSupplier = currentBatchSupplier;
        this.executor = executor;
        this.metadataTableName = String.format(METADATA_TABLE, scopeName, streamName);
        this.epochsWithTransactionsTableName = String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, scopeName, streamName);
        this.streamTablePrefix = String.format(STREAM_TABLE_PREFIX, scopeName, streamName);
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        List<CompletableFuture<Integer>> futures = new LinkedList<>();
        return storeHelper.getAllKeys(getScope(), epochsWithTransactionsTableName)
                   .forEachRemaining(x -> {
                       futures.add(getNumberOfOngoingTransactions(Integer.parseInt(x)));
                   }, executor)
        .thenCompose(v -> Futures.allOfWithResults(futures)
                            .thenApply(list -> list.stream().reduce(0, Integer::sum)));
    }

    private CompletableFuture<Integer> getNumberOfOngoingTransactions(int epoch) {
        String scope = getScope();
        String epochTableName = String.format(EPOCH_TRANSACTIONS_TABLE_FORMAT, scope, getName(), epoch);
        AtomicInteger count = new AtomicInteger(0);
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
        List<CompletableFuture<Void>> futures = new LinkedList<>();
        return Futures.exceptionallyExpecting(storeHelper.getAllKeys(getScope(), epochsWithTransactionsTableName)
                          .forEachRemaining(x -> {
                              String epochTableName = String.format(EPOCH_TRANSACTIONS_TABLE_FORMAT, scope, getName(), Integer.parseInt(x));
                              futures.add(Futures.exceptionallyExpecting(storeHelper.deleteTable(scope, epochTableName, false), 
                                      DATA_NOT_FOUND_PREDICATE, null));
                          }, executor), DATA_NOT_FOUND_PREDICATE, null)
                          .thenCompose(x -> Futures.allOfWithResults(futures))
                          .thenCompose(x -> Futures.exceptionallyExpecting(
                                  storeHelper.deleteTable(scope, epochsWithTransactionsTableName, false), 
                                  DATA_NOT_FOUND_PREDICATE, null))
                          .thenCompose(deleted -> storeHelper.deleteTable(scope, metadataTableName, false));
    }

    @Override
    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime, 
                                                                     final int startingSegmentNumber) {
        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier,
        return Futures.exceptionallyExpecting(getCreationTime(), DATA_NOT_FOUND_PREDICATE, null)
                      .thenCompose(storedCreationTime -> {
                          if (storedCreationTime == null) {
                              return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                      configuration, creationTime, startingSegmentNumber));
                          } else {
                              return Futures.exceptionallyExpecting(getConfiguration(), DATA_NOT_FOUND_PREDICATE, null)
                                            .thenCompose(config -> {
                                                if (config != null) {
                                                    return handleConfigExists(storedCreationTime, config, startingSegmentNumber, 
                                                            storedCreationTime == creationTime);
                                                } else {
                                                    return CompletableFuture.completedFuture(
                                                            new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                                            configuration, storedCreationTime, startingSegmentNumber));
                                                }
                                            });
                              
                          }
                      });
                              
    }

    @Override
    CompletableFuture<Void> createStreamMetadata() {
        return CompletableFuture.allOf(storeHelper.createTable(getScope(), metadataTableName), 
                storeHelper.createTable(getScope(), epochsWithTransactionsTableName));
    }

    private CompletableFuture<CreateStreamResponse> handleConfigExists(long creationTime, StreamConfiguration config, 
                                                                       int startingSegmentNumber, boolean creationTimeMatched) {
        CreateStreamResponse.CreateStatus status = creationTimeMatched ?
                CreateStreamResponse.CreateStatus.NEW : CreateStreamResponse.CreateStatus.EXISTS_CREATING;
        return Futures.exceptionallyExpecting(getState(true), DATA_NOT_FOUND_PREDICATE, null)
                      .thenApply(state -> {
                          if (state == null) {
                              return new CreateStreamResponse(status, config, creationTime, startingSegmentNumber);
                          } else if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                              return new CreateStreamResponse(status, config, creationTime, startingSegmentNumber);
                          } else {
                              return new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE,
                                      config, creationTime, startingSegmentNumber);
                          }
                      });
    }
    
    @Override
    public CompletableFuture<Long> getCreationTime() {
        return cache.getCachedData(getCacheEntryKey(metadataTableName, CREATION_TIME_KEY))
                    .thenApply(data -> BitConverter.readLong(data.getData(), 0));
    }

    private String getCacheEntryKey(String metadataTableName, String creationTimeKey) {
        return metadataTableName + CACHE_KEY_DELIMITER + creationTimeKey;
    }
    
    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] data) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, RETENTION_SET_KEY, data));
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
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, key, record));
    }

    @Override
    CompletableFuture<Data> getStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return cache.getCachedData(getCacheEntryKey(metadataTableName, key));
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);

        return storeHelper.removeEntry(getScope(), metadataTableName, key)
                          .thenAccept(x -> cache.invalidateCache(getCacheEntryKey(metadataTableName, key)));
    }
    
    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, key, data));
    }

    @Override
    CompletableFuture<Data> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        String cacheKey = getCacheEntryKey(metadataTableName, key);
        if (ignoreCached) {
            cache.invalidateCache(cacheKey);
        }
        return cache.getCachedData(cacheKey);
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, Data data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return storeHelper.updateEntry(getScope(), metadataTableName, key, data);
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, CURRENT_EPOCH_KEY, data));
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(Data data) {
        return storeHelper.updateEntry(getScope(), metadataTableName, CURRENT_EPOCH_KEY, data);
    }

    @Override
    CompletableFuture<Data> getCurrentEpochRecordData(boolean ignoreCached) {
        String cacheKey = getCacheEntryKey(metadataTableName, CURRENT_EPOCH_KEY);
        if (ignoreCached) {
            cache.invalidateCache(cacheKey);
        }
        return cache.getCachedData(cacheKey);
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, byte[] data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, key, data));
    }

    @Override
    CompletableFuture<Data> getEpochRecordData(int epoch) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        String cacheEntryKey = getCacheEntryKey(metadataTableName, key);
        return cache.getCachedData(cacheEntryKey);
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, byte[] data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, key, data));
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
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, key, epochData));
    }

    @Override
    CompletableFuture<Data> getSegmentSealedRecordData(long segmentId) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentId);
        return cache.getCachedData(getCacheEntryKey(metadataTableName, key));
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(byte[] epochTransition) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, EPOCH_TRANSITION_KEY, epochTransition));
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

        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, CREATION_TIME_KEY, b));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final byte[] configuration) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, CONFIGURATION_KEY, configuration));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final byte[] state) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, STATE_KEY, state));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, key, b));
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
        String epochTableName = getEpochTable(epoch);
        Map<String, Data> result = new ConcurrentHashMap<>();
        return Futures.exceptionallyExpecting(storeHelper.getAllEntries(scope, epochTableName)
                          .forEachRemaining(x -> {
                              result.put(x.getKey(), x.getValue());
                          }, executor)
                          .thenApply(x -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyMap());
    }

    @Override
    CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final byte[] txnRecord) {
        String epochTable = getEpochTable(epoch);
        String scope = getScope();
        // epochs_with_txn ==> epochs are removed from this only when an epoch is empty and does not match current epoch.reference epoch
        // txns-in-epoch-<epoch>-table ==>
        //
        // create txn ==>
        //  1. add epochs_with_txn entry for the epoch
        //  2. create txns-in-epoch-<epoch> table
        //  3. create txn in epoch
        return Futures.exceptionallyComposeExpecting(storeHelper.addNewEntry(scope, epochTable, txId.toString(), txnRecord),
            DATA_NOT_FOUND_PREDICATE, () -> createEpochTxnTableAndEntries(scope, epochTable, epoch, txId.toString(), txnRecord));
    }

    private CompletableFuture<Version> createEpochTxnTableAndEntries(String scope, String epochTable, int epoch, 
                                                                     String txnId, byte[] txnRecord) {
        return storeHelper.addNewEntry(scope, epochsWithTransactionsTableName, "" + epoch, new byte[0])
            .thenCompose(added -> storeHelper.createTable(scope, epochTable))
            .thenCompose(tableCreated -> storeHelper.addNewEntry(scope, epochTable, txnId, txnRecord));    
    }

    private String getEpochTable(int epoch) {
        return streamTablePrefix + String.format(EPOCH_TRANSACTIONS_TABLE_FORMAT, epoch);
    }

    @Override
    CompletableFuture<Data> getActiveTx(final int epoch, final UUID txId) {
        String epochTable = getEpochTable(epoch);
        return storeHelper.getEntry(getScope(), epochTable, txId.toString());
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final Data data) {
        String epochTable = getEpochTable(epoch);
        return storeHelper.updateEntry(getScope(), epochTable, txId.toString(), data);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        // 1. remove txn from txn-in-epoch table
        // 2. get current epoch --> if txn-epoch < activeEpoch.reference epoch, try deleting empty epoch table.
        String epochTable = getEpochTable(epoch);
        return storeHelper.removeEntry(getScope(), epochTable, txId.toString())
                .thenCompose(v -> tryRemoveEpoch(epoch, epochTable));
    }

    private CompletableFuture<Void> tryRemoveEpoch(int epoch, String epochTable) {
        return getActiveEpoch(true)
                .thenCompose(activeEpoch -> {
                    if (epoch < activeEpoch.getReferenceEpoch()) {
                        return Futures.exceptionallyExpecting(storeHelper.deleteTable(getScope(), epochTable, true).thenApply(v -> true),
                                   DATA_NOT_EMPTY_PREDICATE, false)
                                   .thenCompose(deleted -> {
                                       if (deleted) {
                                           return storeHelper.removeEntry(getScope(), epochsWithTransactionsTableName, "" + epoch);
                                       } else {
                                           return CompletableFuture.completedFuture(null);
                                       }
                                   });
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final byte[] complete) {
        Integer batch = currentBatchSupplier.get();
        String tableName = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);
        
        String key = String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, getScope(), getName(), txId.toString());

        return Futures.exceptionallyComposeExpecting(Futures.toVoid(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, tableName, key, complete)),
                DATA_NOT_FOUND_PREDICATE, () -> tryCreateBatchTable(batch, key, complete));
    }

    private CompletableFuture<Void> tryCreateBatchTable(int batch, String key, byte[] complete) {
        String batchTable = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);
    
        return storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                .thenCompose(v -> storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE, "" + batch, new byte[0]))
                .thenCompose(v -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, batchTable))
                .thenCompose(v -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, batchTable, key, complete)));
    }

    @Override
    CompletableFuture<Data> getCompletedTx(final UUID txId) {
        List<Long> batches = new LinkedList<>();
        return storeHelper.getAllKeys(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE).forEachRemaining(x -> batches.add(Long.parseLong(x)), executor)
                          .thenCompose(v -> {
                              return Futures.allOfWithResults(batches.stream().map(batch -> {
                                  String table = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);
                                  String key = String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, getScope(), getName(), txId.toString());

                                  return Futures.exceptionallyExpecting(storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, table, key), DATA_NOT_FOUND_PREDICATE, null);
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
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, TRUNCATION_KEY, truncationRecord));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final Data truncationRecord) {
        return storeHelper.updateEntry(getScope(), metadataTableName, TRUNCATION_KEY, truncationRecord)
                          .thenApply(r -> {
                              cache.invalidateCache(getCacheEntryKey(metadataTableName, TRUNCATION_KEY));
                              return r;
                          });
    }

    @Override
    CompletableFuture<Data> getTruncationData(boolean ignoreCached) {
        String cacheKey = getCacheEntryKey(metadataTableName, TRUNCATION_KEY);
        if (ignoreCached) {
            cache.invalidateCache(cacheKey);
        }

        return cache.getCachedData(cacheKey);
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final Data configuration) {
        return storeHelper.updateEntry(getScope(), metadataTableName, CONFIGURATION_KEY, configuration)
                          .thenApply(r -> {
                              cache.invalidateCache(getCacheEntryKey(metadataTableName, CONFIGURATION_KEY));
                              return r;
                          });
    }

    @Override
    CompletableFuture<Data> getConfigurationData(boolean ignoreCached) {
        String cacheKey = getCacheEntryKey(metadataTableName, CONFIGURATION_KEY);
        if (ignoreCached) {
            cache.invalidateCache(cacheKey);
        }

        return cache.getCachedData(cacheKey);
    }

    @Override
    CompletableFuture<Version> setStateData(final Data state) {
        return storeHelper.updateEntry(getScope(), metadataTableName, STATE_KEY, state)
                          .thenApply(r -> {
                              cache.invalidateCache(getCacheEntryKey(metadataTableName, STATE_KEY));
                              return r;
                          });
    }

    @Override
    CompletableFuture<Data> getStateData(boolean ignoreCached) {
        String cacheKey = getCacheEntryKey(metadataTableName, STATE_KEY);
        if (ignoreCached) {
            cache.invalidateCache(cacheKey);
        }

        return cache.getCachedData(cacheKey);
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(byte[] committingTxns) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, COMMITTING_TRANSACTIONS_RECORD_KEY, committingTxns));
    }

    @Override
    CompletableFuture<Data> getCommitTxnRecord() {
        return storeHelper.getEntry(getScope(), metadataTableName, COMMITTING_TRANSACTIONS_RECORD_KEY);
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(Data update) {
        return storeHelper.updateEntry(getScope(), metadataTableName, COMMITTING_TRANSACTIONS_RECORD_KEY, update);
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] waitingRequestProcessor) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(), metadataTableName, WAITING_REQUEST_PROCESSOR_PATH, waitingRequestProcessor));
    }

    @Override
    CompletableFuture<Data> getWaitingRequestNode() {
        return storeHelper.getEntry(getScope(), metadataTableName, WAITING_REQUEST_PROCESSOR_PATH);
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        return storeHelper.removeEntry(getScope(), metadataTableName, WAITING_REQUEST_PROCESSOR_PATH);
    }

    @Override
    public void refresh() {
        // refresh all mutable records
        cache.invalidateAll();
    }
    // endregion
}
