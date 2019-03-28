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
import com.google.common.base.Strings;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_EMPTY_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SEPARATOR;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.COMPLETED_TRANSACTIONS_BATCHES_TABLE;

@Slf4j
/**
 * Pravega Table Stream. 
 * This creates two top level tables per stream - metadataTable, epochsWithTransactionsTable. 
 * All metadata records are stored in metadata table. 
 * EpochsWithTransactions is a top level table for storing epochs where any transaction was created. 
 *
 * This class is coded for transaction ids that follow the scheme that msb 32 bits represent epoch
 */
class PravegaTablesStream extends PersistentStreamBase {
    private static final String STREAM_TABLE_PREFIX = "Table" + SEPARATOR + "%s" + SEPARATOR; // stream name
    private static final String METADATA_TABLE = STREAM_TABLE_PREFIX + "metadata" + SEPARATOR + "%s";

    private static final String EPOCHS_WITH_TRANSACTIONS_TABLE = STREAM_TABLE_PREFIX + "epochsWithTransactions" + SEPARATOR + "%s";
    private static final String TRANSACTIONS_IN_EPOCH_TABLE_FORMAT = STREAM_TABLE_PREFIX + "%s" + SEPARATOR + "transactionsInEpoch-%d";

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
    private static final String STREAM_KEY_PREFIX = "Key" + SEPARATOR + "%s" + SEPARATOR + "%s" + SEPARATOR; // scoped stream name
    private static final String COMPLETED_TRANSACTIONS_KEY_FORMAT = STREAM_KEY_PREFIX + "/%s";

    private final PravegaTablesStoreHelper storeHelper;

    private final Supplier<Integer> currentBatchSupplier;
    private final Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier;
    private final AtomicReference<String> idRef;
    private final ScheduledExecutorService executor;
    
    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper,
                        Supplier<Integer> currentBatchSupplier, Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier,
                        ScheduledExecutorService executor) {
        this(scopeName, streamName, storeHelper, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE,
                SealedSegmentsMapShard.SHARD_SIZE, streamsInScopeTableNameSupplier, executor);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper,
                        Supplier<Integer> currentBatchSupplier, int chunkSize, int shardSize,
                        Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier, ScheduledExecutorService executor) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;

        this.currentBatchSupplier = currentBatchSupplier;
        this.streamsInScopeTableNameSupplier = streamsInScopeTableNameSupplier;
        this.idRef = new AtomicReference<>(null);
        this.executor = executor;
    }

    private CompletableFuture<String> getId() {
        String id = idRef.get();

        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            return streamsInScopeTableNameSupplier.get()
                                                  .thenCompose(streamsInScopeTable -> storeHelper.getEntry(getScope(), streamsInScopeTable, getName()))
                                                  .thenComposeAsync(data -> {
                                                      idRef.compareAndSet(null, BitConverter.readUUID(data.getData(), 0).toString());
                                                      return getId();
                                                  });
        }
    }

    private CompletableFuture<String> getMetadataTable() {
        return getId().thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return String.format(METADATA_TABLE, getName(), id);
    }
    
    private CompletableFuture<String> getEpochsWithTransactionsTable() {
        return getId().thenApply(this::getEpochsWithTransactionsTableName);
    }

    private String getEpochsWithTransactionsTableName(String id) {
        return String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, getName(), id);
    }

    private CompletableFuture<String> getTransactionsInEpochTable(int epoch) {
        return getId().thenApply(id -> getTransactionsInEpochTableName(epoch, id));
    }

    private String getTransactionsInEpochTableName(int epoch, String id) {
        return String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, getName(), id, epoch);
    }

    // region overrides
    @Override
    public CompletableFuture<Void> completeCommittingTransactions(VersionedMetadata<CommittingTransactionsRecord> record) {
        // create all transaction entries in committing txn list. 
        // remove all entries from active txn in epoch. 
        // reset CommittingTxnRecord
        long time = System.currentTimeMillis();

        Map<String, byte[]> completedRecords = record.getObject().getTransactionsToCommit().stream()
                                                               .collect(Collectors.toMap(UUID::toString, 
                                                                       x -> new CompletedTxnRecord(time, TxnStatus.COMMITTED).toBytes()));
        return createCompletedTxEntries(completedRecords)
                .thenCompose(x -> getTransactionsInEpochTable(record.getObject().getEpoch())
                        .thenCompose(table -> storeHelper.removeEntries(getScope(), table, completedRecords.keySet())))
                .thenCompose(x -> tryRemoveOlderTransactionsInEpochTables(epoch -> epoch < record.getObject().getEpoch()))
                .thenCompose(x -> Futures.toVoid(updateCommittingTxnRecord(new Data(CommittingTransactionsRecord.EMPTY.toBytes(),
                        record.getVersion()))));
    }

    @Override
    CompletableFuture<Void> createStreamMetadata() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            String epochWithTxnTable = getEpochsWithTransactionsTableName(id);
            return CompletableFuture.allOf(storeHelper.createTable(getScope(), metadataTable),
                    storeHelper.createTable(getScope(), epochWithTxnTable))
                                    .thenAccept(v -> log.debug("stream {}/{} metadata tables {} & {} created", getScope(), getName(), metadataTable,
                                            epochWithTxnTable));
        });

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
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(getScope(), metadataTable, CREATION_TIME_KEY))
                .thenApply(data -> BitConverter.readLong(data.getData(), 0));
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
        return getId()
                .thenCompose(id -> tryRemoveOlderTransactionsInEpochTables(epoch -> true)
                        .thenCompose(v -> getEpochsWithTransactionsTable()
                                .thenCompose(epochWithTxnTable -> Futures.exceptionallyExpecting(
                                        storeHelper.deleteTable(scope, epochWithTxnTable, false),
                                        DATA_NOT_FOUND_PREDICATE, null))
                        .thenCompose(deleted -> storeHelper.deleteTable(scope, getMetadataTableName(id), false))));
    }
    
    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, RETENTION_SET_KEY, data)
                                      .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, RETENTION_SET_KEY));
                });
    }

    @Override
    CompletableFuture<Data> getRetentionSetData() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, RETENTION_SET_KEY));
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(Data retention) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.updateEntry(getScope(), metadataTable, RETENTION_SET_KEY, retention)
                                      .thenApply(v -> {
                                          storeHelper.invalidateCache(getScope(), metadataTable, RETENTION_SET_KEY);
                                          return v;
                                      });
                });
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, byte[] record) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, record)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<Data> getStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(getScope(), metadataTable, key));
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.removeEntry(getScope(), metadataTable, key)
                                                         .thenAccept(x -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, data)
                                                         .thenAccept(x -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<Data> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, key);
                    }
                    return storeHelper.getCachedData(getScope(), metadataTable, key);
                });
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, Data data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.updateEntry(getScope(), metadataTable, key, data)
                                      .thenApply(version -> {
                                          storeHelper.invalidateCache(getScope(), metadataTable, key);
                                          return version;
                                      });
                });
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, CURRENT_EPOCH_KEY, data)
                                      .thenAccept(v -> {
                                          storeHelper.invalidateCache(getScope(), metadataTable, CURRENT_EPOCH_KEY);
                                      });
                });
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(Data data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, CURRENT_EPOCH_KEY, data)
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, CURRENT_EPOCH_KEY);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<Data> getCurrentEpochRecordData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, CURRENT_EPOCH_KEY);
                    }
                    return storeHelper.getCachedData(getScope(), metadataTable, CURRENT_EPOCH_KEY);
                });
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, byte[] data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, data)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)))
                .thenCompose(v -> createEpochTxnTable(epoch));
    }

    private CompletableFuture<Void> createEpochTxnTable(int epoch) {
        return getEpochsWithTransactionsTable()
                .thenCompose(epochWithTxnTable ->
                        storeHelper.addNewEntryIfAbsent(getScope(), epochWithTxnTable, "" + epoch, new byte[0])
                                   .thenCompose(added -> getTransactionsInEpochTable(epoch)
                                           .thenCompose(epochTable -> storeHelper.createTable(getScope(), epochTable))));
    }

    @Override
    CompletableFuture<Data> getEpochRecordData(int epoch) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
                    return storeHelper.getCachedData(getScope(), metadataTable, key);
                });
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, byte[] data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, data)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<Data> getSealedSegmentSizesMapShardData(int shard) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, key));
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, Data data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, key, data)
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, key);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segmentToSeal, int epoch) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentToSeal);
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(), metadataTable, key, epochData)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
    }

    @Override
    CompletableFuture<Data> getSegmentSealedRecordData(long segmentId) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(getScope(), metadataTable, key));
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(byte[] epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, EPOCH_TRANSITION_KEY, epochTransition)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, EPOCH_TRANSITION_KEY)));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(Data epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, EPOCH_TRANSITION_KEY, epochTransition)
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, EPOCH_TRANSITION_KEY);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<Data> getEpochTransitionNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, EPOCH_TRANSITION_KEY));
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, CREATION_TIME_KEY, b)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, CREATION_TIME_KEY)));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final byte[] configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, CONFIGURATION_KEY, configuration)
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, CONFIGURATION_KEY)));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final byte[] state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, STATE_KEY, state)));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, key, b)));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, Data data) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, key, data));
    }

    @Override
    CompletableFuture<Data> getMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable().thenCompose(metadataTable ->
                Futures.exceptionallyExpecting(storeHelper.getEntry(getScope(), metadataTable, key),
                        DATA_NOT_FOUND_PREDICATE, null));
    }

    @Override
    CompletableFuture<Void> removeMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(id -> storeHelper.removeEntry(getScope(), id, key));
    }

    @Override
    public CompletableFuture<Map<String, Data>> getCurrentTxns() {
        return getEpochsWithTransactions()
                .thenCompose(epochsWithTransactions -> {
                    return Futures.allOfWithResults(epochsWithTransactions.stream().map(this::getTxnInEpoch).collect(Collectors.toList()));
                }).thenApply(list -> {
            Map<String, Data> map = new HashMap<>();
            list.forEach(map::putAll);
            return map;
        });
    }

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        return getEpochsWithTransactionsTable()
                .thenCompose(table -> storeHelper.getAllKeys(getScope(), table)
                                                 .collectRemaining(x -> {
                                                     futures.add(getNumberOfOngoingTransactions(Integer.parseInt(x)));
                                                     return true;
                                                 })
                                                 .thenCompose(v -> Futures.allOfWithResults(futures)
                                                                          .thenApply(list -> list.stream().reduce(0, Integer::sum))));
    }

    private CompletableFuture<Integer> getNumberOfOngoingTransactions(int epoch) {
        AtomicInteger count = new AtomicInteger(0);
        return getTransactionsInEpochTable(epoch)
                .thenCompose(tableName -> storeHelper.getAllKeys(getScope(), tableName).collectRemaining(x -> {
                    count.incrementAndGet();
                    return true;
                }).thenApply(x -> count.get()));
    }

    private CompletableFuture<List<Integer>> getEpochsWithTransactions() {
        return getEpochsWithTransactionsTable()
                .thenCompose(epochWithTxnTable -> {
                    List<Integer> epochsWithTransactions = new ArrayList<>();
                    return storeHelper.getAllKeys(getScope(), epochWithTxnTable)
                               .collectRemaining(x -> {
                                   epochsWithTransactions.add(Integer.parseInt(x));
                                   return true;
                               }).thenApply(v -> epochsWithTransactions);
                });
    }
    
    @Override
    public CompletableFuture<Map<String, Data>> getTxnInEpoch(int epoch) {
        String scope = getScope();
        Map<String, Data> result = new ConcurrentHashMap<>();
        return getTransactionsInEpochTable(epoch)
            .thenCompose(tableName -> Futures.exceptionallyExpecting(storeHelper.getAllEntries(scope, tableName)
                                                         .forEachRemaining(x -> {
                                                             result.put(x.getKey(), x.getValue());
                                                         }, executor)
                                                         .thenApply(x -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyMap()));
    }

    @Override
    CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final byte[] txnRecord) {
        String scope = getScope();
        // create txn ==> 
        // if epoch table exists, add txn to epoch 
        //  1. add epochs_with_txn entry for the epoch
        //  2. create txns-in-epoch table
        //  3. create txn in txns-in-epoch
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTable -> storeHelper.addNewEntry(scope, epochTable, txId.toString(), txnRecord));
    }
    
    @Override
    CompletableFuture<Data> getActiveTx(final int epoch, final UUID txId) {
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTxnTable -> storeHelper.getEntry(getScope(), epochTxnTable, txId.toString()));
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final Data data) {
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTxnTable -> storeHelper.updateEntry(getScope(), epochTxnTable, txId.toString(), data));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        // 1. remove txn from txn-in-epoch table
        // 2. get current epoch --> if txn-epoch < activeEpoch.reference epoch, try deleting empty epoch table.
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTransactionsTableName ->
                        storeHelper.removeEntry(getScope(), epochTransactionsTableName, txId.toString()))
                // this is only best case attempt. If the epoch table is not empty, it will be ignored. 
                // if we fail to do this after having removed the transaction, in retried attempt
                // the caller may not find the transaction and never attempt to remove this table.  
                // this can lead to proliferation of tables. 
                // But remove transaction entry is called from . 
                .thenCompose(v -> tryRemoveOlderTransactionsInEpochTables(e -> e < epoch));
    }

    private CompletableFuture<Void> tryRemoveOlderTransactionsInEpochTables(Predicate<Integer> epochPredicate) {
        return getEpochsWithTransactions()
                .thenCompose(list -> {
                    return Futures.allOf(list.stream().filter(epochPredicate)
                                             .map(this::tryRemoveTransactionsInEpochTable)
                                             .collect(Collectors.toList()));
                });
    }

    private CompletableFuture<Void> tryRemoveTransactionsInEpochTable(int epoch) {
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTable -> 
                        storeHelper.deleteTable(getScope(), epochTable, true)
                                                                  .handle((r, e) -> {
                                                                      if (e != null) {
                                                                          if (DATA_NOT_FOUND_PREDICATE.test(e)) {
                                                                              return true;
                                                                          } else if (DATA_NOT_EMPTY_PREDICATE.test(e)) {
                                                                              return false;
                                                                          } else {
                                                                              throw new CompletionException(e);
                                                                          }
                                                                      } else {
                                                                          return true;
                                                                      }
                                                                  })
                      .thenCompose(deleted -> {
                          if (deleted) {
                              return getEpochsWithTransactionsTable()
                                .thenCompose(table -> storeHelper.removeEntry(getScope(), table, "" + epoch));
                          } else {
                              return CompletableFuture.completedFuture(null);
                          }
                      }));
    }
    
    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final byte[] complete) {
        return createCompletedTxEntries(Collections.singletonMap(txId.toString(), complete));
    }

    private CompletableFuture<Void> createCompletedTxEntries(Map<String, byte[]> complete) {
        Integer batch = currentBatchSupplier.get();
        String tableName = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);

        Map<String, byte[]> map = complete.entrySet().stream().collect(Collectors.toMap(
                x -> String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, getScope(), getName(), x.getKey().toString()), Map.Entry::getValue));

        return Futures.toVoid(Futures.exceptionallyComposeExpecting(
                storeHelper.addNewEntriesIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, tableName, map),
                DATA_NOT_FOUND_PREDICATE, () -> tryCreateBatchTable(batch)
                        .thenCompose(v -> storeHelper.addNewEntriesIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, tableName, map))));
    }

    
    
    private CompletableFuture<Void> tryCreateBatchTable(int batch) {
        String batchTable = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);

        return storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .thenAccept(v -> log.debug("batches root table {}/{} created", NameUtils.INTERNAL_SCOPE_NAME,
                                  COMPLETED_TRANSACTIONS_BATCHES_TABLE))
                          .thenCompose(v -> storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                                  "" + batch, new byte[0]))
                          .thenCompose(v -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, batchTable));
    }

    @Override
    CompletableFuture<Data> getCompletedTx(final UUID txId) {
        List<Long> batches = new ArrayList<>();
        return storeHelper.getAllKeys(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .collectRemaining(x -> {
                              batches.add(Long.parseLong(x));
                              return true;
                          })
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
        return getMetadataTable()
                .thenCompose(id -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        id, TRUNCATION_KEY, truncationRecord)));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final Data truncationRecord) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, TRUNCATION_KEY, truncationRecord)
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, TRUNCATION_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<Data> getTruncationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, TRUNCATION_KEY);
                    }

                    return storeHelper.getCachedData(getScope(), metadataTable, TRUNCATION_KEY);
                });
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final Data configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, CONFIGURATION_KEY, configuration)
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, CONFIGURATION_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<Data> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, CONFIGURATION_KEY);
                    }

                    return storeHelper.getCachedData(getScope(), metadataTable, CONFIGURATION_KEY);
                });
    }

    @Override
    CompletableFuture<Version> setStateData(final Data state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, STATE_KEY, state)
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(getScope(), metadataTable, STATE_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<Data> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        storeHelper.invalidateCache(getScope(), metadataTable, STATE_KEY);
                    }

                    return storeHelper.getCachedData(getScope(), metadataTable, STATE_KEY);
                });
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(byte[] committingTxns) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, committingTxns)));
    }

    @Override
    CompletableFuture<Data> getCommitTxnRecord() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY));
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(Data update) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(getScope(), metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, update));
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] waitingRequestProcessor) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(getScope(),
                        metadataTable, WAITING_REQUEST_PROCESSOR_PATH, waitingRequestProcessor)));
    }

    @Override
    CompletableFuture<Data> getWaitingRequestNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(getScope(), metadataTable, WAITING_REQUEST_PROCESSOR_PATH));
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.removeEntry(getScope(), metadataTable, WAITING_REQUEST_PROCESSOR_PATH));
    }

    @Override
    public void refresh() {
        String id = idRef.get();
        if (!Strings.isNullOrEmpty(id)) {
            idRef.set(null);
            // refresh all mutable records
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), STATE_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), CONFIGURATION_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), TRUNCATION_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), EPOCH_TRANSITION_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), COMMITTING_TRANSACTIONS_RECORD_KEY);
            storeHelper.invalidateCache(getScope(), getMetadataTableName(id), CURRENT_EPOCH_KEY);
        }
    }
    // endregion
}
