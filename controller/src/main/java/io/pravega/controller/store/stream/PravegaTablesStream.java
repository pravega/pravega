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
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
    private static final String TRANSACTIONS_TABLE = STREAM_TABLE_PREFIX + "transactionsTable" + SEPARATOR + "%s";

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

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper,
                        Supplier<Integer> currentBatchSupplier, Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier) {
        this(scopeName, streamName, storeHelper, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE,
                SealedSegmentsMapShard.SHARD_SIZE, streamsInScopeTableNameSupplier);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper,
                        Supplier<Integer> currentBatchSupplier, int chunkSize, int shardSize,
                        Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;

        this.currentBatchSupplier = currentBatchSupplier;
        this.streamsInScopeTableNameSupplier = streamsInScopeTableNameSupplier;
        this.idRef = new AtomicReference<>(null);
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

    private CompletableFuture<String> getTransactionsTable() {
        return getId().thenApply(this::getTransactionsTableName);
    }

    private String getTransactionsTableName(String id) {
        return String.format(TRANSACTIONS_TABLE, getName(), id);
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        AtomicInteger count = new AtomicInteger(0);
        return getTransactionsTable()
                .thenCompose(table -> storeHelper.getAllKeys(getScope(), table)
                                                 .collectRemaining(x -> {
                                                     count.incrementAndGet();
                                                     return true;
                                                 })
                                                 .thenApply(v -> count.get()));
    }

    @Override
    public CompletableFuture<Void> deleteStream() {
        String scope = getScope();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        return getId()
                .thenCompose(id -> {
                    String transactionsTable = getTransactionsTableName(id);

                    return CompletableFuture.allOf(Futures.exceptionallyExpecting(
                            storeHelper.deleteTable(scope, transactionsTable, false),
                            DATA_NOT_FOUND_PREDICATE, null),
                            Futures.exceptionallyExpecting(
                                    storeHelper.deleteTable(scope, getMetadataTableName(id), false),
                                    DATA_NOT_FOUND_PREDICATE, null));
                });
    }

    @Override
    CompletableFuture<Void> createStreamMetadata() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            String epochTxnTable = getTransactionsTableName(id);
            return CompletableFuture.allOf(storeHelper.createTable(getScope(), metadataTable),
                    storeHelper.createTable(getScope(), epochTxnTable))
                                    .thenAccept(v -> log.debug("stream {}/{} metadata tables {} & {} created", getScope(), getName(), metadataTable,
                                            epochTxnTable));
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
                                                         .thenAccept(v -> storeHelper.invalidateCache(getScope(), metadataTable, key)));
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
        return getTxnWithFilter(x -> true);
    }

    @Override
    public CompletableFuture<Map<String, Data>> getTxnInEpoch(int epoch) {
        return getTxnWithFilter(x -> RecordHelper.getTransactionEpoch(UUID.fromString(x.getKey())) == epoch);
    }

    private CompletableFuture<Map<String, Data>> getTxnWithFilter(Predicate<Pair<String, Data>> filter) {
        String scope = getScope();
        Map<String, Data> result = new ConcurrentHashMap<>();
        return getTransactionsTable()
                .thenCompose(epochTxnTable -> Futures.exceptionallyExpecting(
                        storeHelper.getAllEntries(scope, epochTxnTable)
                                   .collectRemaining(x -> {
                                       // key is transaction id. 
                                       if (filter.test(x)) {
                                           result.put(x.getKey(), x.getValue());
                                       }
                                       return true;
                                   })
                                   .thenApply(x -> result), DATA_NOT_FOUND_PREDICATE, Collections.emptyMap()));

    }

    @Override
    CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final byte[] txnRecord) {
        String scope = getScope();
        return getTransactionsTable()
                .thenCompose(transactionsTable -> storeHelper.addNewEntry(scope, transactionsTable, txId.toString(), txnRecord));
    }

    @Override
    CompletableFuture<Data> getActiveTx(final int epoch, final UUID txId) {
        return getTransactionsTable()
                .thenCompose(epochTxnTable -> storeHelper.getEntry(getScope(), epochTxnTable, txId.toString()));
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final Data data) {
        return getTransactionsTable()
                .thenCompose(epochTxnTable -> storeHelper.updateEntry(getScope(), epochTxnTable, txId.toString(), data));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        return getTransactionsTable()
                .thenCompose(epochTransactionsTableName ->
                        storeHelper.removeEntry(getScope(), epochTransactionsTableName, txId.toString()));
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final byte[] complete) {
        Integer batch = currentBatchSupplier.get();
        String tableName = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);

        String key = String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, getScope(), getName(), txId.toString());

        return Futures.exceptionallyComposeExpecting(
                Futures.toVoid(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, tableName, key, complete)),
                DATA_NOT_FOUND_PREDICATE, () -> tryCreateBatchTable(batch, key, complete));
    }

    private CompletableFuture<Void> tryCreateBatchTable(int batch, String key, byte[] complete) {
        String batchTable = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch);

        return storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .thenAccept(v -> log.debug("batches root table {}/{} created", NameUtils.INTERNAL_SCOPE_NAME,
                                  COMPLETED_TRANSACTIONS_BATCHES_TABLE))
                          .thenCompose(v -> storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                                  "" + batch, new byte[0]))
                          .thenCompose(v -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, batchTable))
                          .thenCompose(v -> {
                              log.debug("batch table {} created", batchTable);
                              return Futures.toVoid(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, batchTable, key, complete));
                          });
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
