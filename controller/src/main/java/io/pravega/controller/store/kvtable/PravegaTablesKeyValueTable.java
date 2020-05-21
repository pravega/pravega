/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTEpochRecord;
import io.pravega.controller.store.kvtable.records.KVTConfigurationRecord;
import io.pravega.controller.store.kvtable.records.KVTStateRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.*;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega KeyValueTable.
 * This creates a top level table per kvTable - metadataTable.
 * All metadata records are stored in metadata table.
 *
 * Each kvTable is protected against recreation of another kvTable/stream with same name by attaching a UUID to the name.
 */
@Slf4j
class PravegaTablesKeyValueTable extends PersistentKeyValueTableBase {
    private static final String METADATA_TABLE = "metadata" + SEPARATOR + "%s";
    private static final String EPOCHS_WITH_TRANSACTIONS_TABLE = "epochsWithTransactions" + SEPARATOR + "%s";
    private static final String WRITERS_POSITIONS_TABLE = "writersPositions" + SEPARATOR + "%s";
    private static final String TRANSACTIONS_IN_EPOCH_TABLE_FORMAT = "transactionsInEpoch-%d" + SEPARATOR + "%s";

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


    private final PravegaTablesStoreHelper storeHelper;
    private final Supplier<CompletableFuture<String>> metadataTableName;
    private final AtomicReference<String> idRef;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesKeyValueTable(final String scopeName, final String kvtName, PravegaTablesStoreHelper storeHelper,
                               Supplier<CompletableFuture<String>> tableName,
                               ScheduledExecutorService executor) {
        super(scopeName,kvtName);
        this.storeHelper = storeHelper;
        this.metadataTableName = tableName;
        this.idRef = new AtomicReference<>(null);
        this.executor = executor;
    }

    private CompletableFuture<String> getId() {
        String id = idRef.get();

        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            return metadataTableName.get()
                    .thenCompose(kvtsInScopeTable -> storeHelper.getEntry(kvtsInScopeTable, getName(),
                                                          x -> BitConverter.readUUID(x, 0)))
                                                  .thenComposeAsync(data -> {
                                                      idRef.compareAndSet(null, data.getObject().toString());
                                                      return getId();
                                                  });
        }
    }

    @Override
    public void refresh() {
        String id = idRef.getAndSet(null);
        if (!Strings.isNullOrEmpty(id)) {
            // refresh all mutable records
            storeHelper.invalidateCache(getMetadataTableName(id), STATE_KEY);
            storeHelper.invalidateCache(getMetadataTableName(id), CONFIGURATION_KEY);
        }
    }

    @Override
    public CompletableFuture<Long> getCreationTime() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(metadataTable, CREATION_TIME_KEY,
                        data -> BitConverter.readLong(data, 0))).thenApply(VersionedMetadata::getObject);
    }

    private CompletableFuture<String> getMetadataTable() {
        return getId().thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(METADATA_TABLE, id));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final KVTStateRecord state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY, state.toBytes())));
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<KVTStateRecord> state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY,
                        state.getObject().toBytes(), state.getVersion())
                        .thenApply(r -> {
                            storeHelper.invalidateCache(metadataTable, STATE_KEY);
                            return r;
                        }));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTStateRecord>> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, STATE_KEY, KVTStateRecord::fromBytes);
                    }
                    return storeHelper.getCachedData(metadataTable, STATE_KEY, KVTStateRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<KVTConfigurationRecord> configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, CONFIGURATION_KEY,
                        configuration.getObject().toBytes(), configuration.getVersion())
                        .thenApply(r -> {
                            storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY);
                            return r;
                        }));
    }

    @Override
    CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, CONFIGURATION_KEY, KVTConfigurationRecord::fromBytes);
                    }
                    return storeHelper.getCachedData(metadataTable, CONFIGURATION_KEY, KVTConfigurationRecord::fromBytes);
                });
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> checkKeyValueTableExists(final KeyValueTableConfiguration configuration,
                                                                             final long creationTime,
                                                                             final int startingSegmentNumber) {
        // If kvtable exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing kvtable has already been created successfully earlier,
        return storeHelper.expectingDataNotFound(getCreationTime(), null)
                .thenCompose(storedCreationTime -> {
                    if (storedCreationTime == null) {
                        return CompletableFuture.completedFuture(new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW,
                                configuration, creationTime, startingSegmentNumber));
                    } else {
                        return storeHelper.expectingDataNotFound(getConfiguration(), null)
                                .thenCompose(config -> {
                                    if (config != null) {
                                        return handleConfigExists(storedCreationTime, config, startingSegmentNumber,
                                                storedCreationTime == creationTime);
                                    } else {
                                        return CompletableFuture.completedFuture(
                                                new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.NEW,
                                                        configuration, storedCreationTime, startingSegmentNumber));
                                    }
                                });
                    }
                });
    }

    private CompletableFuture<CreateKVTableResponse> handleConfigExists(long creationTime, KeyValueTableConfiguration config,
                                                                       int startingSegmentNumber, boolean creationTimeMatched) {
        CreateKVTableResponse.CreateStatus status = creationTimeMatched ?
                CreateKVTableResponse.CreateStatus.NEW : CreateKVTableResponse.CreateStatus.EXISTS_CREATING;
        return storeHelper.expectingDataNotFound(getState(true), null)
                .thenApply(state -> {
                    if (state == null) {
                        return new CreateKVTableResponse(status, config, creationTime, startingSegmentNumber);
                    } else if (state.equals(KVTableState.UNKNOWN) || state.equals(KVTableState.CREATING)) {
                        return new CreateKVTableResponse(status, config, creationTime, startingSegmentNumber);
                    } else {
                        return new CreateKVTableResponse(CreateKVTableResponse.CreateStatus.EXISTS_ACTIVE,
                                config, creationTime, startingSegmentNumber);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createKVTableMetadata() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            return CompletableFuture.allOf(storeHelper.createTable(metadataTable))
                    .thenAccept(v -> log.debug("stream {}/{} metadata table {} created", getScope(), getName(), metadataTable));
        });
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CREATION_TIME_KEY, b)
                        .thenAccept(v -> storeHelper.invalidateCache(metadataTable, CREATION_TIME_KEY)));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final KVTConfigurationRecord configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CONFIGURATION_KEY, configuration.toBytes())
                        .thenAccept(v -> storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY)));
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, KVTEpochRecord data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, key, data.toBytes())
                        .thenAccept(v -> storeHelper.invalidateCache(metadataTable, key)));
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(KVTEpochRecord data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getEpoch());

        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.addNewEntryIfAbsent(metadataTable, CURRENT_EPOCH_KEY, epochData)
                            .thenAccept(v -> {
                                storeHelper.invalidateCache(metadataTable, CURRENT_EPOCH_KEY);
                            });
                });
    }

/*

    @Override
    public CompletableFuture<Long> getCreationTime() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(metadataTable, CREATION_TIME_KEY,
                        data -> BitConverter.readLong(data, 0))).thenApply(VersionedMetadata::getObject);
    }


*/
/*
    @Override
    public CompletableFuture<Void> deleteKeyValueTable() {
        // delete all tables for this stream even if they are not empty!
        // 1. read epoch table
        // delete all epoch txn specific tables
        // delete epoch txn base table
        // delete metadata table

        // delete stream in scope
        return getId()
                .thenCompose(id -> storeHelper.expectingDataNotFound(tryRemoveOlderTransactionsInEpochTables(epoch -> true), null)
                        .thenCompose(v -> getEpochsWithTransactionsTable()
                                .thenCompose(epochWithTxnTable -> storeHelper.expectingDataNotFound(
                                        storeHelper.deleteTable(epochWithTxnTable, false), null))
                                .thenCompose(deleted -> storeHelper.deleteTable(getMetadataTableName(id), false))));
    }

    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, key, data.toBytes())
                                                         .thenAccept(x -> storeHelper.invalidateCache(metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, key, HistoryTimeSeries::fromBytes);
                    }
                    return storeHelper.getCachedData(metadataTable, key, HistoryTimeSeries::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, VersionedMetadata<HistoryTimeSeries> data) {
        String key = String.format(HISTORY_TIMESERES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.updateEntry(metadataTable, key, data.getObject().toBytes(), data.getVersion())
                                      .thenApply(version -> {
                                          storeHelper.invalidateCache(metadataTable, key);
                                          return version;
                                      });
                });
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getEpoch());

        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.addNewEntryIfAbsent(metadataTable, CURRENT_EPOCH_KEY, epochData)
                                      .thenAccept(v -> {
                                          storeHelper.invalidateCache(metadataTable, CURRENT_EPOCH_KEY);
                                      });
                });
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getObject().getEpoch());

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, CURRENT_EPOCH_KEY, epochData, data.getVersion())
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(metadataTable, CURRENT_EPOCH_KEY);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    CompletableFuture<VersionedMetadata<Integer>> future;
                    if (ignoreCached) {
                        future = storeHelper.getEntry(metadataTable, CURRENT_EPOCH_KEY, x -> BitConverter.readInt(x, 0));
                    } else {
                        future = storeHelper.getCachedData(metadataTable, CURRENT_EPOCH_KEY, x -> BitConverter.readInt(x, 0));
                    }
                    return future.thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject())
                                              .thenApply(epochRecord -> new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
                    return storeHelper.getCachedData(metadataTable, key, EpochRecord::fromBytes);
                });
    }



    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, EPOCH_TRANSITION_KEY, epochTransition.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(metadataTable, EPOCH_TRANSITION_KEY)));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, EPOCH_TRANSITION_KEY,
                        epochTransition.getObject().toBytes(), epochTransition.getVersion())
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(metadataTable, EPOCH_TRANSITION_KEY);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(metadataTable, EPOCH_TRANSITION_KEY, EpochTransitionRecord::fromBytes));
    }

    private CompletableFuture<List<Integer>> getEpochsWithTransactions() {
        return getEpochsWithTransactionsTable()
                .thenCompose(epochWithTxnTable -> {
                    List<Integer> epochsWithTransactions = new ArrayList<>();
                    return storeHelper.getAllKeys(epochWithTxnTable)
                               .collectRemaining(x -> {
                                   epochsWithTransactions.add(Integer.parseInt(x));
                                   return true;
                               }).thenApply(v -> epochsWithTransactions);
                });
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<StreamConfigurationRecord> configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, CONFIGURATION_KEY,
                        configuration.getObject().toBytes(), configuration.getVersion())
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, CONFIGURATION_KEY, StreamConfigurationRecord::fromBytes);
                    }

                    return storeHelper.getCachedData(metadataTable, CONFIGURATION_KEY, StreamConfigurationRecord::fromBytes);
                });
    }




    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String waitingRequestProcessor) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(
                        metadataTable, WAITING_REQUEST_PROCESSOR_PATH, waitingRequestProcessor.getBytes(StandardCharsets.UTF_8))));
    }

    @Override
    CompletableFuture<String> getWaitingRequestNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(metadataTable, WAITING_REQUEST_PROCESSOR_PATH,
                        x -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(x)).toString()))
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.removeEntry(metadataTable, WAITING_REQUEST_PROCESSOR_PATH));
    }


    */

    // endregion
}
