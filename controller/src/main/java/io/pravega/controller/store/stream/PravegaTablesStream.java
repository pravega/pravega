/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.stream.records.StreamSubscriber;
import io.pravega.controller.store.stream.records.SubscriberSet;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
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
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega Table Stream.
 * This creates two top level tables per stream - metadataTable, epochsWithTransactionsTable.
 * All metadata records are stored in metadata table.
 * EpochsWithTransactions is a top level table for storing epochs where any transaction was created.
 * This class is coded for transaction ids that follow the scheme that msb 32 bits represent epoch.
 * Each stream table is protected against recreation of stream by attaching a unique id to the stream when it is created.
 */
@Slf4j
class PravegaTablesStream extends PersistentStreamBase {
    private static final String METADATA_TABLE = "metadata" + SEPARATOR + "%s";
    private static final String EPOCHS_WITH_TRANSACTIONS_TABLE = "epochsWithTransactions" + SEPARATOR + "%s";
    private static final String WRITERS_POSITIONS_TABLE = "writersPositions" + SEPARATOR + "%s";
    private static final String SUBSCRIBERS_TABLE = "subscribers" + SEPARATOR + "%s";
    private static final String TRANSACTIONS_IN_EPOCH_TABLE_FORMAT = "transactionsInEpoch-%d" + SEPARATOR + "%s";

    // metadata keys
    private static final String CREATION_TIME_KEY = "creationTime";
    private static final String CONFIGURATION_KEY = "configuration";
    private static final String TRUNCATION_KEY = "truncation";
    private static final String STATE_KEY = "state";
    private static final String EPOCH_TRANSITION_KEY = "epochTransition";
    private static final String RETENTION_SET_KEY = "retention";
    private static final String SUBSCRIBER_KEY = "subscriber-";
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
    private static final String SUBSCRIBER_KEY_PREFIX = "subscriber_";
    private static final String SUBSCRIBER_SET_KEY = "subscriberset";
    
    // non existent records
    private static final VersionedMetadata<ActiveTxnRecord> NON_EXISTENT_TXN = 
            new VersionedMetadata<>(ActiveTxnRecord.EMPTY, new Version.LongVersion(Long.MIN_VALUE));

    private final PravegaTablesStoreHelper storeHelper;

    private final Supplier<Integer> currentBatchSupplier;
    private final Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier;
    private final AtomicReference<String> idRef;
    private final ZkOrderedStore txnCommitOrderer;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, ZkOrderedStore txnCommitOrderer,
                        Supplier<Integer> currentBatchSupplier, Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier,
                        ScheduledExecutorService executor) {
        this(scopeName, streamName, storeHelper, txnCommitOrderer, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE,
                SealedSegmentsMapShard.SHARD_SIZE, streamsInScopeTableNameSupplier, executor);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, ZkOrderedStore txnCommitOrderer,
                        Supplier<Integer> currentBatchSupplier, int chunkSize, int shardSize,
                        Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier, ScheduledExecutorService executor) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;
        this.txnCommitOrderer = txnCommitOrderer;
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
                                                  .thenCompose(streamsInScopeTable ->
                                                          storeHelper.getEntry(streamsInScopeTable, getName(),
                                                          x -> BitConverter.readUUID(x, 0)))
                                                  .thenComposeAsync(data -> {
                                                      idRef.compareAndSet(null, data.getObject().toString());
                                                      return getId();
                                                  });
        }
    }

    private CompletableFuture<String> getMetadataTable() {
        return getId().thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(METADATA_TABLE, id));
    }

    private CompletableFuture<String> getEpochsWithTransactionsTable() {
        return getId().thenApply(this::getEpochsWithTransactionsTableName);
    }

    private String getEpochsWithTransactionsTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, id));
    }

    private CompletableFuture<String> getTransactionsInEpochTable(int epoch) {
        return getId().thenApply(id -> getTransactionsInEpochTableName(epoch, id));
    }

    private String getTransactionsInEpochTableName(int epoch, String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, epoch, id));
    }

    private CompletableFuture<String> getWritersTable() {
        return getId().thenApply(this::getWritersTableName);
    }

    private String getWritersTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(WRITERS_POSITIONS_TABLE, id));
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
        CompletableFuture<Void> future;
        if (record.getObject().getTransactionsToCommit().size() == 0) {
            future = CompletableFuture.completedFuture(null);
        } else {
            future = generateMarksForTransactions(record.getObject())
                .thenCompose(v -> createCompletedTxEntries(completedRecords))
                    .thenCompose(x -> getTransactionsInEpochTable(record.getObject().getEpoch())
                            .thenCompose(table -> storeHelper.removeEntries(table, completedRecords.keySet())))
                    .thenCompose(x -> tryRemoveOlderTransactionsInEpochTables(epoch -> epoch < record.getObject().getEpoch()));
        }
        return future
                .thenCompose(x -> Futures.toVoid(updateCommittingTxnRecord(new VersionedMetadata<>(CommittingTransactionsRecord.EMPTY,
                        record.getVersion()))));
    }

    @Override
    CompletableFuture<Void> createStreamMetadata() {
        return getId().thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            String epochWithTxnTable = getEpochsWithTransactionsTableName(id);
            String writersPositionsTable = getWritersTableName(id);
            return CompletableFuture.allOf(storeHelper.createTable(metadataTable),
                    storeHelper.createTable(epochWithTxnTable), storeHelper.createTable(writersPositionsTable))
                                    .thenAccept(v -> log.debug("stream {}/{} metadata tables {}, {} {} & {} created", getScope(), getName(), metadataTable,
                                            epochWithTxnTable, writersPositionsTable));
        });
    }

    @Override
    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime,
                                                                     final int startingSegmentNumber) {
        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier,
        return storeHelper.expectingDataNotFound(getCreationTime(), null)
                      .thenCompose(storedCreationTime -> {
                          if (storedCreationTime == null) {
                              return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                      configuration, creationTime, startingSegmentNumber));
                          } else {
                              return storeHelper.expectingDataNotFound(getConfiguration(), null)
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
        return storeHelper.expectingDataNotFound(getState(true), null)
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
                .thenCompose(metadataTable -> storeHelper.getCachedData(metadataTable, CREATION_TIME_KEY,
                        data -> BitConverter.readLong(data, 0))).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> createSubscriber(String newSubscriber) {
        final StreamSubscriber newSubscriberRecord = new StreamSubscriber(newSubscriber, ImmutableMap.of(), System.currentTimeMillis());
        return getMetadataTable()
                .thenCompose(metadataTable -> getSubscriberSetRecord(true)
                        .thenCompose(subscriberSetRecord -> storeHelper.updateEntry(metadataTable, SUBSCRIBER_SET_KEY,
                                SubscriberSet.add(subscriberSetRecord.getObject(), newSubscriber).toBytes(), subscriberSetRecord.getVersion())
                                .thenCompose(v -> storeHelper.addNewEntryIfAbsent(metadataTable, getKeyForSubscriber(newSubscriber), newSubscriberRecord.toBytes()))
                                .thenAccept(v -> storeHelper.invalidateCache(metadataTable, SUBSCRIBER_SET_KEY))
                                .thenAccept(v -> storeHelper.invalidateCache(metadataTable, getKeyForSubscriber(newSubscriber)))));
    }

    @Override
    public CompletableFuture<Void> createSubscribersRecordIfAbsent() {
        return Futures.exceptionallyExpecting(getSubscriberSetRecord(true),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                .thenCompose( subscriberSet -> {
                    if (subscriberSet == null) {
                        SubscriberSet emptySubSet = new SubscriberSet(ImmutableList.of());
                        return Futures.toVoid(getMetadataTable()
                                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, SUBSCRIBER_SET_KEY, emptySubSet.toBytes())));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
        });
    }

    public CompletableFuture<VersionedMetadata<SubscriberSet>> getSubscriberSetRecord(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(table -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(table, SUBSCRIBER_SET_KEY, SubscriberSet::fromBytes);
                    }
                    return storeHelper.getCachedData(table, SUBSCRIBER_SET_KEY, SubscriberSet::fromBytes);
                });
    }

    @Override
    public CompletableFuture<Void> updateSubscriberStreamCut(final StreamSubscriber streamSubscriber) {
        return Futures.toVoid(getSubscriberRecord(streamSubscriber.getSubscriber())
                 .thenCompose(record -> getMetadataTable().thenCompose(table ->
                    storeHelper.updateEntry(table, getKeyForSubscriber(streamSubscriber.getSubscriber()),
                                                                        streamSubscriber.toBytes(), record.getVersion())
                    .thenAccept( v -> storeHelper.invalidateCache(table, getKeyForSubscriber(streamSubscriber.getSubscriber()))))));

    }

    @Override
    public CompletableFuture<Void> removeSubscriber(String subscriber) {
        return Futures.toVoid(getSubscriberSetRecord(true)
                .thenCompose(subscriberSetRecord -> getMetadataTable().thenCompose(table -> {
                    SubscriberSet subSet = SubscriberSet.remove(subscriberSetRecord.getObject(), subscriber);
                    return storeHelper.updateEntry(table, SUBSCRIBER_SET_KEY, subSet.toBytes(), subscriberSetRecord.getVersion())
                            .thenCompose(v -> getSubscriberRecord(subscriber)
                            .thenCompose(subscriberRecord -> storeHelper.removeEntry(table, getKeyForSubscriber(subscriber)))
                            .thenAccept(x -> storeHelper.invalidateCache(table, SUBSCRIBER_SET_KEY))
                            .thenAccept(x -> storeHelper.invalidateCache(table, getKeyForSubscriber(subscriber))));
                })));
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriberRecord(final String subscriber) {
        return getMetadataTable()
                .thenCompose(table -> storeHelper.getEntry(table, getKeyForSubscriber(subscriber), StreamSubscriber::fromBytes));
    }

    private String getKeyForSubscriber(final String subscriber) {
        return SUBSCRIBER_KEY_PREFIX + subscriber;
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers() {
        return getMetadataTable()
                .thenCompose(table -> getSubscriberSetRecord(true)
                        .thenApply(subscribersSet -> subscribersSet.getObject().getSubscribers()));
    }

    @Override
    public CompletableFuture<Void> deleteStream() {
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
    CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.addNewEntryIfAbsent(metadataTable, RETENTION_SET_KEY, data.toBytes())
                                      .thenAccept(v -> storeHelper.invalidateCache(metadataTable, RETENTION_SET_KEY));
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(metadataTable, RETENTION_SET_KEY, RetentionSet::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> retention) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return storeHelper.updateEntry(metadataTable, RETENTION_SET_KEY, retention.getObject().toBytes(), retention.getVersion())
                                      .thenApply(v -> {
                                          storeHelper.invalidateCache(metadataTable, RETENTION_SET_KEY);
                                          return v;
                                      });
                });
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, StreamCutRecord record) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, key, record.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(metadataTable, key, StreamCutRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.removeEntry(metadataTable, key)
                                                         .thenAccept(x -> storeHelper.invalidateCache(metadataTable, key)));
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
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, key, data.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(metadataTable, key)))
                .thenCompose(v -> {
                    if (data.getEpoch() == data.getReferenceEpoch()) {
                        // this is an original epoch. we should create transactions in epoch table
                        return createTransactionsInEpochTable(epoch);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
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
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, SealedSegmentsMapShard data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, key, data.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(metadataTable, key)));
    }

    @Override
    CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(int shard) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(metadataTable, key, SealedSegmentsMapShard::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, VersionedMetadata<SealedSegmentsMapShard> data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, key, data.getObject().toBytes(), data.getVersion())
                                                         .thenApply(v -> {
                                                             storeHelper.invalidateCache(metadataTable, key);
                                                             return v;
                                                         }));
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecords(Collection<Long> segmentsToSeal, int epoch) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, epoch);

        Map<String, byte[]> map = segmentsToSeal.stream().collect(Collectors.toMap(
                x -> String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, x), x -> epochData));

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntriesIfAbsent(metadataTable, map));
    }

    @Override
    CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedData(metadataTable, key, x -> BitConverter.readInt(x, 0)));
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

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CREATION_TIME_KEY, b)
                                                         .thenAccept(v -> storeHelper.invalidateCache(metadataTable, CREATION_TIME_KEY)));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, CONFIGURATION_KEY, configuration.toBytes())
                                                         .thenAccept(v -> storeHelper.invalidateCache(metadataTable, CONFIGURATION_KEY)));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final StateRecord state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY, state.toBytes())));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, key, b)));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        byte[] marker = new byte[Long.BYTES];
        BitConverter.writeLong(marker, 0, data.getObject());
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, key, marker, data.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable().thenCompose(metadataTable ->
                storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(metadataTable, key, x -> BitConverter.readLong(x, 0)), null));
    }

    @Override
    CompletableFuture<Void> removeMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(id -> storeHelper.removeEntry(id, key));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
        return getEpochsWithTransactions()
                .thenCompose(epochsWithTransactions -> {
                    return Futures.allOfWithResults(epochsWithTransactions.stream().map(this::getTxnInEpoch).collect(Collectors.toList()));
                }).thenApply(list -> {
            Map<UUID, ActiveTxnRecord> map = new HashMap<>();
            list.forEach(map::putAll);
            return map;
        });
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
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        return getEpochsWithTransactionsTable()
                .thenCompose(epochsWithTxn -> storeHelper.getAllKeys(epochsWithTxn)
                                                         .forEachRemaining(x -> {
                                                             futures.add(getNumberOfOngoingTransactions(Integer.parseInt(x)));
                                                         }, executor)
                                                         .thenCompose(v -> Futures.allOfWithResults(futures)
                                                                                  .thenApply(list -> list.stream().reduce(0, Integer::sum))));
    }

    private CompletableFuture<Integer> getNumberOfOngoingTransactions(int epoch) {
        AtomicInteger count = new AtomicInteger(0);
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTableName -> storeHelper.getAllKeys(epochTableName).forEachRemaining(x -> count.incrementAndGet(), executor)
                                                          .thenApply(x -> count.get()));
    }

    @Override
    public CompletableFuture<List<Map.Entry<UUID, ActiveTxnRecord>>> getOrderedCommittingTxnInLowestEpoch(int limit) {
        return super.getOrderedCommittingTxnInLowestEpochHelper(txnCommitOrderer, limit, executor);
    }

    @Override
    @VisibleForTesting
    CompletableFuture<Map<Long, UUID>> getAllOrderedCommittingTxns() {
        return super.getAllOrderedCommittingTxnsHelper(txnCommitOrderer);
    }

    @Override
    CompletableFuture<List<ActiveTxnRecord>> getTransactionRecords(int epoch, List<String> txnIds) {
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTxnTable -> storeHelper.getEntries(epochTxnTable, txnIds, 
                        ActiveTxnRecord::fromBytes, NON_EXISTENT_TXN))
        .thenApply(res -> res.stream().map(VersionedMetadata::getObject).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnInEpoch(int epoch) {
        Map<UUID, ActiveTxnRecord> result = new ConcurrentHashMap<>();
        return getTransactionsInEpochTable(epoch)
            .thenCompose(tableName -> storeHelper.expectingDataNotFound(storeHelper.getAllEntries(
                    tableName, ActiveTxnRecord::fromBytes).collectRemaining(x -> {
                        result.put(UUID.fromString(x.getKey()), x.getValue().getObject());
                        return true;
            }).thenApply(v -> result), Collections.emptyMap()));
    }

    @Override
    public CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final ActiveTxnRecord txnRecord) {
        // create txn ==>
        // if epoch table exists, add txn to epoch
        //  1. add epochs_with_txn entry for the epoch
        //  2. create txns-in-epoch table
        //  3. create txn in txns-in-epoch
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTable -> storeHelper.addNewEntryIfAbsent(epochTable, txId.toString(), txnRecord.toBytes()));
    }

    private CompletableFuture<Void> createTransactionsInEpochTable(int epoch) {
        return getEpochsWithTransactionsTable()
                .thenCompose(epochsWithTxnTable -> {
                    return storeHelper.addNewEntryIfAbsent(epochsWithTxnTable, Integer.toString(epoch), new byte[0]);
                }).thenCompose(epochTxnEntryCreated -> {
                    return getTransactionsInEpochTable(epoch)
                            .thenCompose(storeHelper::createTable);
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(final int epoch, final UUID txId) {
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTxnTable -> storeHelper.getEntry(epochTxnTable, txId.toString(), ActiveTxnRecord::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final VersionedMetadata<ActiveTxnRecord> data) {
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTxnTable -> storeHelper.updateEntry(epochTxnTable, txId.toString(), data.getObject().toBytes(), data.getVersion()));
    }

    @Override
    CompletableFuture<Long> addTxnToCommitOrder(UUID txId) {
        return txnCommitOrderer.addEntity(getScope(), getName(), txId.toString());
    }

    @Override
    CompletableFuture<Void> removeTxnsFromCommitOrder(List<Long> orderedPositions) {
        return txnCommitOrderer.removeEntities(getScope(), getName(), orderedPositions);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        // 1. remove txn from txn-in-epoch table
        // 2. get current epoch --> if txn-epoch < activeEpoch.reference epoch, try deleting empty epoch table.
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTransactionsTableName ->
                        storeHelper.removeEntry(epochTransactionsTableName, txId.toString()))
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
                        storeHelper.deleteTable(epochTable, true)
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
                                .thenCompose(table -> storeHelper.removeEntry(table, Integer.toString(epoch)));
                          } else {
                              return CompletableFuture.completedFuture(null);
                          }
                      }));
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final CompletedTxnRecord complete) {
        return createCompletedTxEntries(Collections.singletonMap(txId.toString(), complete.toBytes()));
    }

    private CompletableFuture<Void> createCompletedTxEntries(Map<String, byte[]> complete) {
        Integer batch = currentBatchSupplier.get();
        String tableName = getCompletedTransactionsBatchTableName(batch);

        Map<String, byte[]> map = complete.entrySet().stream().collect(Collectors.toMap(
                x -> getCompletedTransactionKey(getScope(), getName(), x.getKey()), Map.Entry::getValue));

        return Futures.toVoid(Futures.exceptionallyComposeExpecting(
                storeHelper.addNewEntriesIfAbsent(tableName, map),
                DATA_NOT_FOUND_PREDICATE, () -> tryCreateBatchTable(batch)
                        .thenCompose(v -> storeHelper.addNewEntriesIfAbsent(tableName, map))))
                .exceptionally(e -> {
                    throw new CompletionException(e);
                });
    }

    @VisibleForTesting
    static String getCompletedTransactionKey(String scope, String stream, String txnId) {
        return String.format(COMPLETED_TRANSACTIONS_KEY_FORMAT, scope, stream, txnId);
    }

    @VisibleForTesting
    static String getCompletedTransactionsBatchTableName(int batch) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME,
                String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, batch));
    }


    private CompletableFuture<Void> tryCreateBatchTable(int batch) {
        String batchTable = getCompletedTransactionsBatchTableName(batch);

        return storeHelper.createTable(COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .thenAccept(v -> log.debug("batches root table {} created", COMPLETED_TRANSACTIONS_BATCHES_TABLE))
                          .thenCompose(v -> storeHelper.addNewEntryIfAbsent(COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                                  Integer.toString(batch), new byte[0]))
                          .thenCompose(v -> storeHelper.createTable(batchTable));
    }

    @Override
    CompletableFuture<VersionedMetadata<CompletedTxnRecord>> getCompletedTx(final UUID txId) {
        List<Integer> batches = new ArrayList<>();
        return storeHelper.getAllKeys(COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .collectRemaining(x -> {
                              batches.add(Integer.parseInt(x));
                              return true;
                          })
                          .thenCompose(v -> {
                              return Futures.allOfWithResults(batches.stream().map(batch -> {
                                  String table = getCompletedTransactionsBatchTableName(batch);
                                  String key = getCompletedTransactionKey(getScope(), getName(), txId.toString());

                                  return storeHelper.expectingDataNotFound(
                                          storeHelper.getCachedData(table, key, CompletedTxnRecord::fromBytes), null);
                              }).collect(Collectors.toList()));
                          })
                          .thenCompose(result -> {
                              Optional<VersionedMetadata<CompletedTxnRecord>> any = result.stream().filter(Objects::nonNull).findFirst();
                              if (any.isPresent()) {
                                  return CompletableFuture.completedFuture(any.get());
                              } else {
                                  throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "Completed Txn not found");
                              }
                          });
    }

    @Override
    public CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncationRecord) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable,
                        TRUNCATION_KEY, truncationRecord.toBytes())));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final VersionedMetadata<StreamTruncationRecord> truncationRecord) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, TRUNCATION_KEY,
                        truncationRecord.getObject().toBytes(), truncationRecord.getVersion())
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(metadataTable, TRUNCATION_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, TRUNCATION_KEY, StreamTruncationRecord::fromBytes);
                    }

                    return storeHelper.getCachedData(metadataTable, TRUNCATION_KEY, StreamTruncationRecord::fromBytes);
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
    CompletableFuture<Version> setStateData(final VersionedMetadata<StateRecord> state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY,
                        state.getObject().toBytes(), state.getVersion())
                                                         .thenApply(r -> {
                                                             storeHelper.invalidateCache(metadataTable, STATE_KEY);
                                                             return r;
                                                         }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        return storeHelper.getEntry(metadataTable, STATE_KEY, StateRecord::fromBytes);
                    }

                    return storeHelper.getCachedData(metadataTable, STATE_KEY, StateRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(
                        metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, committingTxns.toBytes())));
    }

    @Override
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getEntry(metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY,
                        CommittingTransactionsRecord::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> update) {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY,
                        update.getObject().toBytes(), update.getVersion()));
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

    @Override
    CompletableFuture<Void> createWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position) {
        WriterMark mark = new WriterMark(timestamp, position);
        return Futures.toVoid(getWritersTable()
                .thenCompose(table -> storeHelper.addNewEntry(table, writer, mark.toBytes())));
    }

    @Override
    public CompletableFuture<Void> removeWriterRecord(String writer, Version version) {
        return getWritersTable()
                .thenCompose(table -> storeHelper.removeEntry(table, writer, version));
    }

    @Override
    CompletableFuture<VersionedMetadata<WriterMark>> getWriterMarkRecord(String writer) {
        return getWritersTable()
                .thenCompose(table -> storeHelper.getEntry(table, writer, WriterMark::fromBytes));
    }

    @Override
    CompletableFuture<Void> updateWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position, 
                                                   boolean isAlive, Version version) {
        WriterMark mark = new WriterMark(timestamp, position, isAlive);
        return Futures.toVoid(getWritersTable()
                .thenCompose(table -> storeHelper.updateEntry(table, writer, mark.toBytes(), version)));
    }

    @Override
    public CompletableFuture<Map<String, WriterMark>> getAllWriterMarks() {
        Map<String, WriterMark> result = new ConcurrentHashMap<>();

        return getWritersTable()
                .thenCompose(table -> storeHelper.getAllEntries(table, WriterMark::fromBytes)
                .collectRemaining(x -> {
                    result.put(x.getKey(), x.getValue().getObject());
                    return true;
                })).thenApply(v -> result);
    }

    @Override
    public void refresh() {
        String id = idRef.getAndSet(null);
        if (!Strings.isNullOrEmpty(id)) {
            // refresh all mutable records
            storeHelper.invalidateCache(getMetadataTableName(id), STATE_KEY);
            storeHelper.invalidateCache(getMetadataTableName(id), CONFIGURATION_KEY);
            storeHelper.invalidateCache(getMetadataTableName(id), TRUNCATION_KEY);
            storeHelper.invalidateCache(getMetadataTableName(id), EPOCH_TRANSITION_KEY);
            storeHelper.invalidateCache(getMetadataTableName(id), COMMITTING_TRANSACTIONS_RECORD_KEY);
            storeHelper.invalidateCache(getMetadataTableName(id), CURRENT_EPOCH_KEY);
        }
    }
    // endregion
}
