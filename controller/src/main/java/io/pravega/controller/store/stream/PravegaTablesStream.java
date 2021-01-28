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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
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
import io.pravega.controller.store.stream.records.Subscribers;
import lombok.Data;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.PravegaTablesStoreHelper.*;
import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_EMPTY_PREDICATE;
import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.WRITE_CONFLICT_PREDICATE;
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
    private static final String HISTORY_TIMESERIES_CHUNK_FORMAT = "historyTimeSeriesChunk-%d";
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
    private final Context context;
    private final long operationStartTime;

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, ZkOrderedStore txnCommitOrderer,
                        Supplier<Integer> currentBatchSupplier, Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier,
                        ScheduledExecutorService executor, boolean withContext) {
        this(scopeName, streamName, storeHelper, txnCommitOrderer, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE,
                SealedSegmentsMapShard.SHARD_SIZE, streamsInScopeTableNameSupplier, executor, withContext);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, ZkOrderedStore txnCommitOrderer,
                        Supplier<Integer> currentBatchSupplier, int chunkSize, int shardSize,
                        Supplier<CompletableFuture<String>> streamsInScopeTableNameSupplier, ScheduledExecutorService executor,
                        boolean withContext) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;
        this.txnCommitOrderer = txnCommitOrderer;
        this.currentBatchSupplier = currentBatchSupplier;
        this.streamsInScopeTableNameSupplier = streamsInScopeTableNameSupplier;
        this.idRef = new AtomicReference<>(null);
        this.executor = executor;
        this.context = withContext ? new Context() : null;
        this.operationStartTime = System.currentTimeMillis();
    }

    private CompletableFuture<String> getId() {
        String id = idRef.get();

        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            return streamsInScopeTableNameSupplier.get()
                                                  .thenCompose(streamsInScopeTable ->
                                                          storeHelper.getCachedOrLoad(streamsInScopeTable, getName(),
                                                          BYTES_TO_UUID_FUNCTION, operationStartTime))
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
                .thenCompose(metadataTable -> 
                        getAndLoadToContext(metadataTable, CREATION_TIME_KEY, Long.class, BYTES_TO_LONG_FUNCTION))
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> addSubscriber(String newSubscriber, long newGeneration) {
        return createSubscribersRecordIfAbsent()
                   .thenCompose(y -> getSubscriberSetRecord(true))
                   .thenCompose(subscriberSetRecord -> {
                      if (!subscriberSetRecord.getObject().getSubscribers().contains(newSubscriber)) {
                          // update Subscriber generation, if it is greater than current generation
                          return getMetadataTable()
                                  .thenCompose(metaTable -> {
                                      Subscribers newSubscribers = Subscribers.add(subscriberSetRecord.getObject(), newSubscriber);
                                      return updateAndLoadToContext(metaTable, SUBSCRIBER_SET_KEY, newSubscribers,
                                              Subscribers::toBytes, subscriberSetRecord.getVersion());
                                  });
                      }
                      return CompletableFuture.completedFuture(null);
                   })
                    .thenCompose(v -> Futures.exceptionallyExpecting(getSubscriberRecord(newSubscriber),
                            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                            .thenCompose(subscriberRecord -> {
                                if (subscriberRecord == null) {
                                    final StreamSubscriber newSubscriberRecord = new StreamSubscriber(newSubscriber, newGeneration,
                                                                                                        ImmutableMap.of(), System.currentTimeMillis());
                                    return Futures.toVoid(getMetadataTable().thenApply(metadataTable -> 
                                            addAndLoadToContext(metadataTable, getKeyForSubscriber(newSubscriber),
                                                    newSubscriberRecord, StreamSubscriber::toBytes, true)));
                                } else {
                                    // just update the generation if subscriber already exists...
                                    if (subscriberRecord.getObject().getGeneration() < newGeneration) {
                                      return Futures.toVoid(setSubscriberData(new VersionedMetadata<>(new StreamSubscriber(newSubscriber, newGeneration,
                                              subscriberRecord.getObject().getTruncationStreamCut(), System.currentTimeMillis()),
                                              subscriberRecord.getVersion())));
                                    }
                                }
                                return CompletableFuture.completedFuture(null);
                            })
                    );
    }

    @Override
    public CompletableFuture<Void> createSubscribersRecordIfAbsent() {
        return Futures.exceptionallyExpecting(getSubscriberSetRecord(true),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                .thenCompose(subscriberSetRecord -> {
                    if (subscriberSetRecord == null) {
                        return Futures.toVoid(getMetadataTable()
                                .thenCompose(metadataTable -> addAndLoadToContext(metadataTable, SUBSCRIBER_SET_KEY, 
                                        Subscribers.EMPTY_SET, Subscribers::toBytes, true)));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<VersionedMetadata<Subscribers>> getSubscriberSetRecord(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(table -> {
                    if (ignoreCached) {
                        unloadFromContext(table, SUBSCRIBER_SET_KEY);
                    }
                    return getAndLoadToContext(table, SUBSCRIBER_SET_KEY, Subscribers.class, Subscribers::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> setSubscriberData(final VersionedMetadata<StreamSubscriber> streamSubscriber) {
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, getKeyForSubscriber(streamSubscriber.getObject().getSubscriber()),
                        streamSubscriber.getObject(), StreamSubscriber::toBytes, streamSubscriber.getVersion()));
    }

    @Override
    public CompletableFuture<Void> deleteSubscriber(final String subscriber, final long generation) {
        return getSubscriberRecord(subscriber).thenCompose(subs -> {
            if (generation < subs.getObject().getGeneration()) {
                log.warn("skipped deleting subscriber {} due to generation mismatch", subscriber);
                return CompletableFuture.completedFuture(null);
            }
            return getMetadataTable().thenCompose(table -> storeHelper.removeEntry(table, getKeyForSubscriber(subscriber))
                    .thenAccept(x -> storeHelper.invalidateCache(table, getKeyForSubscriber(subscriber)))
                    .thenCompose(v -> getSubscriberSetRecord(true)
                            .thenCompose(subscriberSetRecord -> {
                                if (subscriberSetRecord.getObject().getSubscribers().contains(subscriber)) {
                                    Subscribers subSet = Subscribers.remove(subscriberSetRecord.getObject(), subscriber);
                                    return Futures.toVoid(updateAndLoadToContext(table, SUBSCRIBER_SET_KEY, subSet, x -> x.toBytes(), subscriberSetRecord.getVersion()));
                                }
                                return CompletableFuture.completedFuture(null);
                            })));
        });
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriberRecord(final String subscriber) {
        return getMetadataTable()
                .thenCompose(table -> storeHelper.getEntry(table, getKeyForSubscriber(subscriber), StreamSubscriber::fromBytes));
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers() {
        return getMetadataTable()
                .thenCompose(table -> getSubscriberSetRecord(true)
                        .thenApply(subscribersSet -> subscribersSet.getObject().getSubscribers().asList()));
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
                                .thenCompose(deleted -> storeHelper.deleteTable(getMetadataTableName(id), false))))
                .thenAccept(v -> clearContext());
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(metadataTable, RETENTION_SET_KEY, data, RetentionSet::toBytes, true)));
    }

    @Override
    CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData() {
        return getMetadataTable()
                .thenCompose(metadataTable -> getAndLoadToContext(metadataTable, RETENTION_SET_KEY, RetentionSet.class, 
                        RetentionSet::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> retention) {
        return getMetadataTable()
                .thenCompose(metadataTable -> 
                        updateAndLoadToContext(metadataTable, RETENTION_SET_KEY, retention.getObject(), RetentionSet::toBytes, retention.getVersion()));
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, StreamCutRecord record) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(metadataTable, key, record, 
                        StreamCutRecord::toBytes, true)));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable()
                .thenCompose(metadataTable -> getAndLoadToContext(metadataTable, key, StreamCutRecord.class,
                        StreamCutRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);

        return getMetadataTable()
                .thenCompose(metadataTable -> removeAndUnload(metadataTable, key, null));
    }

    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data) {
        String key = String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> 
                        Futures.toVoid(addAndLoadToContext(metadataTable, key, data, HistoryTimeSeries::toBytes, true)));
    }

    @Override
    CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        String key = String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unloadFromContext(metadataTable, key);
                        storeHelper.invalidateCache(metadataTable, key);
                    }
                    return getAndLoadToContext(metadataTable, key, HistoryTimeSeries.class, HistoryTimeSeries::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, VersionedMetadata<HistoryTimeSeries> data) {
        String key = String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, key, data.getObject(), 
                        HistoryTimeSeries::toBytes, data.getVersion()));
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    return Futures.toVoid(addAndLoadToContext(metadataTable, CURRENT_EPOCH_KEY, data.getEpoch(), INTEGER_TO_BYTES_FUNCTION, true));
                });
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> data) {
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, CURRENT_EPOCH_KEY, data.getObject().getEpoch(), 
                        INTEGER_TO_BYTES_FUNCTION, data.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unloadFromContext(metadataTable, CURRENT_EPOCH_KEY);
                        storeHelper.invalidateCache(metadataTable, CURRENT_EPOCH_KEY);
                    }
                    return getAndLoadToContext(metadataTable, CURRENT_EPOCH_KEY, Integer.class, BYTES_TO_INTEGER_FUNCTION)
                            .thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject())
                                              .thenApply(epochRecord -> new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
                });
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data) {
        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable()
                .thenCompose(metadataTable -> addAndLoadToContext(metadataTable, key, data, EpochRecord::toBytes, true))
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
                    return getAndLoadToContext(metadataTable, key, EpochRecord.class, EpochRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, SealedSegmentsMapShard data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(metadataTable, key, data, SealedSegmentsMapShard::toBytes, true)));
    }

    @Override
    CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(int shard) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> getAndLoadToContext(metadataTable, key, SealedSegmentsMapShard.class,
                         SealedSegmentsMapShard::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, VersionedMetadata<SealedSegmentsMapShard> data) {
        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, key, data.getObject(), SealedSegmentsMapShard::toBytes, data.getVersion()));
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecords(Collection<Long> segmentsToSeal, int epoch) {
        byte[] epochData = INTEGER_TO_BYTES_FUNCTION.apply(epoch);
        VersionedMetadata<Integer> epochVersionedData = new VersionedMetadata<>(epoch, null);
        Map<String, byte[]> map = new HashMap<>();
        Map<String, VersionedMetadata<Integer>> forContext = new HashMap<>();
        segmentsToSeal.forEach(x -> {
            String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, x);
            map.put(key, epochData);
            forContext.put(key, epochVersionedData);
        });

        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.addNewEntriesIfAbsent(metadataTable, map)
                                                         .thenAccept(v -> loadMultipleEntriesToContext(metadataTable, forContext)));
    }

    @Override
    CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId) {
        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(metadataTable -> getAndLoadToContext(metadataTable, key, Integer.class,
                        BYTES_TO_INTEGER_FUNCTION));
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> 
                        Futures.toVoid(addAndLoadToContext(metadataTable, EPOCH_TRANSITION_KEY, epochTransition, EpochTransitionRecord::toBytes, true)));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> epochTransition) {
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, EPOCH_TRANSITION_KEY,
                            epochTransition.getObject(), EpochTransitionRecord::toBytes, epochTransition.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> 
                        getAndLoadToContext(metadataTable, EPOCH_TRANSITION_KEY, EpochTransitionRecord.class,
                                EpochTransitionRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        return getMetadataTable()
                .thenCompose(metadataTable ->
                        Futures.toVoid(addAndLoadToContext(metadataTable, CREATION_TIME_KEY, creationTime, LONG_TO_BYTES_FUNCTION, true)));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(metadataTable, CONFIGURATION_KEY, configuration, StreamConfigurationRecord::toBytes, true)));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final StateRecord state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(metadataTable, STATE_KEY, state, StateRecord::toBytes, true)));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);

        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(metadataTable, key, timestamp, LONG_TO_BYTES_FUNCTION, true)));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, key, data.getObject(), LONG_TO_BYTES_FUNCTION, data.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable().thenCompose(metadataTable ->
                storeHelper.expectingDataNotFound(
                        getAndLoadToContext(metadataTable, key, Long.class, BYTES_TO_LONG_FUNCTION), null));
    }

    @Override
    CompletableFuture<Void> removeMarkerData(long segmentId) {
        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable()
                .thenCompose(id -> removeAndUnload(id, key, null));
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
        // first get the number of ongoing transactions from the cache. 
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
                        ActiveTxnRecord::fromBytes, NON_EXISTENT_TXN)
                             .thenApply(res -> {
                                 List<ActiveTxnRecord> list = new ArrayList<>();
                                 for (int i = 0; i < txnIds.size(); i++) {
                                     VersionedMetadata<ActiveTxnRecord> txn = res.get(i);
                                     list.add(txn.getObject());
                                     if (!txn.equals(NON_EXISTENT_TXN)) {
                                         loadEntryToContext(epochTxnTable, txnIds.get(i), txn);
                                     }
                                 }
                                 return list;
                             }));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnInEpoch(int epoch) {
        Map<String, VersionedMetadata<ActiveTxnRecord>> result = new ConcurrentHashMap<>();
        return getTransactionsInEpochTable(epoch)
            .thenCompose(tableName -> storeHelper.expectingDataNotFound(storeHelper.getAllEntries(
                    tableName, ActiveTxnRecord::fromBytes).collectRemaining(x -> {
                        result.put(x.getKey(), x.getValue());
                        return true;
            }).thenApply(v -> {
                loadMultipleEntriesToContext(tableName, result);
                return result.entrySet().stream().collect(Collectors.toMap(x -> UUID.fromString(x.getKey()), x -> x.getValue().getObject()));
            }), Collections.emptyMap()));
    }

    @Override
    public CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final ActiveTxnRecord txnRecord) {
        // create txn ==>
        // if epoch table exists, add txn to epoch
        //  1. add epochs_with_txn entry for the epoch
        //  2. create txns-in-epoch table
        //  3. create txn in txns-in-epoch
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTable -> addAndLoadToContext(epochTable, txId.toString(), txnRecord, ActiveTxnRecord::toBytes, true));
    }

    private CompletableFuture<Void> createTransactionsInEpochTable(int epoch) {
        return getEpochsWithTransactionsTable()
                .thenCompose(epochsWithTxnTable -> {
                    return storeHelper.addNewEntryIfAbsent(epochsWithTxnTable, Integer.toString(epoch), x -> x, new byte[0]);
                }).thenCompose(epochTxnEntryCreated -> {
                    return getTransactionsInEpochTable(epoch)
                            .thenCompose(storeHelper::createTable);
                });
    }
    
    @Override
    CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(final int epoch, final UUID txId) {
        return getTransactionsInEpochTable(epoch)
                    .thenCompose(epochTxnTable -> getAndLoadToContext(epochTxnTable, txId.toString(), ActiveTxnRecord.class, 
                            ActiveTxnRecord::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final VersionedMetadata<ActiveTxnRecord> data) {
        return getTransactionsInEpochTable(epoch)
                .thenCompose(epochTxnTable -> updateAndLoadToContext(epochTxnTable, txId.toString(), data.getObject(), 
                        ActiveTxnRecord::toBytes, data.getVersion()));
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
                        removeAndUnload(epochTransactionsTableName, txId.toString(), null))
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
                                  Integer.toString(batch), x -> x, new byte[0]))
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
                                          storeHelper.getCachedOrLoad(table, key, CompletedTxnRecord::fromBytes, operationStartTime), null);
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
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(metadataTable,
                        TRUNCATION_KEY, truncationRecord, StreamTruncationRecord::toBytes, true)));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final VersionedMetadata<StreamTruncationRecord> truncationRecord) {
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, TRUNCATION_KEY,
                        truncationRecord.getObject(), StreamTruncationRecord::toBytes, truncationRecord.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unloadFromContext(metadataTable, TRUNCATION_KEY);
                    }

                    return getAndLoadToContext(metadataTable, TRUNCATION_KEY, StreamTruncationRecord.class, 
                            StreamTruncationRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<StreamConfigurationRecord> configuration) {
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, CONFIGURATION_KEY,
                        configuration.getObject(), StreamConfigurationRecord::toBytes, configuration.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unloadFromContext(metadataTable, CONFIGURATION_KEY);
                    }

                    return getAndLoadToContext(metadataTable, CONFIGURATION_KEY, StreamConfigurationRecord.class,
                            StreamConfigurationRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<StateRecord> state) {
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, STATE_KEY,
                        state.getObject(), StateRecord::toBytes, state.getVersion()));
    }

    @Override
    CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached) {
        return getMetadataTable()
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unloadFromContext(metadataTable, STATE_KEY);
                    }

                    return getAndLoadToContext(metadataTable, STATE_KEY, StateRecord.class, StateRecord::fromBytes);
                });
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(addAndLoadToContext(
                        metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, committingTxns, CommittingTransactionsRecord::toBytes, true)));
    }

    @Override
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord() {
        return getMetadataTable()
                .thenCompose(metadataTable -> getAndLoadToContext(metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, 
                        CommittingTransactionsRecord.class, CommittingTransactionsRecord::fromBytes));
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> update) {
        return getMetadataTable()
                .thenCompose(metadataTable -> updateAndLoadToContext(metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY,
                        update.getObject(), CommittingTransactionsRecord::toBytes, update.getVersion()));
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String waitingRequestProcessor) {
        return getMetadataTable()
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(
                        metadataTable, WAITING_REQUEST_PROCESSOR_PATH, x -> x.getBytes(StandardCharsets.UTF_8), waitingRequestProcessor)));
    }

    @Override
    CompletableFuture<String> getWaitingRequestNode() {
        return getMetadataTable()
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, WAITING_REQUEST_PROCESSOR_PATH,
                        x -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(x)).toString(), System.currentTimeMillis()))
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
                .thenCompose(table -> storeHelper.addNewEntry(table, writer, x -> x.toBytes(), mark)));
    }

    @Override
    public CompletableFuture<Void> removeWriterRecord(String writer, Version version) {
        return getWritersTable()
                .thenCompose(table -> storeHelper.removeEntry(table, writer, version));
    }

    @Override
    CompletableFuture<VersionedMetadata<WriterMark>> getWriterMarkRecord(String writer) {
        return getWritersTable()
                .thenCompose(table -> storeHelper.getCachedOrLoad(table, writer, WriterMark::fromBytes, operationStartTime));
    }

    @Override
    CompletableFuture<Void> updateWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position, 
                                                   boolean isAlive, Version version) {
        WriterMark mark = new WriterMark(timestamp, position, isAlive);
        return Futures.toVoid(getWritersTable()
                .thenCompose(table -> storeHelper.updateEntry(table, writer, x -> x.toBytes(), mark, version)));
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
    // endregion

    private String getKeyForSubscriber(final String subscriber) {
            return SUBSCRIBER_KEY_PREFIX + subscriber;
    }

    private <X> CompletableFuture<VersionedMetadata<X>> getAndLoadToContext(String tableName, String key, Class<X> tClass,
                                                                            Function<byte[], X> fromBytes) {
        if (context != null) {
            VersionedMetadata<X> value = context.get(tableName, key, tClass);
            
            return value != null ? CompletableFuture.completedFuture(value) :
                    storeHelper.getCachedOrLoad(tableName, key, fromBytes, operationStartTime)
                          .thenApply(fetched -> {
                              context.load(tableName, key, fetched);
                              return fetched;
                          })
                          .exceptionally(e -> {
                              if (DATA_NOT_FOUND_PREDICATE.test(e)) {
                                  unloadFromContext(tableName, key);
                              }
                              throw new CompletionException(e);
                          });
        } else {
            return storeHelper.getCachedOrLoad(tableName, key, fromBytes, operationStartTime);
        }
    }

    private void unloadFromContext(String tableName, String key) {
        if (context != null) {
            context.unload(tableName, key);
        }
        storeHelper.invalidateCache(tableName, key);
    }

    private CompletableFuture<Void> removeAndUnload(String tableName, String key, Version version) {
        CompletableFuture<Void> future = version == null ? storeHelper.removeEntry(tableName, key) :
                storeHelper.removeEntry(tableName, key, version);
        return future.thenApply(v -> {
                       if (context != null) {
                           context.unload(tableName, key);
                       }
                       return v;
                   });
    }

    private <T> CompletableFuture<Version> updateAndLoadToContext(String tableName, String key, T value, Function<T, byte[]> toBytes, Version version) {
        return storeHelper.updateEntry(tableName, key, toBytes, value, version)
                          .thenApply(v -> {
                              if (context != null) {
                                  context.load(tableName, key, new VersionedMetadata<>(value, v));
                              }
                              return v;
                          })
                          .exceptionally(e -> {
                              if (WRITE_CONFLICT_PREDICATE.test(e)) {
                                  unloadFromContext(tableName, key);
                                  storeHelper.invalidateCache(tableName, key);
                              }
                              throw new CompletionException(e);
                          });
    }

    private <T> CompletableFuture<Version> addAndLoadToContext(String tableName, String key, T value, Function<T, byte[]> toBytes, 
                                                               boolean addIfAbsent) {
        CompletableFuture<Version> future = addIfAbsent ? 
                storeHelper.addNewEntryIfAbsent(tableName, key, toBytes, value) :
                storeHelper.addNewEntry(tableName, key, toBytes, value);
        
        return future.thenApply(v -> {
                       if (context != null) {
                           // If version is null then we were unable to add the entry as it already existed.
                           // In that case we will not load it in the context. 
                           if (v != null) {
                               context.load(tableName, key, new VersionedMetadata<>(value, v));
                           } 
                       }
                       if (v == null) {
                           storeHelper.invalidateCache(tableName, key);
                       }
                       return v;
                   });
    }
    
    private <T> void loadEntryToContext(String tableName, String key, VersionedMetadata<T> value) {
        if (context != null) {
            context.load(tableName, key, value);
        }
    }

    private <T> void loadMultipleEntriesToContext(String tableName, Map<String, VersionedMetadata<T>> updates) {
        if (context != null) {
            updates.forEach((x, y) -> context.load(tableName, x, y));
        }
    }

    private void clearContext() {
        if (context != null) {
            context.unloadAll();
        }
    }

    @Data
    class Context {
        @GuardedBy("$lock")
        private final Map<String, VersionedMetadata<?>> map = new HashMap<>();

        @Synchronized
        <T> void load(String tableName, String key, VersionedMetadata<T> value) {
            if (value != null) {
                map.put(tableName + key, value);
            }
        }

        @SuppressWarnings("unchecked")
        @Synchronized
        <T> VersionedMetadata<T> get(String tableName, String key, Class<T> tClass) {
            VersionedMetadata<?> versionedMetadata = map.get(tableName + key);
            assert versionedMetadata == null || versionedMetadata.getObject().getClass().isAssignableFrom(tClass);
            return (VersionedMetadata<T>) versionedMetadata;
        }

        @Synchronized
        void unload(String tableName, String key) {
            map.remove(tableName + key);
        }

        @Synchronized
        void unloadAll() {
            map.clear();
        }
    }
}
