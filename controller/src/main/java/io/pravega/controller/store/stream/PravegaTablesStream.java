/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.tracing.TagLogger;
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
import io.pravega.controller.util.Config;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
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
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_UUID_FUNCTION;
import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_LONG_FUNCTION;
import static io.pravega.controller.store.PravegaTablesStoreHelper.LONG_TO_BYTES_FUNCTION;
import static io.pravega.controller.store.PravegaTablesStoreHelper.INTEGER_TO_BYTES_FUNCTION;
import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_INTEGER_FUNCTION;
import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_EMPTY_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.shared.NameUtils.COMMITTING_TRANSACTIONS_RECORD_KEY;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCHES_TABLE;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.CONFIGURATION_KEY;
import static io.pravega.shared.NameUtils.CREATION_TIME_KEY;
import static io.pravega.shared.NameUtils.CURRENT_EPOCH_KEY;
import static io.pravega.shared.NameUtils.EPOCHS_WITH_TRANSACTIONS_TABLE;
import static io.pravega.shared.NameUtils.EPOCH_RECORD_KEY_FORMAT;
import static io.pravega.shared.NameUtils.EPOCH_TRANSITION_KEY;
import static io.pravega.shared.NameUtils.HISTORY_TIMESERIES_CHUNK_FORMAT;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.METADATA_TABLE;
import static io.pravega.shared.NameUtils.RETENTION_SET_KEY;
import static io.pravega.shared.NameUtils.RETENTION_STREAM_CUT_RECORD_KEY_FORMAT;
import static io.pravega.shared.NameUtils.SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT;
import static io.pravega.shared.NameUtils.SEGMENT_MARKER_PATH_FORMAT;
import static io.pravega.shared.NameUtils.SEGMENT_SEALED_EPOCH_KEY_FORMAT;
import static io.pravega.shared.NameUtils.SEPARATOR;
import static io.pravega.shared.NameUtils.STATE_KEY;
import static io.pravega.shared.NameUtils.SUBSCRIBER_KEY_PREFIX;
import static io.pravega.shared.NameUtils.SUBSCRIBER_SET_KEY;
import static io.pravega.shared.NameUtils.TRANSACTIONS_IN_EPOCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.TRUNCATION_KEY;
import static io.pravega.shared.NameUtils.WAITING_REQUEST_PROCESSOR_PATH;
import static io.pravega.shared.NameUtils.WRITERS_POSITIONS_TABLE;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega Table Stream.
 * This creates two top level tables per stream - metadataTable, epochsWithTransactionsTable.
 * All metadata records are stored in metadata table.
 * EpochsWithTransactions is a top level table for storing epochs where any transaction was created.
 * This class is coded for transaction ids that follow the scheme that msb 32 bits represent epoch.
 * Each stream table is protected against recreation of stream by attaching a unique id to the stream when it is created.
 */
class PravegaTablesStream extends PersistentStreamBase {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesStream.class));

    // completed transactions key
    private static final String STREAM_KEY_PREFIX = "Key" + SEPARATOR + "%s" + SEPARATOR + "%s" + SEPARATOR; // scoped stream name
    private static final String COMPLETED_TRANSACTIONS_KEY_FORMAT = STREAM_KEY_PREFIX + "/%s";

    // non existent records
    private static final VersionedMetadata<ActiveTxnRecord> NON_EXISTENT_TXN = 
            new VersionedMetadata<>(ActiveTxnRecord.EMPTY, new Version.LongVersion(Long.MIN_VALUE));

    private final PravegaTablesStoreHelper storeHelper;

    private final Supplier<Integer> currentBatchSupplier;
    private final BiFunction<Boolean, OperationContext, CompletableFuture<String>> streamsInScopeTableNameSupplier;
    private final AtomicReference<String> idRef;
    private final ZkOrderedStore txnCommitOrderer;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, 
                        ZkOrderedStore txnCommitOrderer,
                        Supplier<Integer> currentBatchSupplier, 
                        BiFunction<Boolean, OperationContext, CompletableFuture<String>> streamsInScopeTableNameSupplier,
                        ScheduledExecutorService executor) {
        this(scopeName, streamName, storeHelper, txnCommitOrderer, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE,
                SealedSegmentsMapShard.SHARD_SIZE, streamsInScopeTableNameSupplier, executor);
    }

    @VisibleForTesting
    PravegaTablesStream(final String scopeName, final String streamName, PravegaTablesStoreHelper storeHelper, 
                        ZkOrderedStore txnCommitOrderer,
                        Supplier<Integer> currentBatchSupplier, int chunkSize, int shardSize,
                        BiFunction<Boolean, OperationContext, CompletableFuture<String>> streamsInScopeTableNameSupplier, 
                        ScheduledExecutorService executor) {
        super(scopeName, streamName, chunkSize, shardSize);
        this.storeHelper = storeHelper;
        this.txnCommitOrderer = txnCommitOrderer;
        this.currentBatchSupplier = currentBatchSupplier;
        this.streamsInScopeTableNameSupplier = streamsInScopeTableNameSupplier;
        this.idRef = new AtomicReference<>(null);
        this.executor = executor;
    }

    private CompletableFuture<String> getId(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String id = idRef.get();
        
        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            // first get the scope id from the cache.
            // if the cache does not contain scope id then we load it from the supplier. 
            // if cache contains the scope id then we load the streamid. if not found, we start with the scope id
            return storeHelper.loadFromTableHandleStaleTableName(streamsInScopeTableNameSupplier, getName(),
                    BYTES_TO_UUID_FUNCTION, context)
                          .thenComposeAsync(data -> {
                idRef.compareAndSet(null, data.getObject().toString());
                return getId(context);
            });
        }
    }

    private CompletableFuture<String> getMetadataTable(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getId(context).thenApply(this::getMetadataTableName);
    }

    private String getMetadataTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(METADATA_TABLE, id));
    }

    private CompletableFuture<String> getEpochsWithTransactionsTable(OperationContext context) {
        return getId(context).thenApply(this::getEpochsWithTransactionsTableName);
    }

    private String getEpochsWithTransactionsTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, id));
    }

    private CompletableFuture<String> getTransactionsInEpochTable(int epoch, OperationContext context) {
        return getId(context).thenApply(id -> getTransactionsInEpochTableName(epoch, id));
    }

    private String getTransactionsInEpochTableName(int epoch, String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), 
                String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, epoch, id));
    }

    private CompletableFuture<String> getWritersTable(OperationContext context) {
        return getId(context).thenApply(this::getWritersTableName);
    }

    private String getWritersTableName(String id) {
        return getQualifiedTableName(INTERNAL_SCOPE_NAME, getScope(), getName(), String.format(WRITERS_POSITIONS_TABLE, id));
    }
    // region overrides

    @Override
    public CompletableFuture<Void> completeCommittingTransactions(VersionedMetadata<CommittingTransactionsRecord> record,
                                                                  OperationContext context,
                                                                  Map<String, TxnWriterMark> writerMarks) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        // create all transaction entries in committing txn list.
        // remove all entries from active txn in epoch.
        // reset CommittingTxnRecord
        long time = System.currentTimeMillis();

        List<Map.Entry<String, CompletedTxnRecord>> completedRecords = new ArrayList<>(record.getObject()
                                                                                             .getTransactionsToCommit().size());
        List<String> txnIdStrings = new ArrayList<>(record.getObject().getTransactionsToCommit().size());
        
        record.getObject().getTransactionsToCommit().forEach(x -> {
            completedRecords.add(new AbstractMap.SimpleEntry<>(
                    getCompletedTransactionKey(getScope(), getName(), x.toString()),
                    new CompletedTxnRecord(time, TxnStatus.COMMITTED)));
            txnIdStrings.add(x.toString());
        });
        CompletableFuture<Void> future;
        if (record.getObject().getTransactionsToCommit().size() == 0) {
            future = CompletableFuture.completedFuture(null);
        } else {
            future = generateMarksForTransactions(context, writerMarks)
                .thenCompose(v -> createCompletedTxEntries(completedRecords, context))
                    .thenCompose(x -> getTransactionsInEpochTable(record.getObject().getEpoch(), context)
                            .thenCompose(table -> storeHelper.removeEntries(table, txnIdStrings, context.getRequestId())))
                    .thenCompose(x -> tryRemoveOlderTransactionsInEpochTables(epoch -> epoch < record.getObject().getEpoch(), 
                            context));
        }
        return future
                .thenCompose(x -> Futures.toVoid(updateCommittingTxnRecord(new VersionedMetadata<>(
                        CommittingTransactionsRecord.EMPTY,
                        record.getVersion()), context)));
    }

    @Override
    CompletableFuture<Void> createStreamMetadata(OperationContext context) {
        return getId(context).thenCompose(id -> {
            String metadataTable = getMetadataTableName(id);
            String epochWithTxnTable = getEpochsWithTransactionsTableName(id);
            String writersPositionsTable = getWritersTableName(id);
            return CompletableFuture.allOf(storeHelper.createTable(metadataTable, context.getRequestId()),
                    storeHelper.createTable(epochWithTxnTable, context.getRequestId()), 
                    storeHelper.createTable(writersPositionsTable, 
                            context.getRequestId()))
                                    .thenAccept(v -> log.debug(context.getRequestId(), 
                                            "stream {}/{} metadata tables {}, {} & {} created", getScope(),
                                            getName(), metadataTable,
                                            epochWithTxnTable, writersPositionsTable));
        });
    }

    @Override
    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, 
                                                                     final long creationTime,
                                                                     final int startingSegmentNumber, 
                                                                     final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier,
        return storeHelper.expectingDataNotFound(getCreationTime(context), null)
                      .thenCompose(storedCreationTime -> {
                          if (storedCreationTime == null) {
                              return CompletableFuture.completedFuture(new CreateStreamResponse(
                                      CreateStreamResponse.CreateStatus.NEW,
                                      configuration, creationTime, startingSegmentNumber));
                          } else {
                              return storeHelper.expectingDataNotFound(getConfiguration(context), null)
                                            .thenCompose(config -> {
                                                if (config != null) {
                                                    return handleConfigExists(storedCreationTime, config, startingSegmentNumber,
                                                            storedCreationTime == creationTime, context);
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
                                                                       int startingSegmentNumber, boolean creationTimeMatched,
                                                                       OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        CreateStreamResponse.CreateStatus status = creationTimeMatched ?
                CreateStreamResponse.CreateStatus.NEW : CreateStreamResponse.CreateStatus.EXISTS_CREATING;
        return storeHelper.expectingDataNotFound(getState(true, context), null)
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
    public CompletableFuture<Long> getCreationTime(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable ->
                                storeHelper.getCachedOrLoad(metadataTable, CREATION_TIME_KEY, BYTES_TO_LONG_FUNCTION, 
                                        0L, context.getRequestId()))
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> addSubscriber(String newSubscriber, long newGeneration, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return createSubscribersRecordIfAbsent(context)
                   .thenCompose(y -> getSubscriberSetRecord(true, context))
                   .thenCompose(subscriberSetRecord -> {
                      if (!subscriberSetRecord.getObject().getSubscribers().contains(newSubscriber)) {
                          // update Subscriber generation, if it is greater than current generation
                          return getMetadataTable(context)
                                  .thenCompose(metaTable -> {
                                      Subscribers newSubscribers = Subscribers.add(subscriberSetRecord.getObject(), newSubscriber);
                                      return storeHelper.updateEntry(metaTable, SUBSCRIBER_SET_KEY, newSubscribers,
                                              Subscribers::toBytes, subscriberSetRecord.getVersion(), context.getRequestId());
                                  });
                      }
                      return CompletableFuture.completedFuture(null);
                   })
                    .thenCompose(v -> Futures.exceptionallyExpecting(getSubscriberRecord(newSubscriber, context),
                            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                            .thenCompose(subscriberRecord -> {
                                if (subscriberRecord == null) {
                                    final StreamSubscriber newSubscriberRecord = new StreamSubscriber(newSubscriber,
                                            newGeneration, ImmutableMap.of(), System.currentTimeMillis());
                                    return Futures.toVoid(getMetadataTable(context).thenApply(metadataTable -> 
                                            storeHelper.addNewEntryIfAbsent(metadataTable, getKeyForSubscriber(newSubscriber),
                                                    newSubscriberRecord, StreamSubscriber::toBytes, context.getRequestId())));
                                } else {
                                    // just update the generation if subscriber already exists...
                                    if (subscriberRecord.getObject().getGeneration() < newGeneration) {
                                      return Futures.toVoid(setSubscriberData(new VersionedMetadata<>(
                                              new StreamSubscriber(newSubscriber, newGeneration,
                                              subscriberRecord.getObject().getTruncationStreamCut(), System.currentTimeMillis()),
                                              subscriberRecord.getVersion()), context));
                                    }
                                }
                                return CompletableFuture.completedFuture(null);
                            })
                    );
    }

    @Override
    public CompletableFuture<Void> createSubscribersRecordIfAbsent(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return Futures.exceptionallyExpecting(getSubscriberSetRecord(true, context),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                .thenCompose(subscriberSetRecord -> {
                    if (subscriberSetRecord == null) {
                        return Futures.toVoid(getMetadataTable(context)
                                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, SUBSCRIBER_SET_KEY, 
                                        Subscribers.EMPTY_SET, Subscribers::toBytes, context.getRequestId())));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<VersionedMetadata<Subscribers>> getSubscriberSetRecord(final boolean ignoreCached, 
                                                                                    final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(table -> {
                    if (ignoreCached) {
                        unload(table, SUBSCRIBER_SET_KEY);
                    }
                    return storeHelper.getCachedOrLoad(table, SUBSCRIBER_SET_KEY, Subscribers::fromBytes, 
                            context.getOperationStartTime(), context.getRequestId());
                });
    }

    @Override
    CompletableFuture<Version> setSubscriberData(final VersionedMetadata<StreamSubscriber> streamSubscriber, 
                                                 final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, 
                        getKeyForSubscriber(streamSubscriber.getObject().getSubscriber()),
                        streamSubscriber.getObject(), StreamSubscriber::toBytes, streamSubscriber.getVersion(), 
                        context.getRequestId()));
    }

    @Override
    public CompletableFuture<Void> deleteSubscriber(final String subscriber, final long generation, 
                                                    final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getSubscriberRecord(subscriber, context).thenCompose(subs -> {
            if (generation < subs.getObject().getGeneration()) {
                log.warn(context.getRequestId(), "skipped deleting subscriber {} due to generation mismatch", subscriber);
                return CompletableFuture.completedFuture(null);
            }
            return getMetadataTable(context).thenCompose(table -> storeHelper.removeEntry(table, getKeyForSubscriber(subscriber), 
                    context.getRequestId())
                    .thenAccept(x -> storeHelper.invalidateCache(table, getKeyForSubscriber(subscriber)))
                    .thenCompose(v -> getSubscriberSetRecord(true, context)
                            .thenCompose(subscriberSetRecord -> {
                                if (subscriberSetRecord.getObject().getSubscribers().contains(subscriber)) {
                                    Subscribers subSet = Subscribers.remove(subscriberSetRecord.getObject(), subscriber);
                                    return Futures.toVoid(storeHelper.updateEntry(table, SUBSCRIBER_SET_KEY, subSet, 
                                            Subscribers::toBytes, subscriberSetRecord.getVersion(), context.getRequestId()));
                                }
                                return CompletableFuture.completedFuture(null);
                            })));
        });
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriberRecord(final String subscriber,
                                                                                      final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(table -> storeHelper.getCachedOrLoad(table, getKeyForSubscriber(subscriber), 
                        StreamSubscriber::fromBytes,
                        context.getOperationStartTime(), context.getRequestId()));
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers(final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(table -> getSubscriberSetRecord(true, context)
                        .thenApply(subscribersSet -> subscribersSet.getObject().getSubscribers().asList()));
    }

    @Override
    public CompletableFuture<Void> deleteStream(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        // delete all tables for this stream even if they are not empty!
        // 1. read epoch table
        // delete all epoch txn specific tables
        // delete epoch txn base table
        // delete metadata table

        // delete stream in scope
        return getId(context)
                .thenCompose(id -> storeHelper.expectingDataNotFound(tryRemoveOlderTransactionsInEpochTables(epoch -> true, context), null)
                        .thenCompose(v -> getEpochsWithTransactionsTable(context)
                                .thenCompose(epochWithTxnTable -> storeHelper.expectingDataNotFound(
                                        storeHelper.deleteTable(epochWithTxnTable, false, context.getRequestId()), 
                                        null))
                                .thenCompose(deleted -> storeHelper.deleteTable(getMetadataTableName(id), false,
                                        context.getRequestId()))));
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet data, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, RETENTION_SET_KEY,
                        data, RetentionSet::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, RETENTION_SET_KEY, 
                        RetentionSet::fromBytes,
                        context.getOperationStartTime(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> retention, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> 
                        storeHelper.updateEntry(metadataTable, RETENTION_SET_KEY, retention.getObject(),
                                RetentionSet::toBytes, retention.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, StreamCutRecord record, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, key, record, 
                        StreamCutRecord::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, key, 
                        StreamCutRecord::fromBytes, 0L, context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, recordingTime);

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.removeEntry(metadataTable, key, context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data, 
                                                                     OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> 
                        Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, key, data, HistoryTimeSeries::toBytes,
                                context.getRequestId())));
    }

    @Override
    CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached,
                                                                                          OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unload(metadataTable, key);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, key, HistoryTimeSeries::fromBytes,
                            context.getOperationStartTime(), context.getRequestId());
                });
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, VersionedMetadata<HistoryTimeSeries> data,
                                                                OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, chunkNumber);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, key, data.getObject(), 
                        HistoryTimeSeries::toBytes, data.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    return Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, CURRENT_EPOCH_KEY, 
                            data.getEpoch(), INTEGER_TO_BYTES_FUNCTION, context.getRequestId()));
                });
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> data, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, CURRENT_EPOCH_KEY, data.getObject().getEpoch(), 
                        INTEGER_TO_BYTES_FUNCTION, data.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unload(metadataTable, CURRENT_EPOCH_KEY);
                    }
                    return storeHelper.getCachedOrLoad(metadataTable, CURRENT_EPOCH_KEY, BYTES_TO_INTEGER_FUNCTION, 
                            context.getOperationStartTime(), context.getRequestId())
                            .thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject(), context)
                                              .thenApply(epochRecord -> 
                                                      new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
                });
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.addNewEntryIfAbsent(metadataTable, key, data, EpochRecord::toBytes,
                        context.getRequestId()))
                .thenCompose(v -> {
                    if (data.getEpoch() == data.getReferenceEpoch()) {
                        // this is an original epoch. we should create transactions in epoch table
                        return createTransactionsInEpochTable(epoch, context);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    String key = String.format(EPOCH_RECORD_KEY_FORMAT, epoch);
                    return storeHelper.getCachedOrLoad(metadataTable, key, EpochRecord::fromBytes, 0L, 
                            context.getRequestId());
                });
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, SealedSegmentsMapShard data, 
                                                                         OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, key, data,
                        SealedSegmentsMapShard::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(int shard, 
                                                                                                   OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, key, 
                         SealedSegmentsMapShard::fromBytes, context.getOperationStartTime(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, VersionedMetadata<SealedSegmentsMapShard> data, 
                                                                    OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, shard);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, key, data.getObject(),
                        SealedSegmentsMapShard::toBytes, data.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecords(Collection<Long> segmentsToSeal, int epoch, 
                                                            OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        List<Map.Entry<String, Integer>> values = new ArrayList<>(segmentsToSeal.size());
        segmentsToSeal.forEach(x -> {
            String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, x);
            values.add(new AbstractMap.SimpleEntry<>(key, epoch));
        });

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.addNewEntriesIfAbsent(metadataTable, values, INTEGER_TO_BYTES_FUNCTION, 
                        context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        String key = String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, segmentId);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, key, BYTES_TO_INTEGER_FUNCTION, 
                        0L, context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransition, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> 
                        Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, EPOCH_TRANSITION_KEY, epochTransition, 
                                EpochTransitionRecord::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> epochTransition, 
                                                         OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, EPOCH_TRANSITION_KEY,
                            epochTransition.getObject(), EpochTransitionRecord::toBytes, epochTransition.getVersion(), 
                        context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> 
                        storeHelper.getCachedOrLoad(metadataTable, EPOCH_TRANSITION_KEY, EpochTransitionRecord::fromBytes,
                                context.getOperationStartTime(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable ->
                        Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, CREATION_TIME_KEY, creationTime, 
                                LONG_TO_BYTES_FUNCTION, context.getRequestId())));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord configuration, 
                                                               OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, CONFIGURATION_KEY, 
                        configuration, StreamConfigurationRecord::toBytes, context.getRequestId())));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final StateRecord state, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, STATE_KEY, 
                        state, StateRecord::toBytes, context.getRequestId())));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);

        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable, key, timestamp, 
                        LONG_TO_BYTES_FUNCTION, context.getRequestId())));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, key, data.getObject(), 
                        LONG_TO_BYTES_FUNCTION, data.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable(context).thenCompose(metadataTable ->
                storeHelper.expectingDataNotFound(
                        storeHelper.getCachedOrLoad(metadataTable, key, BYTES_TO_LONG_FUNCTION, 
                                context.getOperationStartTime(), context.getRequestId()), null));
    }

    @Override
    CompletableFuture<Void> removeMarkerData(long segmentId, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        final String key = String.format(SEGMENT_MARKER_PATH_FORMAT, segmentId);
        return getMetadataTable(context)
                .thenCompose(id -> storeHelper.removeEntry(id, key, context.getRequestId()));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getEpochsWithTransactions(context)
                .thenCompose(epochsWithTransactions -> Futures.allOfWithResults(epochsWithTransactions.stream().map(x ->
                        getTxnInEpoch(x, context)).collect(Collectors.toList()))).thenApply(list -> {
            Map<UUID, ActiveTxnRecord> map = new HashMap<>();
            list.forEach(map::putAll);
            return map;
        });
    }

    @Override
    public CompletableFuture<Map<UUID, TxnStatus>> listCompletedTxns(final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        return getTxnInBatch(0, context);
    }

    private CompletableFuture<Map<UUID, TxnStatus>> getTxnInBatch(final Integer batch, final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        Map<String, VersionedMetadata<CompletedTxnRecord>> result = new ConcurrentHashMap<>();
        String tableName = getCompletedTransactionsBatchTableName(batch);
        return storeHelper.expectingDataNotFound(storeHelper.getAllEntries(
                tableName, CompletedTxnRecord::fromBytes, context.getRequestId()).collectRemaining(completedTxnMetadataMap -> {
            result.put(completedTxnMetadataMap.getKey().replace(getCompletedTransactionKey(getScope(), getName(), ""), ""),
                    completedTxnMetadataMap.getValue());
            return true;
        }).thenApply(v -> result.entrySet().stream().sorted((r1, r2) -> Long.compare(r2.getValue().getObject().getCompleteTime(),
                                r1.getValue().getObject().getCompleteTime())).limit( Config.LIST_COMPLETED_TXN_MAX_RECORDS )
                        .collect(Collectors.toMap(completedTxnMap -> UUID.fromString(completedTxnMap.getKey()),
                completedTxnMap -> completedTxnMap.getValue().getObject().getCompletionStatus())))
                .exceptionally(v -> Collections.emptyMap()), Collections.emptyMap());
    }

    private CompletableFuture<List<Integer>> getEpochsWithTransactions(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getEpochsWithTransactionsTable(context)
                .thenCompose(epochWithTxnTable -> {
                    List<Integer> epochsWithTransactions = new ArrayList<>();
                    return storeHelper.getAllKeys(epochWithTxnTable, context.getRequestId())
                               .collectRemaining(x -> {
                                   epochsWithTransactions.add(Integer.parseInt(x));
                                   return true;
                               }).thenApply(v -> epochsWithTransactions);
                });
    }

    @Override
    public CompletableFuture<Long> getNumberOfOngoingTransactions(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        List<CompletableFuture<Long>> futures = new ArrayList<>();
        // first get the number of ongoing transactions from the cache. 
        return getEpochsWithTransactionsTable(context)
                .thenCompose(epochsWithTxn -> storeHelper.getAllKeys(epochsWithTxn, context.getRequestId())
                                                         .forEachRemaining(x -> {
                                                             futures.add(getNumberOfOngoingTransactions(
                                                                     Integer.parseInt(x), context));
                                                         }, executor)
                                                         .thenCompose(v -> Futures.allOfWithResults(futures)
                                                                                  .thenApply(list -> list
                                                                                          .stream()
                                                                                          .reduce(0L, Long::sum))));
    }

    private CompletableFuture<Long> getNumberOfOngoingTransactions(int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        AtomicInteger count = new AtomicInteger(0);
        return getTransactionsInEpochTable(epoch, context)
                .thenCompose(epochTableName -> storeHelper.getEntryCount(epochTableName, context.getRequestId()));
    }

    @Override
    public CompletableFuture<List<VersionedTransactionData>> getOrderedCommittingTxnInLowestEpoch(
            int limit, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return super.getOrderedCommittingTxnInLowestEpochHelper(txnCommitOrderer, limit, executor, context);
    }

    @Override
    @VisibleForTesting
    CompletableFuture<Map<Long, UUID>> getAllOrderedCommittingTxns(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return super.getAllOrderedCommittingTxnsHelper(txnCommitOrderer, context);
    }

    @Override
    CompletableFuture<List<ActiveTxnRecord>> getTransactionRecords(int epoch, List<String> txnIds, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getTransactionsInEpochTable(epoch, context)
                .thenCompose(epochTxnTable -> storeHelper.getEntries(epochTxnTable, txnIds,
                        ActiveTxnRecord::fromBytes, NON_EXISTENT_TXN, context.getRequestId())
                             .thenApply(res -> {
                                 List<ActiveTxnRecord> list = new ArrayList<>();
                                 for (int i = 0; i < txnIds.size(); i++) {
                                     VersionedMetadata<ActiveTxnRecord> txn = res.get(i);
                                     list.add(txn.getObject());
                                 }
                                 return list;
                             }));
    }

    @Override
    CompletableFuture<List<VersionedTransactionData>> getVersionedTransactionRecords(int epoch, List<String> txnIds, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getTransactionsInEpochTable(epoch, context)
                .thenCompose(epochTxnTable -> storeHelper.getEntries(epochTxnTable, txnIds,
                        ActiveTxnRecord::fromBytes, NON_EXISTENT_TXN, context.getRequestId())
                             .thenApply(res -> {
                                 List<VersionedTransactionData> list = new ArrayList<>();
                                 for (int i = 0; i < txnIds.size(); i++) {
                                     VersionedMetadata<ActiveTxnRecord> txn = res.get(i);
                                     ActiveTxnRecord activeTxnRecord = txn.getObject();
                                     if (!ActiveTxnRecord.EMPTY.equals(activeTxnRecord)) {
                                         VersionedTransactionData vdata = new VersionedTransactionData(epoch, UUID.fromString(txnIds.get(i)), txn.getVersion(),
                                                 activeTxnRecord.getTxnStatus(), activeTxnRecord.getTxCreationTimestamp(),
                                                 activeTxnRecord.getMaxExecutionExpiryTime(), activeTxnRecord.getWriterId(),
                                                 activeTxnRecord.getCommitTime(), activeTxnRecord.getCommitOrder(),
                                                 activeTxnRecord.getCommitOffsets());
                                         list.add(vdata);
                                     }
                                 }
                                 return list;
                             }));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnInEpoch(int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        Map<String, VersionedMetadata<ActiveTxnRecord>> result = new ConcurrentHashMap<>();
        return getTransactionsInEpochTable(epoch, context)
            .thenCompose(tableName -> storeHelper.expectingDataNotFound(storeHelper.getAllEntries(
                    tableName, ActiveTxnRecord::fromBytes, context.getRequestId()).collectRemaining(x -> {
                        result.put(x.getKey(), x.getValue());
                        return true;
            }).thenApply(v -> result.entrySet().stream().collect(Collectors.toMap(x -> UUID.fromString(x.getKey()),
                    x -> x.getValue().getObject()))), Collections.emptyMap()));
    }

    @Override
    public CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final ActiveTxnRecord txnRecord, 
                                                           OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        // create txn ==>
        // if epoch table exists, add txn to epoch
        //  1. add epochs_with_txn entry for the epoch
        //  2. create txns-in-epoch table
        //  3. create txn in txns-in-epoch
        return getTransactionsInEpochTable(epoch, context)
                .thenCompose(epochTable -> storeHelper.addNewEntryIfAbsent(epochTable, txId.toString(), txnRecord, 
                        ActiveTxnRecord::toBytes, context.getRequestId()));
    }

    private CompletableFuture<Void> createTransactionsInEpochTable(int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getEpochsWithTransactionsTable(context)
                .thenCompose(epochsWithTxnTable -> storeHelper.addNewEntryIfAbsent(epochsWithTxnTable, Integer.toString(epoch), new byte[0], x -> x,
                        context.getRequestId())).thenCompose(epochTxnEntryCreated -> getTransactionsInEpochTable(epoch, context)
                        .thenCompose(x -> storeHelper.createTable(x, context.getRequestId())));
    }
    
    @Override
    CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(final int epoch, final UUID txId, 
                                                                      final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getTransactionsInEpochTable(epoch, context)
                    .thenCompose(epochTxnTable -> storeHelper.getCachedOrLoad(epochTxnTable, txId.toString(),  
                            ActiveTxnRecord::fromBytes, context.getOperationStartTime(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final VersionedMetadata<ActiveTxnRecord> data, 
                                              OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getTransactionsInEpochTable(epoch, context)
                .thenCompose(epochTxnTable -> storeHelper.updateEntry(epochTxnTable, txId.toString(), data.getObject(), 
                        ActiveTxnRecord::toBytes, data.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Long> addTxnToCommitOrder(UUID txId, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return txnCommitOrderer.addEntity(getScope(), getName(), txId.toString(), context.getRequestId());
    }

    @Override
    CompletableFuture<Void> removeTxnsFromCommitOrder(List<Long> orderedPositions, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        return txnCommitOrderer.removeEntities(getScope(), getName(), orderedPositions);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        // 1. remove txn from txn-in-epoch table
        // 2. get current epoch --> if txn-epoch < activeEpoch.reference epoch, try deleting empty epoch table.
        return getTransactionsInEpochTable(epoch, context)
                .thenCompose(epochTransactionsTableName ->
                        storeHelper.removeEntry(epochTransactionsTableName, txId.toString(), context.getRequestId()))
                // this is only best case attempt. If the epoch table is not empty, it will be ignored.
                // if we fail to do this after having removed the transaction, in retried attempt
                // the caller may not find the transaction and never attempt to remove this table.
                // this can lead to proliferation of tables.
                // But remove transaction entry is called from .
                .thenCompose(v -> tryRemoveOlderTransactionsInEpochTables(e -> e < epoch, context));
    }

    private CompletableFuture<Void> tryRemoveOlderTransactionsInEpochTables(Predicate<Integer> epochPredicate, 
                                                                            OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        return getEpochsWithTransactions(context)
                .thenCompose(list -> Futures.allOf(list.stream().filter(epochPredicate)
                                         .map(x -> tryRemoveTransactionsInEpochTable(x, context))
                                         .collect(Collectors.toList())));
    }

    private CompletableFuture<Void> tryRemoveTransactionsInEpochTable(int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getTransactionsInEpochTable(epoch, context)
                .thenCompose(epochTable ->
                        storeHelper.deleteTable(epochTable, true, context.getRequestId())
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
                              return getEpochsWithTransactionsTable(context)
                                .thenCompose(table -> storeHelper.removeEntry(table, Integer.toString(epoch), 
                                        context.getRequestId()));
                          } else {
                              return CompletableFuture.completedFuture(null);
                          }
                      }));
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final CompletedTxnRecord complete, 
                                                   OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return createCompletedTxEntries(Collections.singletonList(new AbstractMap.SimpleEntry<>(
                getCompletedTransactionKey(getScope(), getName(), txId.toString()), complete)), context);
    }

    private CompletableFuture<Void> createCompletedTxEntries(List<Map.Entry<String, CompletedTxnRecord>> complete, 
                                                             OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        Integer batch = currentBatchSupplier.get();
        String tableName = getCompletedTransactionsBatchTableName(batch);
        
        return Futures.toVoid(Futures.exceptionallyComposeExpecting(
                storeHelper.addNewEntriesIfAbsent(tableName, complete, CompletedTxnRecord::toBytes, context.getRequestId()),
                DATA_NOT_FOUND_PREDICATE, () -> tryCreateBatchTable(batch, context)
                        .thenCompose(v -> storeHelper.addNewEntriesIfAbsent(tableName, complete, CompletedTxnRecord::toBytes, 
                                context.getRequestId()))))
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


    private CompletableFuture<Void> tryCreateBatchTable(int batch, OperationContext context) {
        String batchTable = getCompletedTransactionsBatchTableName(batch);

        return Futures.exceptionallyComposeExpecting(storeHelper.addNewEntryIfAbsent(COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                Integer.toString(batch), new byte[0], x -> x, context.getRequestId()), 
                e -> Exceptions.unwrap(e) instanceof StoreException.DataContainerNotFoundException, 
                () -> storeHelper.createTable(COMPLETED_TRANSACTIONS_BATCHES_TABLE, context.getRequestId())
                                 .thenAccept(v -> log.debug(context.getRequestId(), 
                                         "batches root table {} created", COMPLETED_TRANSACTIONS_BATCHES_TABLE))
                                 .thenCompose(v -> storeHelper.addNewEntryIfAbsent(COMPLETED_TRANSACTIONS_BATCHES_TABLE,
                                         Integer.toString(batch), new byte[0], x -> x, context.getRequestId())))
                                 .thenCompose(v -> storeHelper.createTable(batchTable, context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<CompletedTxnRecord>> getCompletedTx(final UUID txId, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        List<Integer> batches = new ArrayList<>();
        return storeHelper.getAllKeys(COMPLETED_TRANSACTIONS_BATCHES_TABLE, context.getRequestId())
                          .collectRemaining(x -> {
                              batches.add(Integer.parseInt(x));
                              return true;
                          })
                          .thenCompose(v -> {
                              return Futures.allOfWithResults(batches.stream().map(batch -> {
                                  String table = getCompletedTransactionsBatchTableName(batch);
                                  String key = getCompletedTransactionKey(getScope(), getName(), txId.toString());

                                  return storeHelper.expectingDataNotFound(
                                          storeHelper.getCachedOrLoad(table, key, CompletedTxnRecord::fromBytes, 
                                                  0L, context.getRequestId()), null);
                              }).collect(Collectors.toList()));
                          })
                          .thenCompose(result -> {
                              Optional<VersionedMetadata<CompletedTxnRecord>> any = result.stream()
                                                                                          .filter(Objects::nonNull).findFirst();
                              if (any.isPresent()) {
                                  return CompletableFuture.completedFuture(any.get());
                              } else {
                                  throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, 
                                          "Completed Txn not found");
                              }
                          });
    }

    @Override
    public CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncationRecord, 
                                                                OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(metadataTable,
                        TRUNCATION_KEY, truncationRecord, StreamTruncationRecord::toBytes, context.getRequestId())));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final VersionedMetadata<StreamTruncationRecord> truncationRecord, 
                                                 OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, TRUNCATION_KEY,
                        truncationRecord.getObject(), StreamTruncationRecord::toBytes, truncationRecord.getVersion(), 
                        context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unload(metadataTable, TRUNCATION_KEY);
                    }

                    return storeHelper.getCachedOrLoad(metadataTable, TRUNCATION_KEY,  
                            StreamTruncationRecord::fromBytes, context.getOperationStartTime(), context.getRequestId());
                });
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<StreamConfigurationRecord> configuration, 
                                                    final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, CONFIGURATION_KEY,
                        configuration.getObject(), StreamConfigurationRecord::toBytes, configuration.getVersion(), 
                        context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached,
                                                                                         OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unload(metadataTable, CONFIGURATION_KEY);
                    }

                    return storeHelper.getCachedOrLoad(metadataTable, CONFIGURATION_KEY, 
                            StreamConfigurationRecord::fromBytes, context.getOperationStartTime(), context.getRequestId());
                });
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<StateRecord> state, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, STATE_KEY,
                        state.getObject(), StateRecord::toBytes, state.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> {
                    if (ignoreCached) {
                        unload(metadataTable, STATE_KEY);
                    }

                    return storeHelper.getCachedOrLoad(metadataTable, STATE_KEY, StateRecord::fromBytes, 
                            context.getOperationStartTime(), context.getRequestId());
                });
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns, 
                                                          OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(
                        metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, committingTxns, CommittingTransactionsRecord::toBytes, 
                        context.getRequestId())));
    }

    @Override
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY, 
                        CommittingTransactionsRecord::fromBytes, context.getOperationStartTime(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> update, 
                                                         OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.updateEntry(metadataTable, COMMITTING_TRANSACTIONS_RECORD_KEY,
                        update.getObject(), CommittingTransactionsRecord::toBytes, update.getVersion(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String waitingRequestProcessor, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(
                        metadataTable, WAITING_REQUEST_PROCESSOR_PATH, waitingRequestProcessor, 
                        x -> x.getBytes(StandardCharsets.UTF_8), 
                        context.getRequestId())));
    }

    @Override
    CompletableFuture<String> getWaitingRequestNode(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.getCachedOrLoad(metadataTable, WAITING_REQUEST_PROCESSOR_PATH,
                        x -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(x)).toString(), System.currentTimeMillis(), 
                        context.getRequestId()))
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getMetadataTable(context)
                .thenCompose(metadataTable -> storeHelper.removeEntry(metadataTable, WAITING_REQUEST_PROCESSOR_PATH, 
                        context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> createWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position, 
                                                   OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        WriterMark mark = new WriterMark(timestamp, position);
        return Futures.toVoid(getWritersTable(context)
                .thenCompose(table -> storeHelper.addNewEntry(table, writer, mark, WriterMark::toBytes, context.getRequestId())));
    }

    @Override
    public CompletableFuture<Void> removeWriterRecord(String writer, Version version, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getWritersTable(context)
                .thenCompose(table -> storeHelper.removeEntry(table, writer, version, context.getRequestId()));
    }

    @Override
    CompletableFuture<VersionedMetadata<WriterMark>> getWriterMarkRecord(String writer, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getWritersTable(context)
                .thenCompose(table -> storeHelper.getCachedOrLoad(table, writer, WriterMark::fromBytes, 
                        context.getOperationStartTime(), context.getRequestId()));
    }

    @Override
    CompletableFuture<Void> updateWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position, 
                                                   boolean isAlive, Version version, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        WriterMark mark = new WriterMark(timestamp, position, isAlive);
        return Futures.toVoid(getWritersTable(context)
                .thenCompose(table -> storeHelper.updateEntry(table, writer, mark, WriterMark::toBytes, version, 
                        context.getRequestId())));
    }

    @Override
    public CompletableFuture<Map<String, WriterMark>> getAllWriterMarks(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        Map<String, WriterMark> result = new ConcurrentHashMap<>();

        return getWritersTable(context)
                .thenCompose(table -> storeHelper.getAllEntries(table, WriterMark::fromBytes, context.getRequestId())
                .collectRemaining(x -> {
                    result.put(x.getKey(), x.getValue().getObject());
                    return true;
                })).thenApply(v -> result);
    }

    @Override
    public void refresh() {
        idRef.set(null);
    }
    // endregion

    private String getKeyForSubscriber(final String subscriber) {
            return SUBSCRIBER_KEY_PREFIX + subscriber;
    }
    
    private void unload(String tableName, String key) {
        storeHelper.invalidateCache(tableName, key);
    }
}
