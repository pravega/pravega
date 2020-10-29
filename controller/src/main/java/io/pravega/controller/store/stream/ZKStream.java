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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.stream.records.StreamSubscriber;
import io.pravega.controller.store.stream.records.SubscriberSet;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.utils.ZKPaths;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 * <p>
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
@Slf4j
class ZKStream extends PersistentStreamBase {
    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAM_PATH = SCOPE_PATH + "/%s";
    private static final String CREATION_TIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configuration";
    private static final String TRUNCATION_PATH = STREAM_PATH + "/truncation";
    private static final String SUBSCRIBERS_PATH = STREAM_PATH + "/subscribers";
    private static final String STATE_PATH = STREAM_PATH + "/state";
    private static final String EPOCH_TRANSITION_PATH = STREAM_PATH + "/epochTransition";
    private static final String RETENTION_SET_PATH = STREAM_PATH + "/retention";
    private static final String RETENTION_STREAM_CUT_RECORD_PATH = STREAM_PATH + "/retentionCuts";
    private static final String CURRENT_EPOCH_RECORD = STREAM_PATH + "/currentEpochRecord";
    private static final String EPOCH_RECORD = STREAM_PATH + "/epochRecords";
    private static final String HISTORY_TIMESERIES_CHUNK_PATH = STREAM_PATH + "/historyTimeSeriesChunks";
    private static final String SEGMENTS_SEALED_SIZE_MAP_SHARD_PATH = STREAM_PATH + "/segmentsSealedSizeMapShardPath";
    private static final String SEGMENT_SEALED_EPOCH_PATH = STREAM_PATH + "/segmentSealedEpochPath";
    private static final String COMMITTING_TXNS_PATH = STREAM_PATH + "/committingTxns";
    private static final String WRITER_POSITIONS_PATH = STREAM_PATH + "/writerPositions";
    private static final String WAITING_REQUEST_PROCESSOR_PATH = STREAM_PATH + "/waitingRequestProcessor";
    private static final String MARKER_PATH = STREAM_PATH + "/markers";
    private static final String ID_PATH = STREAM_PATH + "/id";
    private static final String STREAM_ACTIVE_TX_PATH = ZKStreamMetadataStore.ACTIVE_TX_ROOT_PATH + "/%s/%S";
    private static final String STREAM_COMPLETED_TX_BATCH_PATH = ZKStreamMetadataStore.COMPLETED_TX_BATCH_PATH + "/%s/%s";

    private final ZKStoreHelper store;
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final String creationPath;
    private final String configurationPath;
    private final String truncationPath;
    private final String subscribersPath;
    private final String statePath;
    private final String epochTransitionPath;
    private final String committingTxnsPath;
    private final String waitingRequestProcessorPath;
    private final String activeTxRoot;
    private final String markerPath;
    private final String idPath;
    @Getter(AccessLevel.PACKAGE)
    private final String streamPath;
    private final String retentionSetPath;
    private final String retentionStreamCutRecordPathFormat;
    private final String currentEpochRecordPath;
    private final String epochRecordPathFormat;
    private final String historyTimeSeriesChunkPathFormat;
    private final String segmentSealedEpochPathFormat;
    private final String segmentsSealedSizeMapShardPathFormat;
    private final String writerPositionsPath;

    private final Supplier<Integer> currentBatchSupplier;
    private final Executor executor;
    private final ZkOrderedStore txnCommitOrderer;
    
    private final AtomicReference<String> idRef;

    @VisibleForTesting
    ZKStream(final String scopeName, final String streamName, ZKStoreHelper storeHelper, Executor executor, ZkOrderedStore txnCommitOrderer) {
        this(scopeName, streamName, storeHelper, () -> 0, executor, txnCommitOrderer);
    }

    @VisibleForTesting
    ZKStream(final String scopeName, final String streamName, ZKStoreHelper storeHelper, int chunkSize, int shardSize, Executor executor, ZkOrderedStore txnCommitOrderer) {
        this(scopeName, streamName, storeHelper, () -> 0, chunkSize, shardSize, executor, txnCommitOrderer);
    }

    @VisibleForTesting
    ZKStream(final String scopeName, final String streamName, ZKStoreHelper storeHelper, Supplier<Integer> currentBatchSupplier,
             Executor executor, ZkOrderedStore txnCommitOrderer) {
        this(scopeName, streamName, storeHelper, currentBatchSupplier, HistoryTimeSeries.HISTORY_CHUNK_SIZE, SealedSegmentsMapShard.SHARD_SIZE,
                executor, txnCommitOrderer);
    }
    
    @VisibleForTesting
    ZKStream(final String scopeName, final String streamName, ZKStoreHelper storeHelper, Supplier<Integer> currentBatchSupplier,
             int chunkSize, int shardSize, Executor executor, ZkOrderedStore txnCommitOrderer) {
        super(scopeName, streamName, chunkSize, shardSize);
        store = storeHelper;
        streamPath = String.format(STREAM_PATH, scopeName, streamName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, streamName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, streamName);
        truncationPath = String.format(TRUNCATION_PATH, scopeName, streamName);
        subscribersPath = String.format(SUBSCRIBERS_PATH, scopeName, streamName);
        statePath = String.format(STATE_PATH, scopeName, streamName);
        retentionSetPath = String.format(RETENTION_SET_PATH, scopeName, streamName);
        retentionStreamCutRecordPathFormat = String.format(RETENTION_STREAM_CUT_RECORD_PATH, scopeName, streamName) + "/%d";
        epochTransitionPath = String.format(EPOCH_TRANSITION_PATH, scopeName, streamName);
        activeTxRoot = String.format(STREAM_ACTIVE_TX_PATH, scopeName, streamName);
        committingTxnsPath = String.format(COMMITTING_TXNS_PATH, scopeName, streamName);
        waitingRequestProcessorPath = String.format(WAITING_REQUEST_PROCESSOR_PATH, scopeName, streamName);
        markerPath = String.format(MARKER_PATH, scopeName, streamName);
        idPath = String.format(ID_PATH, scopeName, streamName);
        currentEpochRecordPath = String.format(CURRENT_EPOCH_RECORD, scopeName, streamName);
        epochRecordPathFormat = String.format(EPOCH_RECORD, scopeName, streamName) + "/%d";
        historyTimeSeriesChunkPathFormat = String.format(HISTORY_TIMESERIES_CHUNK_PATH, scopeName, streamName) + "/%d";
        segmentSealedEpochPathFormat = String.format(SEGMENT_SEALED_EPOCH_PATH, scopeName, streamName) + "/%d";
        segmentsSealedSizeMapShardPathFormat = String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_PATH, scopeName, streamName) + "/%d";
        writerPositionsPath = String.format(WRITER_POSITIONS_PATH, scopeName, streamName);
        idRef = new AtomicReference<>();
        this.currentBatchSupplier = currentBatchSupplier;
        this.executor = executor;
        this.txnCommitOrderer = txnCommitOrderer;
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
    public CompletableFuture<Void> deleteStream() {
        return store.deleteTree(streamPath);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime, final int startingSegmentNumber) {
        // If stream exists, but is in a partially complete state, then fetch its creation time and configuration and any
        // metadata that is available from a previous run. If the existing stream has already been created successfully earlier,
        return store.checkExists(creationPath).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                        configuration, creationTime, startingSegmentNumber));
            }

            return getCreationTime().thenCompose(storedCreationTime ->
                    store.checkExists(configurationPath).thenCompose(configExists -> {
                        if (configExists) {
                            return handleConfigExists(storedCreationTime, startingSegmentNumber, storedCreationTime == creationTime);
                        } else {
                            return CompletableFuture.completedFuture(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW,
                                    configuration, storedCreationTime, startingSegmentNumber));
                        }
                    }));
        });
    }

    @Override
    CompletableFuture<Void> createStreamMetadata() {
        return Futures.toVoid(store.createZNodeIfNotExist(getStreamPath()));
    }

    private CompletableFuture<CreateStreamResponse> handleConfigExists(long creationTime, int startingSegmentNumber, boolean creationTimeMatched) {
        CreateStreamResponse.CreateStatus status = creationTimeMatched ?
                CreateStreamResponse.CreateStatus.NEW : CreateStreamResponse.CreateStatus.EXISTS_CREATING;

        return getConfiguration().thenCompose(config -> store.checkExists(statePath)
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
        return getId().thenCompose(id -> store.getCachedData(creationPath, id, x -> BitConverter.readLong(x, 0))
                .thenApply(VersionedMetadata::getObject));
    }

    @Override
    public CompletableFuture<Void> createSubscriber(String subscriber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriberRecord(String subscriber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> updateSubscriberStreamCut(final StreamSubscriber subscriber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> removeSubscriber(String subscriber) {
        throw new UnsupportedOperationException();
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet data) {
        return Futures.toVoid(store.createZNodeIfNotExist(retentionSetPath, data.toBytes()));
    }

    @Override
    CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData() {
        return store.getData(retentionSetPath, RetentionSet::fromBytes);
    }


    @Override
    CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> retention) {
        return store.setData(retentionSetPath, retention.getObject().toBytes(), retention.getVersion())
                .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, StreamCutRecord record) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);
        return Futures.toVoid(store.createZNodeIfNotExist(path, record.toBytes()));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);
        return getId().thenCompose(id -> store.getCachedData(path, id, StreamCutRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);

        return getId().thenCompose(id -> store.deletePath(path, false)
                    .thenAccept(x -> store.invalidateCache(path, id)));
    }
    
    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data) {
        String path = String.format(historyTimeSeriesChunkPathFormat, chunkNumber);
        return Futures.toVoid(store.createZNodeIfNotExist(path, data.toBytes()));
    }

    @Override
    CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        return getId().thenCompose(id -> {
            String path = String.format(historyTimeSeriesChunkPathFormat, chunkNumber);
            if (ignoreCached) {
                return store.getData(path, HistoryTimeSeries::fromBytes);
            }
            return store.getCachedData(path, id, HistoryTimeSeries::fromBytes);
        });
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int chunkNumber, VersionedMetadata<HistoryTimeSeries> data) {
        String path = String.format(historyTimeSeriesChunkPathFormat, chunkNumber);
        return store.setData(path, data.getObject().toBytes(), data.getVersion())
                .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getEpoch());

        return Futures.toVoid(store.createZNodeIfNotExist(currentEpochRecordPath, epochData));
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> data) {
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, data.getObject().getEpoch());

        return store.setData(currentEpochRecordPath, epochData, data.getVersion())
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached) {
        return getId().thenCompose(id -> {

            CompletableFuture<VersionedMetadata<Integer>> future;
            if (ignoreCached) {
                future = store.getData(currentEpochRecordPath, x -> BitConverter.readInt(x, 0));
            } else {
                future = store.getCachedData(currentEpochRecordPath, id, x -> BitConverter.readInt(x, 0));
            }
            return future.thenCompose(versionedEpochNumber -> getEpochRecord(versionedEpochNumber.getObject())
                    .thenApply(epochRecord -> new VersionedMetadata<>(epochRecord, versionedEpochNumber.getVersion())));
        });
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data) {
        String path = String.format(epochRecordPathFormat, epoch);
        return Futures.toVoid(store.createZNodeIfNotExist(path, data.toBytes()));
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch) {
        String path = String.format(epochRecordPathFormat, epoch);
        return getId().thenCompose(id -> store.getCachedData(path, id, EpochRecord::fromBytes));
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, SealedSegmentsMapShard data) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return Futures.toVoid(store.createZNodeIfNotExist(path, data.toBytes()));
    }

    @Override
    CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(int shard) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return store.getData(path, SealedSegmentsMapShard::fromBytes);
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, VersionedMetadata<SealedSegmentsMapShard> data) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return store.setData(path, data.getObject().toBytes(), data.getVersion())
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecords(Collection<Long> segmentToSeal, int epoch) {
        return Futures.allOf(segmentToSeal.stream().map(x -> createSegmentSealedEpochRecordData(x, epoch)).collect(Collectors.toList()));
    }

    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segmentToSeal, int epoch) {
        String path = String.format(segmentSealedEpochPathFormat, segmentToSeal);
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, epoch);
        return Futures.toVoid(store.createZNodeIfNotExist(path, epochData));
    }

    @Override
    CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId) {
        String path = String.format(segmentSealedEpochPathFormat, segmentId);
        return getId().thenCompose(id -> store.getCachedData(path, id, x -> BitConverter.readInt(x, 0)));
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransition) {
        return Futures.toVoid(store.createZNodeIfNotExist(epochTransitionPath, epochTransition.toBytes()));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> epochTransition) {
        return store.setData(epochTransitionPath, epochTransition.getObject().toBytes(), epochTransition.getVersion())
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode() {
        return store.getData(epochTransitionPath, EpochTransitionRecord::fromBytes);
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return Futures.toVoid(store.createZNodeIfNotExist(creationPath, b));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord configuration) {
        return Futures.toVoid(store.createZNodeIfNotExist(configurationPath, configuration.toBytes()));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final StateRecord state) {
        return Futures.toVoid(store.createZNodeIfNotExist(statePath, state.toBytes()));
    }

    @Override
    CompletableFuture<Void> createSubscribersRecordIfAbsent() {
        SubscriberSet subscribersSetRecord = new SubscriberSet(ImmutableList.of());
        return Futures.toVoid(store.createZNodeIfNotExist(subscribersPath, subscribersSetRecord.toBytes()));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return getId().thenCompose(id -> store.createZNodeIfNotExist(path, b)
                    .thenAccept(x -> store.invalidateCache(markerPath, id)));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, data.getObject());

        return store.setData(path, b, data.getVersion()).thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId) {
        final CompletableFuture<VersionedMetadata<Long>> result = new CompletableFuture<>();
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));
        store.getData(path, x -> BitConverter.readLong(x, 0))
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

        return getId().thenCompose(id -> store.deletePath(path, false)
                    .whenComplete((r, e) -> store.invalidateCache(path, id)));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
        return store.getChildren(activeTxRoot)
                    .thenCompose(children -> {
                        return Futures.allOfWithResults(children.stream().map(x -> getTxnInEpoch(Integer.parseInt(x))).collect(Collectors.toList()))
                                      .thenApply(list -> {
                                          Map<UUID, ActiveTxnRecord> map = new HashMap<>();
                                          list.forEach(map::putAll);
                                          return map;
                                      });
                    });
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnInEpoch(int epoch) {
        VersionedMetadata<ActiveTxnRecord> empty = getEmptyData();
        return Futures.exceptionallyExpecting(store.getChildren(getEpochPath(epoch)),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, Collections.emptyList())
                      .thenCompose(txIds -> Futures.allOfWithResults(txIds.stream().collect(
                              Collectors.toMap(txId -> txId, 
                                      txId -> Futures.exceptionallyExpecting(store.getData(getActiveTxPath(epoch, txId), ActiveTxnRecord::fromBytes),
                                      e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, empty)))
                              ).thenApply(txnMap -> txnMap.entrySet().stream().filter(x -> !x.getValue().equals(empty))
                                                          .collect(Collectors.toMap(x -> UUID.fromString(x.getKey()), 
                                                                  x -> x.getValue().getObject())))
                      );
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
    CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final ActiveTxnRecord txnRecord) {
        final String activePath = getActiveTxPath(epoch, txId.toString());
        // we will always create parent if needed so that transactions are created successfully even if the epoch znode
        // previously found to be empty and deleted.
        // For this, send createParent flag = true
        return store.createZNodeIfNotExist(activePath, txnRecord.toBytes(), true)
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(final int epoch, final UUID txId) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return store.getData(activeTxPath, ActiveTxnRecord::fromBytes);
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final int epoch, final UUID txId, final VersionedMetadata<ActiveTxnRecord> data) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return store.setData(activeTxPath, data.getObject().toBytes(), data.getVersion())
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId) {
        final String activePath = getActiveTxPath(epoch, txId.toString());
        // attempt to delete empty epoch nodes by sending deleteEmptyContainer flag as true.
        return store.deletePath(activePath, true);
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
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final CompletedTxnRecord complete) {
        String root = String.format(STREAM_COMPLETED_TX_BATCH_PATH, currentBatchSupplier.get(), getScope(), getName());
        String path = ZKPaths.makePath(root, txId.toString());

        return Futures.toVoid(store.createZNodeIfNotExist(path, complete.toBytes()));
    }


    @Override
    CompletableFuture<VersionedMetadata<CompletedTxnRecord>> getCompletedTx(final UUID txId) {
        return getId().thenCompose(id -> store.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH)
                    .thenCompose(children -> {
                        return Futures.allOfWithResults(children.stream().map(child -> {
                            String root = String.format(STREAM_COMPLETED_TX_BATCH_PATH, Long.parseLong(child), getScope(), getName());
                            String path = ZKPaths.makePath(root, txId.toString());

                            return store.getCachedData(path, id, CompletedTxnRecord::fromBytes)
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
                        Optional<VersionedMetadata<CompletedTxnRecord>> any = result.stream().filter(Objects::nonNull).findFirst();
                        if (any.isPresent()) {
                            return CompletableFuture.completedFuture(any.get());
                        } else {
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "Completed Txn not found");
                        }
                    }));
    }

    @Override
    public CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncationRecord) {
        return Futures.toVoid(store.createZNodeIfNotExist(truncationPath, truncationRecord.toBytes()));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final VersionedMetadata<StreamTruncationRecord> truncationRecord) {
        return getId().thenCompose(id -> store.setData(truncationPath, truncationRecord.getObject().toBytes(), truncationRecord.getVersion())
                    .thenApply(r -> {
                        store.invalidateCache(truncationPath, id);
                        return new Version.IntVersion(r);
                    }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached) {
        return getId().thenCompose(id -> {
            if (ignoreCached) {
                return store.getData(truncationPath, StreamTruncationRecord::fromBytes);
            }

            return store.getCachedData(truncationPath, id, StreamTruncationRecord::fromBytes);
        });
    }

    @Override
    CompletableFuture<Version> setConfigurationData(final VersionedMetadata<StreamConfigurationRecord> configuration) {
        return getId().thenCompose(id -> store.setData(configurationPath, configuration.getObject().toBytes(), configuration.getVersion())
                    .thenApply(r -> {
                        store.invalidateCache(configurationPath, id);
                        return new Version.IntVersion(r);
                    }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached) {
        return getId().thenCompose(id -> {
            if (ignoreCached) {
                return store.getData(configurationPath, StreamConfigurationRecord::fromBytes);
            }

            return store.getCachedData(configurationPath, id, StreamConfigurationRecord::fromBytes);
        });
    }

    @Override
    CompletableFuture<Version> setStateData(final VersionedMetadata<StateRecord> state) {
        return getId().thenCompose(id -> store.setData(statePath, state.getObject().toBytes(), state.getVersion())
                    .thenApply(r -> {
                        store.invalidateCache(statePath, id);
                        return new Version.IntVersion(r);
                    }));
    }

    @Override
    CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached) {
        return getId().thenCompose(id -> {
            if (ignoreCached) {
                return store.getData(statePath, StateRecord::fromBytes);
            }

            return store.getCachedData(statePath, id, StateRecord::fromBytes);
        });
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns) {
        return Futures.toVoid(store.createZNodeIfNotExist(committingTxnsPath, committingTxns.toBytes()));
    }

    @Override
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord() {
        return store.getData(committingTxnsPath, CommittingTransactionsRecord::fromBytes);
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> update) {
        return store.setData(committingTxnsPath, update.getObject().toBytes(), update.getVersion())
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    public CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String waitingRequestProcessor) {
        return Futures.toVoid(store.createZNodeIfNotExist(waitingRequestProcessorPath, 
                waitingRequestProcessor.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public CompletableFuture<String> getWaitingRequestNode() {
        return store.getData(waitingRequestProcessorPath, x -> StandardCharsets.UTF_8.decode(ByteBuffer.wrap(x)).toString())
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> deleteWaitingRequestNode() {
        return store.deletePath(waitingRequestProcessorPath, false);
    }

    @Override
    CompletableFuture<Void> createWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position) {
        String writerPath = getWriterPath(writer);
        WriterMark mark = new WriterMark(timestamp, position);
        return Futures.toVoid(store.createZNode(writerPath, mark.toBytes()));
    }

    @Override
    public CompletableFuture<Void> removeWriterRecord(String writer, Version version) {
        String writerPath = getWriterPath(writer);
        return store.deleteNode(writerPath, version);
    }

    @Override
    CompletableFuture<VersionedMetadata<WriterMark>> getWriterMarkRecord(String writer) {
        String writerPath = getWriterPath(writer);
        return store.getData(writerPath, WriterMark::fromBytes);
    }

    @Override
    CompletableFuture<Void> updateWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position,
                                                   boolean isAlive, Version version) {
        String writerPath = getWriterPath(writer);
        WriterMark mark = new WriterMark(timestamp, position, isAlive);

        return Futures.toVoid(store.setData(writerPath, mark.toBytes(), version));
    }

    private String getWriterPath(String writer) {
        return ZKPaths.makePath(writerPositionsPath, writer);
    }

    @Override
    public CompletableFuture<Map<String, WriterMark>> getAllWriterMarks() {
        return store.getChildren(writerPositionsPath)
                .thenCompose(children -> {
                    return Futures.allOfWithResults(children.stream().collect(Collectors.toMap(writer -> writer, 
                            writer -> Futures.exceptionallyExpecting(getWriterMark(writer), 
                                    DATA_NOT_FOUND_PREDICATE, WriterMark.EMPTY))))
                            .thenApply(map -> map.entrySet().stream().filter(x -> !x.getValue().equals(WriterMark.EMPTY))
                                                 .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
                });
    }

    @Override
    public void refresh() {
        String id = this.idRef.getAndSet(null);
        id = id == null ? "" : id;
        // invalidate all mutable records in the cache 
        store.invalidateCache(statePath, id);
        store.invalidateCache(configurationPath, id);
        store.invalidateCache(truncationPath, id);
        store.invalidateCache(epochTransitionPath, id);
        store.invalidateCache(committingTxnsPath, id);
        store.invalidateCache(currentEpochRecordPath, id);
    }

    /**
     * Method to retrieve unique Id for the stream. We use streamPosition as the unique id for the stream.
     * If the id had been previously retrieved then this method simply returns
     * the previous value else it retrieves the stored stream position from zookeeper. 
     * The id of a stream is fixed for lifecycle of a stream and only changes when the stream is deleted and recreated. 
     * The id is used for caching entities and safeguarding against stream recreation. 
     * @return CompletableFuture which when completed contains stream's position as the id. 
     */
    private CompletableFuture<String> getId() {
        String id = this.idRef.get();
        if (!Strings.isNullOrEmpty(id)) {
            return CompletableFuture.completedFuture(id);
        } else {
            // We will return empty string as id if the position does not exist. 
            // This can happen if stream is being created and we access the cache. 
            // Even if we access/load the cache against empty id, eventually cache will be populated against correct id 
            // once it is created. 
            return Futures.exceptionallyExpecting(getStreamPosition()
                    .thenApply(pos -> {
                        String s = pos.toString();
                        this.idRef.compareAndSet(null, s);
                        return s;
                    }), e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, "");
        }
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

    CompletableFuture<Void> createStreamPositionNodeIfAbsent(int streamPosition) {
        byte[] b = new byte[Integer.BYTES];
        BitConverter.writeInt(b, 0, streamPosition);

        return Futures.toVoid(store.createZNodeIfNotExist(idPath, b));
    }
    
    CompletableFuture<Integer> getStreamPosition() {
        return store.getData(idPath, x -> BitConverter.readInt(x, 0))
                .thenApply(VersionedMetadata::getObject);
    }

    private static <T> VersionedMetadata<T> getEmptyData() {
        return new VersionedMetadata<>(null, new Version.IntVersion(Integer.MIN_VALUE));
    }

}
