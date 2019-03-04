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
import io.netty.util.internal.ConcurrentSet;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.utils.ZKPaths;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.store.stream.ZKStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;

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
    private static final String WAITING_REQUEST_PROCESSOR_PATH = STREAM_PATH + "/waitingRequestProcessor";
    private static final String MARKER_PATH = STREAM_PATH + "/markers";
    private static final String ID_PATH = STREAM_PATH + "/id";
    private static final String STREAM_ACTIVE_TX_PATH = ZKStreamMetadataStore.ACTIVE_TX_ROOT_PATH + "/%s/%S";
    private static final String STREAM_COMPLETED_TX_BATCH_PATH = ZKStreamMetadataStore.COMPLETED_TX_BATCH_PATH + "/%s/%s";

    private static final Data EMPTY_DATA = new Data(null, new Version.IntVersion(Integer.MIN_VALUE));

    private final ZKStoreHelper store;
    private final String creationPath;
    private final String configurationPath;
    private final String truncationPath;
    private final String statePath;
    private final String epochTransitionPath;
    private final String committingTxnsPath;
    private final String waitingRequestProcessorPath;
    private final String activeTxRoot;
    private final String markerPath;
    private final String idPath;
    private final String scopePath;
    @Getter(AccessLevel.PACKAGE)
    private final String streamPath;
    private final String retentionSetPath;
    private final String retentionStreamCutRecordPathFormat;
    private final String currentEpochRecordPath;
    private final String epochRecordPathFormat;
    private final String historyTimeSeriesChunkPathFormat;
    private final String segmentSealedEpochPathFormat;
    private final String segmentsSealedSizeMapShardPathFormat;

    private final Cache cache;
    private final Supplier<Integer> currentBatchSupplier;
    private final Executor executor;
    private final ZkOrderedStore txnCommitOrderer;
    
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
        scopePath = String.format(SCOPE_PATH, scopeName);
        streamPath = String.format(STREAM_PATH, scopeName, streamName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, streamName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, streamName);
        truncationPath = String.format(TRUNCATION_PATH, scopeName, streamName);
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

        cache = new Cache(store::getData);
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
        return store.checkExists(scopePath)
                    .thenAccept(x -> {
                        if (!x) {
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopePath);
                        }
                    });
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] data) {
        return Futures.toVoid(store.createZNodeIfNotExist(retentionSetPath, data));
    }

    @Override
    CompletableFuture<Data> getRetentionSetData() {
        return store.getData(retentionSetPath);
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(Data retention) {
        return store.setData(retentionSetPath, retention)
                .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long recordingTime, byte[] record) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);
        return Futures.toVoid(store.createZNodeIfNotExist(path, record));
    }

    @Override
    CompletableFuture<Data> getStreamCutRecordData(long recordingTime) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);
        return cache.getCachedData(path);
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        String path = String.format(retentionStreamCutRecordPathFormat, recordingTime);

        return store.deletePath(path, false)
                    .thenAccept(x -> cache.invalidateCache(path));
    }
    
    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data) {
        String path = String.format(historyTimeSeriesChunkPathFormat, chunkNumber);
        return Futures.toVoid(store.createZNodeIfNotExist(path, data));
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
        return store.setData(path, data)
                .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data) {
        return Futures.toVoid(store.createZNodeIfNotExist(currentEpochRecordPath, data));
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(Data data) {
        return store.setData(currentEpochRecordPath, data)
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
        return Futures.toVoid(store.createZNodeIfNotExist(path, data));
    }

    @Override
    CompletableFuture<Data> getEpochRecordData(int epoch) {
        String path = String.format(epochRecordPathFormat, epoch);
        return cache.getCachedData(path);
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shard, byte[] data) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return Futures.toVoid(store.createZNodeIfNotExist(path, data));
    }

    @Override
    CompletableFuture<Data> getSealedSegmentSizesMapShardData(int shard) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return store.getData(path);
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, Data data) {
        String path = String.format(segmentsSealedSizeMapShardPathFormat, shard);
        return store.setData(path, data)
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segmentToSeal, int epoch) {
        String path = String.format(segmentSealedEpochPathFormat, segmentToSeal);
        byte[] epochData = new byte[Integer.BYTES];
        BitConverter.writeInt(epochData, 0, epoch);
        return Futures.toVoid(store.createZNodeIfNotExist(path, epochData));
    }

    @Override
    CompletableFuture<Data> getSegmentSealedRecordData(long segmentId) {
        String path = String.format(segmentSealedEpochPathFormat, segmentId);
        return cache.getCachedData(path);
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(byte[] epochTransition) {
        return Futures.toVoid(store.createZNodeIfNotExist(epochTransitionPath, epochTransition));
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(Data epochTransition) {
        return store.setData(epochTransitionPath, epochTransition)
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Data> getEpochTransitionNode() {
        return store.getData(epochTransitionPath);
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, creationTime);

        return Futures.toVoid(store.createZNodeIfNotExist(creationPath, b));
    }

    @Override
    public CompletableFuture<Void> createConfigurationIfAbsent(final byte[] configuration) {
        return Futures.toVoid(store.createZNodeIfNotExist(configurationPath, configuration));
    }

    @Override
    public CompletableFuture<Void> createStateIfAbsent(final byte[] state) {
        return Futures.toVoid(store.createZNodeIfNotExist(statePath, state));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);

        return store.createZNodeIfNotExist(path, b)
                    .thenAccept(x -> cache.invalidateCache(markerPath));
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, Data data) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));

        return store.setData(path, data).thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Data> getMarkerData(long segmentId) {
        final CompletableFuture<Data> result = new CompletableFuture<>();
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentId));
        store.getData(path)
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

        return store.deletePath(path, false)
                    .whenComplete((r, e) -> cache.invalidateCache(path));
    }

    @Override
    public CompletableFuture<Map<String, Data>> getCurrentTxns() {
        return store.getChildren(activeTxRoot)
                    .thenCompose(children -> {
                        return Futures.allOfWithResults(children.stream().map(x -> getTxnInEpoch(Integer.parseInt(x))).collect(Collectors.toList()))
                                      .thenApply(list -> {
                                          Map<String, Data> map = new HashMap<>();
                                          list.forEach(map::putAll);
                                          return map;
                                      });
                    });
    }

    private CompletableFuture<Map<String, Data>> getTxnInEpoch(int epoch) {
        return Futures.exceptionallyExpecting(store.getChildren(getEpochPath(epoch)),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, Collections.emptyList())
                      .thenCompose(txIds -> Futures.allOfWithResults(txIds.stream().collect(
                              Collectors.toMap(txId -> txId, txId -> Futures.exceptionallyExpecting(store.getData(getActiveTxPath(UUID.fromString(txId))),
                                      e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, EMPTY_DATA)))
                              ).thenApply(txnMap -> txnMap.entrySet().stream().filter(x -> !x.getValue().equals(EMPTY_DATA))
                                                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                      );
    }

    @Override
    public CompletableFuture<List<UUID>> getOrderedCommittingTxnInLowestEpoch() {
        // get all transactions that have been added to commit orderer.
        return Futures.exceptionallyExpecting(txnCommitOrderer.getEntitiesWithPosition(getScope(), getName()),
                DATA_NOT_FOUND_PREDICATE, Collections.emptyMap())
                      .thenCompose(allTxns -> {
                          // group transactions by epoch and then iterate over it from smallest epoch to largest
                          val groupByEpoch = allTxns.entrySet().stream().collect(
                                  Collectors.groupingBy(x -> RecordHelper.getTransactionEpoch(UUID.fromString(x.getKey()))));

                          // sort transaction groups by epoch
                          val iterator = groupByEpoch
                                  .entrySet().stream()
                                  .sorted(Comparator.comparingInt(Map.Entry::getKey))
                                  .iterator();

                          // We will opportunistically identify ordered positions that are stale (either transaction is no longer active)
                          // or its a duplicate entry or transaction is aborting. 
                          ConcurrentSet<Long> toPurge = new ConcurrentSet<>();
                          ConcurrentHashMap<String, ActiveTxnRecord> transactionsMap = new ConcurrentHashMap<>();

                          // Collect transactions that are in committing state from smallest available epoch 
                          // smallest epoch has transactions in committing state, we should break, else continue.
                          // also remove any transaction order references which are invalid.
                          return Futures.loop(() -> iterator.hasNext() && transactionsMap.isEmpty(), () -> {
                              return processTransactionsInEpoch(iterator.next(), toPurge, transactionsMap);
                          }, executor).thenCompose(v -> txnCommitOrderer.removeEntities(getScope(), getName(), toPurge))
                                        .thenApply(v ->
                                                transactionsMap.entrySet().stream().sorted(
                                                        Comparator.comparing(x -> x.getValue().getCommitOrder()))
                                                               .map(x -> UUID.fromString(x.getKey())).collect(Collectors.toList()));
                      });
    }

    private CompletableFuture<Void> processTransactionsInEpoch(Map.Entry<Integer, List<Map.Entry<String, Long>>> nextEpoch,
                                                               ConcurrentSet<Long> toPurge, 
                                                               ConcurrentHashMap<String, ActiveTxnRecord> transactionsMap) {
        List<Map.Entry<String, Long>> txnIds = nextEpoch.getValue();

        return Futures.allOf(txnIds.stream().map(txnIdOrder -> {
            String txnId = txnIdOrder.getKey();
            long order = txnIdOrder.getValue();
            return Futures.exceptionallyExpecting(getTxnRecord(txnId), DATA_NOT_FOUND_PREDICATE, ActiveTxnRecord.EMPTY)
                          .thenAccept(txnRecord -> {
                              switch (txnRecord.getTxnStatus()) {
                                  case COMMITTING:
                                      if (txnRecord.getCommitOrder() == order) {
                                          // if entry matches record's position then include it
                                          transactionsMap.put(txnId, txnRecord);
                                      } else {
                                          toPurge.add(order);
                                      }
                                      break;
                                  case OPEN:  // do nothing
                                      break;
                                  default:
                                      // Aborting, aborted, unknown and committed 
                                      toPurge.add(order);
                                      break;
                              }
                          });

        }).collect(Collectors.toList()));
    }
    
    private CompletableFuture<ActiveTxnRecord> getTxnRecord(String txId) {
        return Futures.exceptionallyExpecting(
                getActiveTx(UUID.fromString(txId)).thenApply(y -> ActiveTxnRecord.fromBytes(y.getData())),
                DATA_NOT_FOUND_PREDICATE, ActiveTxnRecord.EMPTY);
    }

    @Override
    CompletableFuture<Version> createNewTransaction(final UUID txId, final byte[] txnRecord) {
        final String activePath = getActiveTxPath(txId);
        // we will always create parent if needed so that transactions are created successfully even if the epoch znode
        // previously found to be empty and deleted.
        // For this, send createParent flag = true
        return store.createZNodeIfNotExist(activePath, txnRecord, true)
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Data> getActiveTx(final UUID txId) {
        final String activeTxPath = getActiveTxPath(txId);
        return store.getData(activeTxPath);
    }

    @Override
    CompletableFuture<Version> updateActiveTx(final UUID txId, final Data data) {
        final String activeTxPath = getActiveTxPath(txId);
        return store.setData(activeTxPath, data)
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final UUID txId) {
        final String activePath = getActiveTxPath(txId);
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
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final byte[] complete) {
        String root = String.format(STREAM_COMPLETED_TX_BATCH_PATH, currentBatchSupplier.get(), getScope(), getName());
        String path = ZKPaths.makePath(root, txId.toString());

        return Futures.toVoid(store.createZNodeIfNotExist(path, complete));
    }


    @Override
    CompletableFuture<Data> getCompletedTx(final UUID txId) {
        return store.getChildren(ZKStreamMetadataStore.COMPLETED_TX_BATCH_ROOT_PATH)
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
        return Futures.toVoid(store.createZNodeIfNotExist(truncationPath, truncationRecord));
    }

    @Override
    CompletableFuture<Version> setTruncationData(final Data truncationRecord) {
        return store.setData(truncationPath, truncationRecord)
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
        return store.setData(configurationPath, configuration)
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
        return store.setData(statePath, state)
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
        return Futures.toVoid(store.createZNodeIfNotExist(committingTxnsPath, committingTxns));
    }

    @Override
    CompletableFuture<Data> getCommitTxnRecord() {
        return store.getData(committingTxnsPath);
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(Data update) {
        return store.setData(committingTxnsPath, update)
                    .thenApply(Version.IntVersion::new);
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] waitingRequestProcessor) {
        return Futures.toVoid(store.createZNodeIfNotExist(waitingRequestProcessorPath, waitingRequestProcessor));
    }

    @Override
    CompletableFuture<Data> getWaitingRequestNode() {
        return store.getData(waitingRequestProcessorPath);
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        return store.deletePath(waitingRequestProcessorPath, false);
    }

    @Override
    public void refresh() {
        // refresh all mutable records
        cache.invalidateCache(statePath);
        cache.invalidateCache(configurationPath);
        cache.invalidateCache(truncationPath);
        cache.invalidateCache(epochRecordPathFormat);
        cache.invalidateCache(committingTxnsPath);
        cache.invalidateCache(currentEpochRecordPath);
        cache.invalidateCache(currentEpochRecordPath);
    }
    // endregion

    // region private helpers
    @VisibleForTesting
    String getActiveTxPath(final UUID txId) {
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return ZKPaths.makePath(ZKPaths.makePath(activeTxRoot, Integer.toString(epoch)), txId.toString());
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
        return store.getData(idPath)
                .thenApply(data -> BitConverter.readInt(data.getData(), 0));
    }
}
