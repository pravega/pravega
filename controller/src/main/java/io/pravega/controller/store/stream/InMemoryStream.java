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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RecordHelper;
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
import io.pravega.controller.util.Config;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.Optional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class InMemoryStream extends PersistentStreamBase {

    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private VersionedMetadata<StreamConfigurationRecord> configuration;
    @GuardedBy("lock")
    private VersionedMetadata<StreamTruncationRecord> truncationRecord;
    @GuardedBy("lock")
    private VersionedMetadata<StateRecord> state;
    @GuardedBy("lock")
    private VersionedMetadata<EpochRecord> currentEpochRecord;
    @GuardedBy("lock")
    private Map<Integer, VersionedMetadata<EpochRecord>> epochRecords = new HashMap<>();
    @GuardedBy("lock")
    private Map<Integer, VersionedMetadata<HistoryTimeSeries>> historyTimeSeries = new HashMap<>();
    @GuardedBy("lock")
    private VersionedMetadata<RetentionSet> retentionSet;
    @GuardedBy("lock")
    private final Map<Long, VersionedMetadata<StreamCutRecord>> streamCutRecords = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Integer, VersionedMetadata<SealedSegmentsMapShard>> sealedSegmentsShards = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Long, VersionedMetadata<Integer>> segmentSealingEpochs = new HashMap<>();
    @GuardedBy("lock")
    private VersionedMetadata<EpochTransitionRecord> epochTransition;
    @GuardedBy("lock")
    private VersionedMetadata<CommittingTransactionsRecord> committingTxnRecord;
    @GuardedBy("lock")
    private String waitingRequestNode;

    private final Object txnsLock = new Object();
    @GuardedBy("txnsLock")
    private final Map<UUID, VersionedMetadata<ActiveTxnRecord>> activeTxns = new HashMap<>();
    private final AtomicLong counter = new AtomicLong();
    private final ConcurrentHashMap<Long, UUID> transactionCommitOrder = new ConcurrentHashMap<>();
    @GuardedBy("txnsLock")
    private final Cache<UUID, VersionedMetadata<CompletedTxnRecord>> completedTxns;
    private final Object markersLock = new Object();
    @GuardedBy("markersLock")
    private final Map<Long, VersionedMetadata<Long>> markers = new HashMap<>();
    /**
     * This is used to guard updates to values in epoch txn map.
     * This ensures that we remove an epoch node if an only if there are no transactions against that epoch.
     * Note: there can be only two epochs at max concurrently. So using one lock for both of their updates is okay.
     */
    @GuardedBy("txnsLock")
    private final Map<Integer, Set<UUID>> epochTxnMap = new HashMap<>();

    private final Object writersLock = new Object();

    @GuardedBy("writersLock")
    private final Map<String, VersionedMetadata<WriterMark>> writerMarks = new HashMap<>();

    private final Object subscribersLock = new Object();

    @GuardedBy("subscribersLock")
    private final List<VersionedMetadata<StreamSubscriber>> streamSubscribers = new ArrayList<>();

    InMemoryStream(String scope, String name) {
        this(scope, name, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS).toMillis());
    }

    @VisibleForTesting
    InMemoryStream(String scope, String name, int chunkSize, int shardSize) {
        this(scope, name, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS).toMillis(), chunkSize, shardSize);
    }

    @VisibleForTesting
    InMemoryStream(String scope, String name, long completedTxnTTL) {
        this(scope, name, completedTxnTTL, HistoryTimeSeries.HISTORY_CHUNK_SIZE, SealedSegmentsMapShard.SHARD_SIZE);
    }

    @VisibleForTesting
    InMemoryStream(String scope, String name, long completedTxnTTL, int chunkSize, int shardSize) {
        super(scope, name, chunkSize, shardSize);
        completedTxns = CacheBuilder.newBuilder()
                                    .expireAfterWrite(completedTxnTTL, TimeUnit.MILLISECONDS).build();
    }

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        synchronized (txnsLock) {
            return CompletableFuture.completedFuture(activeTxns.size());
        }
    }

    @Override
    public void refresh() {

    }

    @Override
    CompletableFuture<Void> deleteStream() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<CreateStreamResponse> checkStreamExists(StreamConfiguration configuration, long timestamp, final int startingSegmentNumber) {
        CompletableFuture<CreateStreamResponse> result = new CompletableFuture<>();

        final long time;
        final StreamConfigurationRecord config;
        final VersionedMetadata<StateRecord> currentState;
        synchronized (lock) {
            time = creationTime.get();
            config = this.configuration == null ? null : this.configuration.getObject();
            currentState = this.state;
        }

        if (time != Long.MIN_VALUE) {
            if (config != null) {
                handleStreamMetadataExists(timestamp, result, time, startingSegmentNumber, config.getStreamConfiguration(), currentState);
            } else {
                result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW, configuration, time, startingSegmentNumber));
            }
        } else {
            result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW, configuration, timestamp, startingSegmentNumber));
        }

        return result;
    }

    @Override
    CompletableFuture<Void> createStreamMetadata() {
        return CompletableFuture.completedFuture(null);
    }

    private void handleStreamMetadataExists(final long timestamp, CompletableFuture<CreateStreamResponse> result, final long time,
                                            final int startingSegmentNumber, final StreamConfiguration config, VersionedMetadata<StateRecord> currentState) {
        if (currentState != null) {
            State stateVal = currentState.getObject().getState();
            if (stateVal.equals(State.UNKNOWN) || stateVal.equals(State.CREATING)) {
                CreateStreamResponse.CreateStatus status;
                status = (time == timestamp) ? CreateStreamResponse.CreateStatus.NEW :
                        CreateStreamResponse.CreateStatus.EXISTS_CREATING;
                result.complete(new CreateStreamResponse(status, config, time, startingSegmentNumber));
            } else {
                result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, config, time, startingSegmentNumber));
            }
        } else {
            CreateStreamResponse.CreateStatus status = (time == timestamp) ? CreateStreamResponse.CreateStatus.NEW :
                    CreateStreamResponse.CreateStatus.EXISTS_CREATING;

            result.complete(new CreateStreamResponse(status, config, time, startingSegmentNumber));
        }
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long timestamp) {
        creationTime.compareAndSet(Long.MIN_VALUE, timestamp);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Long> getCreationTime() {
        return CompletableFuture.completedFuture(creationTime.get());
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(StreamConfigurationRecord config) {
        Preconditions.checkNotNull(config);

        synchronized (lock) {
            if (configuration == null) {
                configuration = new VersionedMetadata<>(config, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createTruncationDataIfAbsent(StreamTruncationRecord truncation) {
        Preconditions.checkNotNull(truncation);

        synchronized (lock) {
            if (truncationRecord == null) {
                truncationRecord = new VersionedMetadata<>(truncation, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> setConfigurationData(VersionedMetadata<StreamConfigurationRecord> newConfig) {
        Preconditions.checkNotNull(newConfig);

        CompletableFuture<Version> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.configuration == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                if (Objects.equals(this.configuration.getVersion(), newConfig.getVersion())) {
                    this.configuration = updatedCopy(new VersionedMetadata<>(newConfig.getObject(), this.configuration.getVersion()));
                    result.complete(this.configuration.getVersion());
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
                }
            }
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.configuration == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(this.configuration);
        }
    }

    @Override
    CompletableFuture<Version> setTruncationData(VersionedMetadata<StreamTruncationRecord> truncationRecord) {
        Preconditions.checkNotNull(truncationRecord);

        CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<StreamTruncationRecord> updatedCopy = updatedCopy(truncationRecord);
        synchronized (lock) {
            if (this.truncationRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "truncation record not found"));
            } else if (Objects.equals(this.truncationRecord.getVersion(), truncationRecord.getVersion())) {
                this.truncationRecord = updatedCopy;
                result.complete(updatedCopy.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.truncationRecord == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(this.truncationRecord);
        }
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(StateRecord state) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            if (this.state == null) {
                this.state = new VersionedMetadata<>(state, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createSubscribersRecordIfAbsent() {
        Preconditions.checkNotNull(streamSubscribers);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> setStateData(VersionedMetadata<StateRecord> newState) {
        Preconditions.checkNotNull(newState);

        CompletableFuture<Version> result = new CompletableFuture<>();
        synchronized (lock) {
            if (Objects.equals(this.state.getVersion(), newState.getVersion())) {
                this.state = updatedCopy(newState);
                result.complete(this.state.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.state == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(state);
        }
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long key, StreamCutRecord tData) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            streamCutRecords.putIfAbsent(key, new VersionedMetadata<>(tData, new Version.IntVersion(0)));
        }
        return CompletableFuture.completedFuture(null);

    }

    @Override
    CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime) {
        synchronized (lock) {
            if (!this.streamCutRecords.containsKey(recordingTime)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(streamCutRecords.get(recordingTime));
        }
    }

    @Override
    CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime) {
        synchronized (lock) {
            this.streamCutRecords.remove(recordingTime);

            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data) {
        Preconditions.checkNotNull(data);

        VersionedMetadata<HistoryTimeSeries> copy = new VersionedMetadata<>(data, new Version.IntVersion(0));
        synchronized (lock) {
            historyTimeSeries.putIfAbsent(chunkNumber, copy);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        synchronized (lock) {
            if (!this.historyTimeSeries.containsKey(chunkNumber)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(historyTimeSeries.get(chunkNumber));
        }
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int historyChunk, VersionedMetadata<HistoryTimeSeries> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getObject());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<HistoryTimeSeries> copy = updatedCopy(updated);
        synchronized (lock) {
            if (!historyTimeSeries.containsKey(historyChunk)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "History timeseries chunk for stream: " + getName()));
            } else if (historyTimeSeries.get(historyChunk).getVersion().equals(updated.getVersion())) {
                this.historyTimeSeries.put(historyChunk, copy);
                result.complete(copy.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "History time series for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                this.currentEpochRecord = new VersionedMetadata<>(data, new Version.IntVersion(0));
            }
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getObject());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<EpochRecord> copy = updatedCopy(updated);
        synchronized (lock) {
            if (currentEpochRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "current epoch record for stream: " + getName()));
            } else if (currentEpochRecord.getVersion().equals(updated.getVersion())) {
                this.currentEpochRecord = copy;
                result.complete(copy.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "current epoch record for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(this.currentEpochRecord);
        }
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.epochRecords.putIfAbsent(epoch, new VersionedMetadata<>(data, new Version.IntVersion(0)));
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch) {
        synchronized (lock) {
            if (!this.epochRecords.containsKey(epoch)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(this.epochRecords.get(epoch));
        }
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shardNumber, SealedSegmentsMapShard data) {
        Preconditions.checkNotNull(data);

        VersionedMetadata<SealedSegmentsMapShard> copy = new VersionedMetadata<>(data, new Version.IntVersion(0));
        synchronized (lock) {
            sealedSegmentsShards.putIfAbsent(shardNumber, copy);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(int shard) {
        synchronized (lock) {
            if (!this.sealedSegmentsShards.containsKey(shard)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(sealedSegmentsShards.get(shard));
        }
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, VersionedMetadata<SealedSegmentsMapShard> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getObject());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<SealedSegmentsMapShard> copy = updatedCopy(updated);
        synchronized (lock) {
            if (!sealedSegmentsShards.containsKey(shard)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "sealed segment size map shard for stream: " + getName()));
            } else if (sealedSegmentsShards.get(shard).getVersion().equals(updated.getVersion())) {
                this.sealedSegmentsShards.put(shard, copy);
                result.complete(copy.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "History time series for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createSegmentSealedEpochRecords(Collection<Long> segmentToSeal, int epoch) {
        return Futures.allOf(segmentToSeal.stream().map(x -> createSegmentSealedEpochRecordData(x, epoch)).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> createSegmentSealedEpochRecordData(long segment, int epoch) {
        Preconditions.checkNotNull(epoch);

        synchronized (lock) {
            segmentSealingEpochs.putIfAbsent(segment, new VersionedMetadata<>(epoch, new Version.IntVersion(0)));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId) {
        synchronized (lock) {
            if (!this.segmentSealingEpochs.containsKey(segmentId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(segmentSealingEpochs.get(segmentId));
        }
    }

    @Override
    CompletableFuture<Version> createNewTransaction(int epoch, UUID txId, ActiveTxnRecord data) {
        Preconditions.checkNotNull(txId);

        final CompletableFuture<Version> result = new CompletableFuture<>();
        final VersionedMetadata<ActiveTxnRecord> txnData = new VersionedMetadata<>(data, new Version.IntVersion(0));

        synchronized (txnsLock) {
            activeTxns.putIfAbsent(txId, txnData);
            epochTxnMap.compute(epoch, (x, y) -> {
                if (y == null) {
                    y = new HashSet<>();
                }
                y.add(txId);
                return y;
            });
            result.complete(new Version.IntVersion(0));
        }

        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(int epoch, UUID txId) {
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            }

            return CompletableFuture.completedFuture(activeTxns.get(txId));
        }
    }

    @Override
    CompletableFuture<Version> updateActiveTx(int epoch, UUID txId, VersionedMetadata<ActiveTxnRecord> data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<ActiveTxnRecord> updatedCopy = updatedCopy(data);
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            } else {
                activeTxns.compute(txId, (x, y) -> {
                    if (data.getVersion().equals(y.getVersion())) {
                        result.complete(updatedCopy.getVersion());
                        return updatedCopy;
                    } else {
                        result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                                "Stream: " + getName() + " transaction id : " + txId));
                        return y;
                    }
                });
                result.complete(activeTxns.get(txId).getVersion());
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Long> addTxnToCommitOrder(UUID txId) {
        long orderedPosition = counter.getAndIncrement();
        transactionCommitOrder.put(orderedPosition, txId);
        return CompletableFuture.completedFuture(orderedPosition);
    }

    @Override
    CompletableFuture<Void> removeTxnsFromCommitOrder(List<Long> positions) {
        positions.forEach(transactionCommitOrder::remove);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<VersionedMetadata<CompletedTxnRecord>> getCompletedTx(UUID txId) {
        Preconditions.checkNotNull(txId);
        synchronized (txnsLock) {
            VersionedMetadata<CompletedTxnRecord> value = completedTxns.getIfPresent(txId);
            if (value == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            }
            return CompletableFuture.completedFuture(value);
        }
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(int epoch, UUID txId) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            activeTxns.remove(txId);
            epochTxnMap.computeIfPresent(epoch, (x, y) -> {
                y.remove(txId);
                return y;
            });

            if (epochTxnMap.get(epoch).isEmpty()) {
                epochTxnMap.remove(epoch);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, CompletedTxnRecord complete) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            VersionedMetadata<CompletedTxnRecord> value = completedTxns.getIfPresent(txId);
            if (value == null) {
                completedTxns.put(txId, new VersionedMetadata<>(complete, new Version.IntVersion(0)));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        synchronized (markersLock) {
            markers.putIfAbsent(segmentId, new VersionedMetadata<>(timestamp, new Version.IntVersion(0)));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data) {
        CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<Long> next = updatedCopy(data);
        synchronized (markersLock) {
            if (!markers.containsKey(segmentId)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Segment number: " + segmentId));
            } else {
                markers.compute(segmentId, (x, y) -> {
                    if (y.getVersion().equals(data.getVersion())) {
                        result.complete(next.getVersion());
                        return next;
                    } else {
                        result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                                "Stream: " + getName() + " Segment number: " + segmentId));
                        return y;
                    }
                });
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> removeMarkerData(long segmentId) {
        synchronized (markersLock) {
            markers.remove(segmentId);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId) {
        synchronized (markersLock) {
            if (!markers.containsKey(segmentId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Segment: " + segmentId));
            }
            return CompletableFuture.completedFuture(markers.get(segmentId));
        }
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
        synchronized (txnsLock) {
            return CompletableFuture.completedFuture(Collections.unmodifiableMap(
                    activeTxns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getObject()))));
        }
    }

    @Override
    CompletableFuture<List<Map.Entry<UUID, ActiveTxnRecord>>> getOrderedCommittingTxnInLowestEpoch(int limit) {
        List<Long> toPurge = new ArrayList<>();
        Map<UUID, ActiveTxnRecord> committing = new HashMap<>();
        AtomicInteger smallestEpoch = new AtomicInteger(Integer.MAX_VALUE);
        // take smallest epoch and collect transactions from smallest epoch.
        transactionCommitOrder
                .forEach((order, txId) -> {
                    int epoch = RecordHelper.getTransactionEpoch(txId);
                    ActiveTxnRecord record;
                    synchronized (txnsLock) {
                        record = activeTxns.containsKey(txId) ? activeTxns.get(txId).getObject() :
                                ActiveTxnRecord.EMPTY;
                    }
                    switch (record.getTxnStatus()) {
                        case COMMITTING:
                            if (record.getCommitOrder() == order) {
                                // if entry matches record's position then include it
                                committing.put(txId, record);
                                if (smallestEpoch.get() > epoch) {
                                    smallestEpoch.set(epoch);
                                }
                            } else {
                                toPurge.add(order);
                            }
                            break;
                        case OPEN:  // do nothing
                            break;
                        case COMMITTED:
                        case ABORTING:
                        case ABORTED:
                        case UNKNOWN:
                            // Aborting, aborted, unknown and committed 
                            toPurge.add(order);
                            break;
                    }
                });
        // remove all stale transactions from transactionCommitOrder 
        toPurge.forEach(transactionCommitOrder::remove);

        // take smallest epoch from committing transactions. order transactions in this epoch by 
        // ordered position
        List<Map.Entry<UUID, ActiveTxnRecord>> list = committing.entrySet().stream().filter(x -> RecordHelper.getTransactionEpoch(x.getKey()) == smallestEpoch.get())
                                                                .sorted(Comparator.comparing(x -> x.getValue().getCommitOrder()))
                                                                .limit(limit)
                                                                .collect(Collectors.toList());

        return CompletableFuture.completedFuture(list);
    }

    @Override
    CompletableFuture<Map<Long, UUID>> getAllOrderedCommittingTxns() {
        synchronized (txnsLock) {
            return CompletableFuture.completedFuture(Collections.unmodifiableMap(transactionCommitOrder));
        }
    }

    @Override
    CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnInEpoch(int epoch) {
        synchronized (txnsLock) {
            Set<UUID> transactions = epochTxnMap.get(epoch);
            Map<UUID, ActiveTxnRecord> map;
            if (transactions != null) {
                map = activeTxns.entrySet().stream().filter(x -> transactions.contains(x.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getObject()));
                map = Collections.unmodifiableMap(map);
            } else {
                map = Collections.emptyMap();
            }
            return CompletableFuture.completedFuture(map);
        }
    }
     
    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet retention) {
        Preconditions.checkNotNull(retention);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.retentionSet = new VersionedMetadata<>(retention, new Version.IntVersion(0));
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData() {
        CompletableFuture<VersionedMetadata<RetentionSet>> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.retentionSet == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                result.complete(retentionSet);
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> addSubscriber(String subscriber, long generation) {
        synchronized (subscribersLock) {
            Optional<VersionedMetadata<StreamSubscriber>> foundSubscriber = streamSubscribers.stream()
                    .filter(sub -> sub.getObject().getSubscriber().equals(subscriber)).findAny();
            if (foundSubscriber.isEmpty()) {
                streamSubscribers.add(new VersionedMetadata<>(new StreamSubscriber(subscriber, generation, ImmutableMap.of(),
                        System.currentTimeMillis()), new Version.IntVersion(0)));
            } else {
                if (foundSubscriber.get().getObject().getGeneration() < generation) {
                    setSubscriberData(new VersionedMetadata<>(new StreamSubscriber(subscriber, generation,
                            foundSubscriber.get().getObject().getTruncationStreamCut(),
                            System.currentTimeMillis()), foundSubscriber.get().getVersion()));
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteSubscriber(final String subscriber, final long generation) {
        synchronized (subscribersLock) {
            Optional<VersionedMetadata<StreamSubscriber>> foundSubscriber = streamSubscribers.stream()
                    .filter(sub -> sub.getObject().getSubscriber().equals(subscriber)).findAny();
            if (foundSubscriber.isPresent() && generation >= foundSubscriber.get().getObject().getGeneration()) {
                streamSubscribers.remove(foundSubscriber.get());
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriberRecord(String subscriber) {
        CompletableFuture<VersionedMetadata<StreamSubscriber>> result = new CompletableFuture<>();
        synchronized (subscribersLock) {
            Optional<VersionedMetadata<StreamSubscriber>> existingSubscriber = streamSubscribers.stream()
                    .filter(s2 -> s2.getObject().getSubscriber().equals(subscriber)).findFirst();
            if (existingSubscriber.isEmpty()) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "subscriber not found"));
            } else {
                result.complete(existingSubscriber.get());
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers() {
        List<String> result;
        synchronized (subscribersLock) {
            result = streamSubscribers.stream().map(s -> s.getObject().getSubscriber()).collect(Collectors.toList());
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Version> setSubscriberData(final VersionedMetadata<StreamSubscriber> subscriberData) {
        VersionedMetadata<StreamSubscriber> updatedSubscriber = updatedCopy(subscriberData);
        synchronized (subscribersLock) {
            Optional<VersionedMetadata<StreamSubscriber>> previousSubscriber = streamSubscribers.stream()
                    .filter(s -> s.getObject().getSubscriber().equals(subscriberData.getObject().getSubscriber())).findAny();
            if (previousSubscriber.isPresent()) {
                streamSubscribers.remove(previousSubscriber.get());
            }
            streamSubscribers.add(updatedSubscriber);
        }
        return CompletableFuture.completedFuture(updatedSubscriber.getVersion());
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> retention) {
        Preconditions.checkNotNull(retention);
        Preconditions.checkNotNull(retention.getObject());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<RetentionSet> next = updatedCopy(retention);
        synchronized (lock) {
            if (retentionSet == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "retentionSet for stream: " + getName()));
            } else if (retentionSet.getVersion().equals(retention.getVersion())) {
                this.retentionSet = next;
                result.complete(next.getVersion());
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "retentionSet for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransitionData) {
        Preconditions.checkNotNull(epochTransitionData);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.epochTransition == null) {
                this.epochTransition = new VersionedMetadata<>(epochTransitionData, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> record) {
        Preconditions.checkNotNull(record);

        CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<EpochTransitionRecord> updatedCopy = updatedCopy(record);
        synchronized (lock) {
            if (this.epochTransition == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "epoch transition not found"));
            } else if (!Objects.equals(this.epochTransition.getVersion(), record.getVersion())) {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, "epoch transition version mismatch"));
            } else {
                this.epochTransition = updatedCopy;
                result.complete(epochTransition.getVersion());
            }
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode() {
        CompletableFuture<VersionedMetadata<EpochTransitionRecord>> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.epochTransition == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "epoch transition not found"));
            } else {
                result.complete(epochTransition);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns) {
        Preconditions.checkNotNull(committingTxns);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.committingTxnRecord == null) {
                this.committingTxnRecord = new VersionedMetadata<>(committingTxns, new Version.IntVersion(0));
            }
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord() {
        CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.committingTxnRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "committing transactions not found"));
            } else {
                result.complete(committingTxnRecord);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> record) {
        Preconditions.checkNotNull(record);

        CompletableFuture<Version> result = new CompletableFuture<>();
        VersionedMetadata<CommittingTransactionsRecord> updatedCopy = updatedCopy(record);
        synchronized (lock) {
            if (this.committingTxnRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "committing txn not found"));
            } else if (!Objects.equals(this.committingTxnRecord.getVersion(), record.getVersion())) {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, "committing txn version mismatch"));
            } else {
                this.committingTxnRecord = updatedCopy;
                result.complete(committingTxnRecord.getVersion());
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String data) {
        synchronized (lock) {
            if (waitingRequestNode == null) {
                waitingRequestNode = data;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<String> getWaitingRequestNode() {
        CompletableFuture<String> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.waitingRequestNode == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "waiting request node not found"));
            } else {
                result.complete(waitingRequestNode);
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteWaitingRequestNode() {
        synchronized (lock) {
            this.waitingRequestNode = null;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position) {
        WriterMark mark = new WriterMark(timestamp, position);

        synchronized (writersLock) {
            VersionedMetadata<WriterMark> existing = writerMarks.get(writer);
            if (existing != null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_EXISTS, "writer mark exists"));
            } else {
                writerMarks.put(writer, new VersionedMetadata<>(mark, new Version.IntVersion(0)));
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    @Override
    public CompletableFuture<Void> removeWriterRecord(String writer, Version version) {
        synchronized (writersLock) {
            VersionedMetadata<WriterMark> existing = writerMarks.get(writer);
            if (existing != null && !Objects.equals(existing.getVersion(), version)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.WRITE_CONFLICT, "writer mark version mismatch"));
            } else {
                writerMarks.remove(writer);
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    @Override
    CompletableFuture<VersionedMetadata<WriterMark>> getWriterMarkRecord(String writer) {
        CompletableFuture<VersionedMetadata<WriterMark>> result = new CompletableFuture<>();
        
        synchronized (writersLock) {
            VersionedMetadata<WriterMark> mark = writerMarks.get(writer);
            if (mark == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "writer mark not found"));
            } else {
                result.complete(mark);
            }
        }
        
        return result;
    }

    @Override
    public CompletableFuture<Map<String, WriterMark>> getAllWriterMarks() {
        Map<String, WriterMark> result;
        synchronized (writersLock) {
            result = writerMarks.entrySet().stream().collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().getObject()));
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    CompletableFuture<Void> updateWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position, 
                                                   boolean isAlive, Version version) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        VersionedMetadata<WriterMark> updatedCopy = updatedCopy(new VersionedMetadata<>(new WriterMark(timestamp, position, isAlive), version));
        synchronized (writersLock) {
            VersionedMetadata<WriterMark> existing = writerMarks.get(writer);
            if (existing == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "writer mark not found"));
            } else if (!Objects.equals(existing.getVersion(), version)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, "writer mark version mismatch"));
            } else {
                this.writerMarks.put(writer, updatedCopy);
                result.complete(null);
            }
        }
        return result;
    }

    private <T> VersionedMetadata<T> updatedCopy(VersionedMetadata<T> input) {
        return new VersionedMetadata<>(input.getObject(), new Version.IntVersion(input.getVersion().asIntVersion().getIntValue() + 1));
    }
}
