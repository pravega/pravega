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
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.util.Config;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class InMemoryStream extends PersistentStreamBase {

    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private Data configuration;
    @GuardedBy("lock")
    private Data truncationRecord;
    @GuardedBy("lock")
    private Data state;
    @GuardedBy("lock")
    private Data currentEpochRecord;
    @GuardedBy("lock")
    private Map<Integer, Data> epochRecords = new HashMap<>();
    @GuardedBy("lock")
    private Map<Integer, Data> historyTimeSeries = new HashMap<>();
    @GuardedBy("lock")
    private Data retentionSet;;
    @GuardedBy("lock")
    private final Map<Long, Data> streamCutRecords = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Integer, Data> sealedSegmentsShards = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Long, Data> segmentSealingEpochs = new HashMap<>();
    @GuardedBy("lock")
    private Data epochTransition;
    @GuardedBy("lock")
    private Data committingTxnRecord;
    @GuardedBy("lock")
    private Data waitingRequestNode;

    private final Object txnsLock = new Object();
    @GuardedBy("txnsLock")
    private final Map<String, Data> activeTxns = new HashMap<>();
    @GuardedBy("txnsLock")
    private final Cache<String, Data> completedTxns;
    private final Object markersLock = new Object();
    @GuardedBy("markersLock")
    private final Map<Long, Data> markers = new HashMap<>();
    /**
     * This is used to guard updates to values in epoch txn map.
     * This ensures that we remove an epoch node if an only if there are no transactions against that epoch.
     * Note: there can be only two epochs at max concurrently. So using one lock for both of their updates is okay.
     */
    @GuardedBy("txnsLock")
    private final Map<Integer, Set<String>> epochTxnMap = new HashMap<>();

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
        final Data currentState;
        synchronized (lock) {
            time = creationTime.get();
            config = this.configuration == null ? null : StreamConfigurationRecord.fromBytes(this.configuration.getData());
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

    private void handleStreamMetadataExists(final long timestamp, CompletableFuture<CreateStreamResponse> result, final long time,
                                            final int startingSegmentNumber, final StreamConfiguration config, Data currentState) {
        if (currentState != null) {
            State stateVal = StateRecord.fromBytes(currentState.getData()).getState();
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
    CompletableFuture<Void> createConfigurationIfAbsent(byte[] config) {
        Preconditions.checkNotNull(config);

        synchronized (lock) {
            if (configuration == null) {
                configuration = new Data(config, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createTruncationDataIfAbsent(byte[] truncation) {
        Preconditions.checkNotNull(truncation);

        synchronized (lock) {
            if (truncationRecord == null) {
                truncationRecord = new Data(truncation, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> setConfigurationData(Data newConfig) {
        Preconditions.checkNotNull(newConfig);

        CompletableFuture<Version> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.configuration == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                if (Objects.equals(this.configuration.getVersion(), newConfig.getVersion())) {
                    this.configuration = updatedCopy(new Data(newConfig.getData(), this.configuration.getVersion()));
                    result.complete(this.configuration.getVersion());
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
                }
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data> getConfigurationData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.configuration == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(this.configuration));
        }
    }

    @Override
    CompletableFuture<Version> setTruncationData(Data truncationRecord) {
        Preconditions.checkNotNull(truncationRecord);

        CompletableFuture<Version> result = new CompletableFuture<>();
        Data updatedCopy = updatedCopy(truncationRecord);
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
    CompletableFuture<Data> getTruncationData(boolean ignoreCached) {
        synchronized (lock) {
            return CompletableFuture.completedFuture(copy(this.truncationRecord));
        }
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(byte[] state) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            if (this.state == null) {
                this.state = new Data(state, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> setStateData(Data newState) {
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
    CompletableFuture<Data> getStateData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.state == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(state));
        }
    }

    @Override
    CompletableFuture<Void> createStreamCutRecordData(long key, byte[] tData) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            streamCutRecords.putIfAbsent(key, new Data(tData, new Version.IntVersion(0)));
        }
        return CompletableFuture.completedFuture(null);

    }

    @Override
    CompletableFuture<Data> getStreamCutRecordData(long recordingTime) {
        synchronized (lock) {
            if (!this.streamCutRecords.containsKey(recordingTime)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(streamCutRecords.get(recordingTime)));
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
    CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, byte[] data) {
        Preconditions.checkNotNull(data);

        Data copy = new Data(Arrays.copyOf(data, data.length), new Version.IntVersion(0));
        synchronized (lock) {
            historyTimeSeries.putIfAbsent(chunkNumber, copy);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached) {
        synchronized (lock) {
            if (!this.historyTimeSeries.containsKey(chunkNumber)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(historyTimeSeries.get(chunkNumber)));
        }
    }

    @Override
    CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int historyChunk, Data updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        Data copy = updatedCopy(updated);
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
    CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(byte[] data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                this.currentEpochRecord = new Data(data, new Version.IntVersion(0));
            }
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Version> updateCurrentEpochRecordData(Data updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        Data copy = updatedCopy(updated);
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
    CompletableFuture<Data> getCurrentEpochRecordData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.currentEpochRecord == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(this.currentEpochRecord));
        }
    }

    @Override
    CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, byte[] data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.epochRecords.putIfAbsent(epoch, new Data(data, new Version.IntVersion(0)));
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Data> getEpochRecordData(int epoch) {
        synchronized (lock) {
            if (!this.epochRecords.containsKey(epoch)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(this.epochRecords.get(epoch)));
        }
    }

    @Override
    CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shardNumber, byte[] data) {
        Preconditions.checkNotNull(data);

        Data copy = new Data(Arrays.copyOf(data, data.length), new Version.IntVersion(0));
        synchronized (lock) {
            sealedSegmentsShards.putIfAbsent(shardNumber, copy);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data> getSealedSegmentSizesMapShardData(int shard) {
        synchronized (lock) {
            if (!this.sealedSegmentsShards.containsKey(shard)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(sealedSegmentsShards.get(shard)));
        }
    }

    @Override
    CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, Data updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        Data copy = updatedCopy(updated);
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
    CompletableFuture<Void> createSegmentSealedEpochRecordData(long segment, int epoch) {
        Preconditions.checkNotNull(epoch);
        byte[] array = new byte[Integer.BYTES];
        BitConverter.writeInt(array, 0, epoch);

        synchronized (lock) {
            segmentSealingEpochs.putIfAbsent(segment, new Data(array, new Version.IntVersion(0)));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data> getSegmentSealedRecordData(long segmentId) {
        synchronized (lock) {
            if (!this.segmentSealingEpochs.containsKey(segmentId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(segmentSealingEpochs.get(segmentId)));
        }
    }

    @Override
    CompletableFuture<Version> createNewTransaction(int epoch, UUID txId, byte[] data) {
        Preconditions.checkNotNull(txId);

        final CompletableFuture<Version> result = new CompletableFuture<>();
        final Data txnData = new Data(Arrays.copyOf(data, data.length), new Version.IntVersion(0));

        synchronized (txnsLock) {
            activeTxns.putIfAbsent(txId.toString(), txnData);
            epochTxnMap.compute(epoch, (x, y) -> {
                if (y == null) {
                    y = new HashSet<>();
                }
                y.add(txId.toString());
                return y;
            });
            result.complete(new Version.IntVersion(0));
        }

        return result;
    }

    @Override
    CompletableFuture<Data> getActiveTx(int epoch, UUID txId) {
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            }

            return CompletableFuture.completedFuture(copy(activeTxns.get(txId.toString())));
        }
    }

    @Override
    CompletableFuture<Version> updateActiveTx(int epoch, UUID txId, Data data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Version> result = new CompletableFuture<>();
        Data updatedCopy = updatedCopy(data);
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            } else {
                activeTxns.compute(txId.toString(), (x, y) -> {
                    if (data.getVersion().equals(y.getVersion())) {
                        result.complete(updatedCopy.getVersion());
                        return updatedCopy;
                    } else {
                        result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                                "Stream: " + getName() + " transaction id : " + txId));
                        return y;
                    }
                });
                result.complete(activeTxns.get(txId.toString()).getVersion());
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Data> getCompletedTx(UUID txId) {
        Preconditions.checkNotNull(txId);
        synchronized (txnsLock) {
            Data value = completedTxns.getIfPresent(txId.toString());
            if (value == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            }
            return CompletableFuture.completedFuture(copy(value));
        }
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(int epoch, UUID txId) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            activeTxns.remove(txId.toString());
            epochTxnMap.computeIfPresent(epoch, (x, y) -> {
                y.remove(txId.toString());
                return y;
            });

            if (epochTxnMap.get(epoch).isEmpty()) {
                epochTxnMap.remove(epoch);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, byte[] complete) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            Data value = completedTxns.getIfPresent(txId.toString());
            if (value == null) {
                completedTxns.put(txId.toString(), new Data(complete, new Version.IntVersion(0)));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);
        synchronized (markersLock) {
            markers.putIfAbsent(segmentId, new Data(b, new Version.IntVersion(0)));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> updateMarkerData(long segmentId, Data data) {
        CompletableFuture<Version> result = new CompletableFuture<>();
        Data next = updatedCopy(data);
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
    CompletableFuture<Data> getMarkerData(long segmentId) {
        synchronized (markersLock) {
            if (!markers.containsKey(segmentId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Segment: " + segmentId));
            }
            return CompletableFuture.completedFuture(copy(markers.get(segmentId)));
        }
    }

    @Override
    CompletableFuture<Map<String, Data>> getCurrentTxns() {
        synchronized (txnsLock) {
            Map<String, Data> map = activeTxns.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, x -> copy(x.getValue())));
            return CompletableFuture.completedFuture(Collections.unmodifiableMap(map));
        }
    }

    @Override
    CompletableFuture<Map<String, Data>> getTxnInEpoch(int epoch) {
        synchronized (txnsLock) {
            Set<String> transactions = epochTxnMap.get(epoch);
            Map<String, Data> map;
            if (transactions != null) {
                map = activeTxns.entrySet().stream().filter(x -> transactions.contains(x.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, x -> copy(x.getValue())));
                map = Collections.unmodifiableMap(map);
            } else {
                map = Collections.emptyMap();
            }
            return CompletableFuture.completedFuture(map);
        }
    }

    @Override
    CompletableFuture<Void> checkScopeExists() throws StoreException {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createRetentionSetDataIfAbsent(byte[] retention) {
        Preconditions.checkNotNull(retention);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.retentionSet = new Data(retention, new Version.IntVersion(0));
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Data> getRetentionSetData() {
        CompletableFuture<Data> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.retentionSet == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                result.complete(copy(retentionSet));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Version> updateRetentionSetData(Data retention) {
        Preconditions.checkNotNull(retention);
        Preconditions.checkNotNull(retention.getData());

        final CompletableFuture<Version> result = new CompletableFuture<>();
        Data next = updatedCopy(retention);
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
    CompletableFuture<Void> createEpochTransitionIfAbsent(byte[] epochTransitionData) {
        Preconditions.checkNotNull(epochTransitionData);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.epochTransition == null) {
                this.epochTransition = new Data(epochTransitionData, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Version> updateEpochTransitionNode(Data record) {
        Preconditions.checkNotNull(record);

        CompletableFuture<Version> result = new CompletableFuture<>();
        Data updatedCopy = updatedCopy(record);
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
    CompletableFuture<Data> getEpochTransitionNode() {
        CompletableFuture<Data> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.epochTransition == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "epoch transition not found"));
            } else {
                result.complete(copy(epochTransition));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createCommitTxnRecordIfAbsent(byte[] committingTxns) {
        Preconditions.checkNotNull(committingTxns);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.committingTxnRecord == null) {
                this.committingTxnRecord = new Data(Arrays.copyOf(committingTxns, committingTxns.length), new Version.IntVersion(0));
            }
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Data> getCommitTxnRecord() {
        CompletableFuture<Data> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.committingTxnRecord == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "committing transactions not found"));
            } else {
                result.complete(copy(committingTxnRecord));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Version> updateCommittingTxnRecord(Data record) {
        Preconditions.checkNotNull(record);

        CompletableFuture<Version> result = new CompletableFuture<>();
        Data updatedCopy = updatedCopy(record);
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
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] data) {
        synchronized (lock) {
            if (waitingRequestNode == null) {
                waitingRequestNode = new Data(data, new Version.IntVersion(0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data> getWaitingRequestNode() {
        CompletableFuture<Data> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.waitingRequestNode == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "waiting request node not found"));
            } else {
                result.complete(copy(waitingRequestNode));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> deleteWaitingRequestNode() {
        synchronized (lock) {
            this.waitingRequestNode = null;
        }
        return CompletableFuture.completedFuture(null);
    }

    private Data copy(Data input) {
        return new Data(Arrays.copyOf(input.getData(), input.getData().length), input.getVersion());
    }

    private Data updatedCopy(Data input) {
        return new Data(Arrays.copyOf(input.getData(), input.getData().length), new Version.IntVersion(input.getVersion().asIntVersion().getIntValue() + 1));
    }
}
