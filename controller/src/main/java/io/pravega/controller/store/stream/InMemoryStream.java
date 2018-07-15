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
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StateRecord;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
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

public class InMemoryStream extends PersistentStreamBase<Integer> {

    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private Data<Integer> configuration;
    @GuardedBy("lock")
    private Data<Integer> truncationRecord;
    @GuardedBy("lock")
    private Data<Integer> state;
    @GuardedBy("lock")
    private Data<Integer> segmentTable;
    @GuardedBy("lock")
    private Data<Integer> segmentIndex;
    @GuardedBy("lock")
    private Data<Integer> historyTable;
    @GuardedBy("lock")
    private Data<Integer> historyIndex;
    @GuardedBy("lock")
    private Data<Integer> retentionSet;
    @GuardedBy("lock")
    private Data<Integer> sealedSegments;
    @GuardedBy("lock")
    private Data<Integer> epochTransition;
    @GuardedBy("lock")
    private Data<Integer> committingTxnRecord;
    @GuardedBy("lock")
    private Data<Integer> waitingRequestNode;

    private final Object txnsLock = new Object();
    @GuardedBy("txnsLock")
    private final Map<String, Data<Integer>> activeTxns = new HashMap<>();
    @GuardedBy("txnsLock")
    private final Cache<String, Data<Integer>> completedTxns;
    private final Object markersLock = new Object();
    @GuardedBy("markersLock")
    private final Map<Long, Data<Integer>> markers = new HashMap<>();
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
    InMemoryStream(String scope, String name, long completedTxnTTL) {
        super(scope, name);
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
    CompletableFuture<CreateStreamResponse> checkStreamExists(StreamConfiguration configuration, long timestamp) {
        CompletableFuture<CreateStreamResponse> result = new CompletableFuture<>();

        final long time;
        final StreamConfigurationRecord config;
        final Data<Integer> currentState;
        synchronized (lock) {
            time = creationTime.get();
            config = this.configuration == null ? null : StreamConfigurationRecord.parse(this.configuration.getData());
            currentState = this.state;
        }

        if (time != Long.MIN_VALUE) {
            if (config != null) {
                handleStreamMetadataExists(timestamp, result, time, config.getStreamConfiguration(), currentState);
            } else {
                result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW, configuration, time));
            }
        } else {
            result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.NEW, configuration, timestamp));
        }

        return result;
    }

    private void handleStreamMetadataExists(final long timestamp, CompletableFuture<CreateStreamResponse> result, final long time,
                                            final StreamConfiguration config, Data<Integer> currentState) {
        if (currentState != null) {
            State stateVal = StateRecord.parse(currentState.getData()).getState();
            if (stateVal.equals(State.UNKNOWN) || stateVal.equals(State.CREATING)) {
                CreateStreamResponse.CreateStatus status;
                status = (time == timestamp) ? CreateStreamResponse.CreateStatus.NEW :
                        CreateStreamResponse.CreateStatus.EXISTS_CREATING;
                result.complete(new CreateStreamResponse(status, config, time));
            } else {
                result.complete(new CreateStreamResponse(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, config, time));
            }
        } else {
            CreateStreamResponse.CreateStatus status = (time == timestamp) ? CreateStreamResponse.CreateStatus.NEW :
                    CreateStreamResponse.CreateStatus.EXISTS_CREATING;

            result.complete(new CreateStreamResponse(status, config, time));
        }
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long timestamp) {
        creationTime.compareAndSet(Long.MIN_VALUE, timestamp);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(StreamConfigurationRecord config) {
        Preconditions.checkNotNull(config);

        synchronized (lock) {
            if (configuration == null) {
                configuration = new Data<>(config.toByteArray(), 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createTruncationDataIfAbsent(StreamTruncationRecord truncation) {
        Preconditions.checkNotNull(truncation);

        synchronized (lock) {
            if (truncationRecord == null) {
                truncationRecord = new Data<>(truncation.toByteArray(), 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> setConfigurationData(Data<Integer> newConfig) {
        Preconditions.checkNotNull(newConfig);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.configuration == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                if (Objects.equals(this.configuration.getVersion(), newConfig.getVersion())) {
                    this.configuration = new Data<>(newConfig.getData(), this.configuration.getVersion() + 1);
                    result.complete(null);
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
                }

                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getConfigurationData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.configuration == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(this.configuration));
        }
    }

    @Override
    CompletableFuture<Void> setTruncationData(Data<Integer> truncationRecord) {
        Preconditions.checkNotNull(truncationRecord);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.truncationRecord == null) {
                this.truncationRecord = new Data<>(truncationRecord.getData(), 0);
            } else {
                if (Objects.equals(this.truncationRecord.getVersion(), truncationRecord.getVersion())) {
                    this.truncationRecord = new Data<>(truncationRecord.getData(), this.truncationRecord.getVersion() + 1);
                    result.complete(null);
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
                }

                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getTruncationData(boolean ignoreCached) {
        synchronized (lock) {
            return CompletableFuture.completedFuture(copy(this.truncationRecord));
        }
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(State state) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            if (this.state == null) {
                this.state = new Data<>(StateRecord.builder().state(state).build().toByteArray(), 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> setStateData(Data<Integer> newState) {
        Preconditions.checkNotNull(newState);

        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (lock) {
            if (Objects.equals(this.state.getVersion(), newState.getVersion())) {
                this.state = new Data<>(newState.getData(), newState.getVersion() + 1);
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, getName()));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getStateData(boolean ignoreCached) {
        synchronized (lock) {
            if (this.state == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(state));
        }
    }

    @Override
    CompletableFuture<Void> createSegmentIndexIfAbsent(Data<Integer> data) {
        synchronized (lock) {
            if (segmentIndex == null) {
                segmentIndex = new Data<>(data.getData(), 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentIndex() {
        synchronized (lock) {
            if (this.segmentIndex == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(this.segmentIndex));
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentIndexFromStore() {
        return getSegmentIndex();
    }

    @Override
    CompletableFuture<Void> updateSegmentIndex(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(data);
        synchronized (lock) {
            if (segmentIndex == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Segment index for stream: " + getName()));
            } else if (segmentIndex.getVersion().equals(data.getVersion())) {
                segmentIndex = copy;
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "Segment index for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createSegmentTableIfAbsent(final Data<Integer> data) {
        synchronized (lock) {
            if (segmentTable == null) {
                segmentTable = new Data<>(data.getData(), 0);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentTable() {
        synchronized (lock) {
            if (this.segmentTable == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(this.segmentTable));
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentTableFromStore() {
        return getSegmentTable();
    }

    @Override
    CompletableFuture<Void> updateSegmentTable(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(data);
        synchronized (lock) {
            if (segmentTable == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Segment table for stream: " + getName()));
            } else if (segmentTable.getVersion().equals(data.getVersion())) {
                segmentTable = copy;
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "Segment table for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createHistoryIndexIfAbsent(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        Data<Integer> copy = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0);
        synchronized (lock) {
            if (historyIndex == null) {
                this.historyIndex = copy;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryIndex() {
        synchronized (lock) {
            if (this.historyIndex == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(historyIndex));
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryIndexFromStore() {
        return getHistoryIndex();
    }

    @Override
    CompletableFuture<Void> updateHistoryIndex(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Void> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(updated);
        synchronized (lock) {
            if (historyIndex == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Indextable for stream: " + getName()));
            } else if (historyIndex.getVersion().equals(updated.getVersion())) {
                this.historyIndex = copy;
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "Indextable for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createHistoryTableIfAbsent(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        Data<Integer> copy = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0);
        synchronized (lock) {
            if (historyTable == null) {
                this.historyTable = copy;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateHistoryTable(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();
        Data<Integer> copy = updatedCopy(updated);
        synchronized (lock) {
            if (historyTable == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Historytable for stream: " + getName()));
            } else {
                if (historyTable.getVersion().equals(updated.getVersion())) {
                    this.historyTable = copy;
                    result.complete(null);
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                            "Historytable for stream: " + getName()));
                }
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTable() {
        synchronized (lock) {
            if (this.historyTable == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(historyTable));
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTableFromStore() {
        return getHistoryTable();
    }

    @Override
    CompletableFuture<Void> createNewTransaction(UUID txId, long timestamp, long leaseExpiryTime, long maxExecutionExpiryTime) {
        Preconditions.checkNotNull(txId);

        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Data<Integer> txnData = new Data<>(
                new ActiveTxnRecord(timestamp, leaseExpiryTime, maxExecutionExpiryTime, TxnStatus.OPEN)
                        .toByteArray(), 0);
        int epoch = getTransactionEpoch(txId);

        synchronized (txnsLock) {
            activeTxns.putIfAbsent(txId.toString(), txnData);
            epochTxnMap.compute(epoch, (x, y) -> {
                if (y == null) {
                    y = new HashSet<>();
                }
                y.add(txId.toString());
                return y;
            });
            result.complete(null);
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(int epoch, UUID txId) {
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            }

            return CompletableFuture.completedFuture(copy(activeTxns.get(txId.toString())));
        }
    }

    @Override
    CompletableFuture<Void> updateActiveTx(int epoch, UUID txId, Data<Integer> data) {
        Preconditions.checkNotNull(data);

        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            } else {
                activeTxns.compute(txId.toString(), (x, y) -> new Data<>(data.getData(), y.getVersion() + 1));
                result.complete(null);
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Void> sealActiveTx(int epoch, UUID txId, boolean commit, ActiveTxnRecord txnRecord,
                                         int version) {
        Preconditions.checkNotNull(txId);

        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString()));
            } else {
                activeTxns.compute(txId.toString(), (x, y) -> {
                    if (version != y.getVersion()) {
                        result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                                "Stream: " + getName() + " Transaction: " + txId.toString()));
                        return y;
                    } else {
                        ActiveTxnRecord previous = ActiveTxnRecord.parse(y.getData());
                        ActiveTxnRecord updated = new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                                previous.getLeaseExpiryTime(),
                                previous.getMaxExecutionExpiryTime(),
                                commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING);
                        result.complete(null);
                        return new Data<>(updated.toByteArray(), y.getVersion() + 1);
                    }
                });
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(UUID txId) {
        Preconditions.checkNotNull(txId);
        synchronized (txnsLock) {
            Data<Integer> value = completedTxns.getIfPresent(txId.toString());
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
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, TxnStatus complete, long timestamp) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            Data<Integer> value = completedTxns.getIfPresent(txId.toString());
            if (value == null) {
                completedTxns.put(txId.toString(), new Data<>(new CompletedTxnRecord(timestamp, complete).toByteArray(), 0));
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createMarkerData(long segmentId, long timestamp) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);
        synchronized (markersLock) {
            markers.putIfAbsent(segmentId, new Data<>(b, 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateMarkerData(long segmentId, Data<Integer> data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Data<Integer> next = updatedCopy(data);
        synchronized (markersLock) {
            if (!markers.containsKey(segmentId)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Segment number: " + segmentId));
            } else {
                markers.compute(segmentId, (x, y) -> {
                    if (y.getVersion().equals(data.getVersion())) {
                        result.complete(null);
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
    CompletableFuture<Data<Integer>> getMarkerData(long segmentId) {
        synchronized (markersLock) {
            if (!markers.containsKey(segmentId)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Segment: " + segmentId));
            }
            return CompletableFuture.completedFuture(copy(markers.get(segmentId)));
        }
    }

    @Override
    CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        synchronized (txnsLock) {
            Map<String, Data<Integer>> map = activeTxns.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, x -> copy(x.getValue())));
            return CompletableFuture.completedFuture(Collections.unmodifiableMap(map));
        }
    }

    @Override
    CompletableFuture<Map<String, Data<Integer>>> getTxnInEpoch(int epoch) {
        synchronized (txnsLock) {
            Set<String> transactions = epochTxnMap.get(epoch);
            Map<String, Data<Integer>> map;
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
    CompletableFuture<Void> createRetentionSet(byte[] retention) {
        Preconditions.checkNotNull(retention);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            this.retentionSet = new Data<>(retention, 0);
            result.complete(null);
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getRetentionSet() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

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
    CompletableFuture<Void> updateRetentionSet(Data<Integer> retention) {
        Preconditions.checkNotNull(retention);
        Preconditions.checkNotNull(retention.getData());

        final CompletableFuture<Void> result = new CompletableFuture<>();
        Data<Integer> next = updatedCopy(retention);
        synchronized (lock) {
            if (retentionSet == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "retentionSet for stream: " + getName()));
            } else if (retentionSet.getVersion().equals(retention.getVersion())) {
                this.retentionSet = next;
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "retentionSet for stream: " + getName()));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createEpochTransitionNode(byte[] epochTransitionData) {
        Preconditions.checkNotNull(epochTransitionData);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.epochTransition != null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_EXISTS, "epoch transition exists"));
            } else {
                this.epochTransition = new Data<>(epochTransitionData, 0);
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> updateEpochTransitionNode(byte[] epochTransitionData) {
        Preconditions.checkNotNull(epochTransitionData);

        CompletableFuture<Void> result = new CompletableFuture<>();
        byte[] copy = Arrays.copyOf(epochTransitionData, epochTransitionData.length);
        synchronized (lock) {
            if (this.epochTransition == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "epoch transition not found"));
            } else {
                this.epochTransition = new Data<>(copy, this.epochTransition.getVersion() + 1);
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getEpochTransitionNode() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

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
    CompletableFuture<Void> deleteEpochTransitionNode() {
        synchronized (lock) {
            this.epochTransition = null;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createSealedSegmentsRecord(byte[] sealedSegmentRecord) {
        Preconditions.checkNotNull(sealedSegmentRecord);

        synchronized (lock) {
            this.sealedSegments = new Data<>(sealedSegmentRecord, 0);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getSealedSegmentsRecord() {
        synchronized (lock) {
            if (this.sealedSegments == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            } else {
                return CompletableFuture.completedFuture(copy(sealedSegments));
            }
        }
    }

    @Override
    CompletableFuture<Void> updateSealedSegmentsRecord(Data<Integer> sealedSegments) {
        Preconditions.checkNotNull(sealedSegments);
        Preconditions.checkNotNull(sealedSegments.getData());
        Data<Integer> update = updatedCopy(sealedSegments);
        synchronized (lock) {
            if (this.sealedSegments == null) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "sealedSegments for stream: " + getName()));
            } else if (this.sealedSegments.getVersion().equals(sealedSegments.getVersion())) {
                this.sealedSegments = update;
                return CompletableFuture.completedFuture(null);
            } else {
                return Futures.failedFuture(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                        "sealedSegments for stream: " + getName()));
            }
        }
    }

    @Override
    CompletableFuture<Void> createCommittingTxnRecord(byte[] committingTxns) {
        Preconditions.checkNotNull(committingTxns);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.committingTxnRecord != null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_EXISTS, "committing transactions record exists"));
            } else {
                this.committingTxnRecord = new Data<>(committingTxns, 0);
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getCommittingTxnRecord() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

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
    CompletableFuture<Void> deleteCommittingTxnRecord() {
        synchronized (lock) {
            this.committingTxnRecord = null;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] data) {
        synchronized (lock) {
            if (waitingRequestNode == null) {
                waitingRequestNode = new Data<>(data, 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getWaitingRequestNode() {
        CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

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

    private Data<Integer> copy(Data<Integer> input) {
        return new Data<>(Arrays.copyOf(input.getData(), input.getData().length), input.getVersion());
    }

    private Data<Integer> updatedCopy(Data<Integer> input) {
        return new Data<>(Arrays.copyOf(input.getData(), input.getData().length), input.getVersion() + 1);
    }
}
