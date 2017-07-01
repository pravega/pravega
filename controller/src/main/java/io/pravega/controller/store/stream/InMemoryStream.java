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

import com.google.common.base.Preconditions;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.TableHelper;
import org.apache.commons.lang.SerializationUtils;

import javax.annotation.concurrent.GuardedBy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class InMemoryStream extends PersistentStreamBase<Integer> {

    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();
    @GuardedBy("lock")
    private StreamConfiguration configuration;
    @GuardedBy("lock")
    private Data<Integer> state;
    @GuardedBy("lock")
    private Data<Integer> segmentTable;
    @GuardedBy("lock")
    private Data<Integer> historyTable;
    @GuardedBy("lock")
    private Data<Integer> indexTable;

    private final Object txnsLock = new Object();
    @GuardedBy("txnsLock")
    private final Map<String, Data<Integer>> activeTxns = new HashMap<>();
    @GuardedBy("txnsLock")
    private final Map<String, Data<Integer>> completedTxns = new HashMap<>();
    private final Object markersLock = new Object();
    @GuardedBy("markersLock")
    private final Map<Integer, Data<Integer>> markers = new HashMap<>();
    /**
     * This is used to guard updates to values in epoch txn map.
     * This ensures that we remove an epoch node if an only if there are no transactions against that epoch.
     * Note: there can be only two epochs at max concurrently. So using one lock for both of their updates is okay.
     */
    @GuardedBy("txnsLock")
    private final Map<Integer, Set<String>> epochTxnMap = new HashMap<>();
    private final AtomicInteger activeEpoch = new AtomicInteger();

    InMemoryStream(String scope, String name) {
        super(scope, name);
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
        final StreamConfiguration config;
        final Data<Integer> currentState;
        synchronized (lock) {
            time = creationTime.get();
            config = this.configuration;
            currentState = this.state;
        }

        if (time != Long.MIN_VALUE) {
            if (config != null) {
                handleStreamMetadataExists(timestamp, result, time, config, currentState);
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
            State stateVal = (State) SerializationUtils.deserialize(currentState.getData());
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
    CompletableFuture<Void> createConfigurationIfAbsent(StreamConfiguration config) {
        Preconditions.checkNotNull(config);

        synchronized (lock) {
            if (configuration == null) {
                configuration = config;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> setConfigurationData(StreamConfiguration configuration) {
        Preconditions.checkNotNull(configuration);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.configuration == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "configuration"));
            } else {
                this.configuration = configuration;
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<StreamConfiguration> getConfigurationData() {
        synchronized (lock) {
            if (this.configuration == null) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(this.configuration);
        }
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent(State state) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            if (this.state == null) {
                this.state = new Data<>(SerializationUtils.serialize(state), 0);
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
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, "state"));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getStateData() {
        synchronized (lock) {
            if (this.state == null) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(state));
        }
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
    CompletableFuture<Segment> getSegmentRow(int number) {
        return getSegmentTable()
                .thenApply(x -> TableHelper.getSegment(number, x.getData()));
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentTable() {
        synchronized (lock) {
            if (this.segmentTable == null) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(this.segmentTable));
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentTableFromStore() {
        return getSegmentTable();
    }

    @Override
    CompletableFuture<Void> setSegmentTable(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (lock) {
            if (segmentTable == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "segmentTable"));
            } else if (segmentTable.getVersion().equals(data.getVersion())) {
                segmentTable = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), data.getVersion() + 1);
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, "segmentTable"));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createIndexTableIfAbsent(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        synchronized (lock) {
            if (indexTable == null) {
                indexTable = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getIndexTable() {
        synchronized (lock) {
            if (this.indexTable == null) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(indexTable));
        }
    }

    @Override
    CompletableFuture<Void> updateIndexTable(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (lock) {
            if (indexTable == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "index"));
            } else if (indexTable.getVersion().equals(updated.getVersion())) {
                indexTable = new Data<>(Arrays.copyOf(updated.getData(), updated.getData().length), updated.getVersion() + 1);
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, "segmentTable"));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createHistoryTableIfAbsent(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (historyTable == null) {
                historyTable = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateHistoryTable(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (lock) {
            if (historyTable == null) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "history"));
            } else {
                if (historyTable.getVersion().equals(updated.getVersion())) {
                    historyTable = new Data<>(Arrays.copyOf(updated.getData(), updated.getData().length), updated.getVersion() + 1);
                    result.complete(null);
                } else {
                    result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT, "segmentTable"));
                }
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTable() {
        synchronized (lock) {
            if (this.historyTable == null) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }

            return CompletableFuture.completedFuture(copy(historyTable));
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTableFromStore() {
        return getHistoryTable();
    }

    @Override
    CompletableFuture<Void> createEpochNodeIfAbsent(int epoch) {
        Preconditions.checkArgument(epochTxnMap.size() <= 2);
        activeEpoch.compareAndSet(epoch - 1, epoch);
        synchronized (txnsLock) {
            epochTxnMap.putIfAbsent(epoch, new HashSet<>());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> deleteEpochNode(int epoch) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (txnsLock) {
            if (epochTxnMap.getOrDefault(epoch, Collections.emptySet()).isEmpty()) {
                epochTxnMap.remove(epoch);
                result.complete(null);
            } else {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_CONTAINS_ELEMENTS, "epoch"));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Integer> createNewTransaction(UUID txId, long timestamp, long leaseExpiryTime, long maxExecutionExpiryTime,
                                                    long scaleGracePeriod) {
        Preconditions.checkNotNull(txId);

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        final Data<Integer> txnData = new Data<>(
                new ActiveTxnRecord(timestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, TxnStatus.OPEN)
                .toByteArray(), 0);
        synchronized (txnsLock) {
            activeTxns.putIfAbsent(txId.toString(), txnData);
        }
        int epoch = activeEpoch.get();
        synchronized (txnsLock) {
            if (!epochTxnMap.containsKey(epoch)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "epoch"));
            } else {
                epochTxnMap.compute(epoch, (x, y) -> {
                    y.add(txId.toString());
                    return y;
                });
                result.complete(epoch);
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Integer> getTransactionEpoch(UUID txId) {
        Optional<Integer> epoch;
        synchronized (txnsLock) {
            epoch = epochTxnMap.entrySet().stream().filter(x -> x.getValue().contains(txId.toString())).findFirst()
                    .map(Map.Entry::getKey);
        }
        return epoch.map(CompletableFuture::completedFuture)
                .orElseGet(() -> FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        txId.toString())));
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(int epoch, UUID txId) {
        synchronized (txnsLock) {
            if (!activeTxns.containsKey(txId.toString())) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
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
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "active txn"));
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
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "txn"));
            } else {
                activeTxns.compute(txId.toString(), (x, y) -> {
                    if (version != y.getVersion()) {
                        result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT,
                                txId.toString()));
                        return y;
                    } else {
                        ActiveTxnRecord previous = ActiveTxnRecord.parse(y.getData());
                        ActiveTxnRecord updated = new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                                previous.getLeaseExpiryTime(),
                                previous.getMaxExecutionExpiryTime(),
                                previous.getScaleGracePeriod(),
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
            if (!completedTxns.containsKey(txId.toString())) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(completedTxns.get(txId.toString())));
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
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, TxnStatus complete, long timestamp) {
        Preconditions.checkNotNull(txId);

        synchronized (txnsLock) {
            completedTxns.putIfAbsent(txId.toString(), new Data<>(new CompletedTxnRecord(timestamp, complete).toByteArray(), 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, timestamp);
        synchronized (markersLock) {
            markers.putIfAbsent(segmentNumber, new Data<>(b, 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<Integer> data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (markersLock) {
            if (!markers.containsKey(segmentNumber)) {
                result.completeExceptionally(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "segment number"));
            } else {
                markers.compute(segmentNumber, (x, y) -> {
                    if (y.getVersion().equals(data.getVersion())) {
                        result.complete(null);
                        return new Data<>(Arrays.copyOf(data.getData(), data.getData().length), data.getVersion() + 1);
                    } else {
                        result.completeExceptionally(StoreException.create(StoreException.Type.WRITE_CONFLICT));
                        return y;
                    }
                });
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> removeMarkerData(int segmentNumber) {
        synchronized (markersLock) {
            markers.remove(segmentNumber);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getMarkerData(int segmentNumber) {
        synchronized (markersLock) {
            if (!markers.containsKey(segmentNumber)) {
                return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, getName()));
            }
            return CompletableFuture.completedFuture(copy(markers.get(segmentNumber)));
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
    CompletableFuture<Void> checkScopeExists() throws StoreException {
        return CompletableFuture.completedFuture(null);
    }

    private Data<Integer> copy(Data<Integer> input) {
        return new Data<>(Arrays.copyOf(input.getData(), input.getData().length), input.getVersion());
    }
}
