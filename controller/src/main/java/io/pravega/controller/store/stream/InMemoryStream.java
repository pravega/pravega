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
import io.pravega.controller.store.stream.tables.Create;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.TableHelper;
import org.apache.commons.lang.SerializationUtils;

import javax.annotation.concurrent.GuardedBy;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InMemoryStream extends PersistentStreamBase<Integer> {

    private AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final AtomicReference<StreamConfiguration> configuration = new AtomicReference<>();

    private final Object stateLock = new Object();
    @GuardedBy("stateLock")
    private final AtomicReference<Data<Integer>> state = new AtomicReference<>();
    private final Object segmentTableLock = new Object();
    @GuardedBy("segmentTableLock")
    private final AtomicReference<Data<Integer>> segmentTable = new AtomicReference<>();
    private final Object historyTableLock = new Object();
    @GuardedBy("historyTableLock")
    private final AtomicReference<Data<Integer>> historyTable = new AtomicReference<>();
    private final Object indexTableLock = new Object();
    @GuardedBy("indexTableLock")
    private final AtomicReference<Data<Integer>> indexTable = new AtomicReference<>();
    private final Object activeTxnsLock = new Object();
    private final ConcurrentHashMap<String, Data<Integer>> activeTxns = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Data<Integer>> completedTxns = new ConcurrentHashMap<>();
    private final Object markersLock = new Object();
    private final ConcurrentHashMap<Integer, Data<Integer>> markers = new ConcurrentHashMap<>();
    /**
     * This is used to guard updates to values in epoch txn map.
     * This ensures that we remove an epoch node if an only if there are no transactions against that epoch.
     * Note: there can be only two epochs at max concurrently. So using one lock for both of their updates is okay.
     */
    private final Object epochTxnLock = new Object();
    private final ConcurrentHashMap<Integer, Set<String>> epochTxnMap = new ConcurrentHashMap<>();
    private final AtomicInteger activeEpoch = new AtomicInteger();

    InMemoryStream(String scope, String name) {
        super(scope, name);
    }

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        return CompletableFuture.completedFuture(activeTxns.size());
    }

    @Override
    public void refresh() {

    }

    @Override
    CompletableFuture<Void> deleteStream() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> checkStreamExists(Create create) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        if (creationTime.get() != Long.MIN_VALUE && create.getCreationTime() != creationTime.get()) {
            result.completeExceptionally(StoreException.create(StoreException.Type.NODE_EXISTS, getName()));
        } else {
            result.complete(null);
        }

        return result;
    }

    @Override
    CompletableFuture<Void> storeCreationTime(Create create) {
        creationTime.compareAndSet(Long.MIN_VALUE, create.getCreationTime());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createConfiguration(Create create) {
        this.configuration.set(create.getConfiguration());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> setConfigurationData(StreamConfiguration configuration) {
        this.configuration.set(configuration);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<StreamConfiguration> getConfigurationData() {
        if (this.configuration.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(this.configuration.get());
    }

    @Override
    CompletableFuture<Void> createState(State state) {
        synchronized (stateLock) {
            this.state.set(new Data<>(SerializationUtils.serialize(state), 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> setStateData(Data<Integer> newState) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (stateLock) {
            this.state.updateAndGet(x -> {
                Preconditions.checkNotNull(x);
                if (Objects.equals(x.getVersion(), newState.getVersion())) {
                    result.complete(null);
                    return copy(new Data<>(newState.getData(), newState.getVersion() + 1));
                } else {
                    result.completeExceptionally(new WriteConflictException("state"));
                    return x;
                }
            });
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getStateData() {
        if (this.state.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(copy(state.get()));
    }

    @Override
    CompletableFuture<Void> createSegmentTable(Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        synchronized (segmentTableLock) {
            segmentTable.set(new Data<>(TableHelper.updateSegmentTable(startingSegmentNumber,
                    new byte[0],
                    newRanges,
                    create.getCreationTime()), 0));
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
        if (this.segmentTable.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(copy(segmentTable.get()));
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentTableFromStore() {
        return getSegmentTable();
    }

    @Override
    CompletableFuture<Void> setSegmentTable(Data<Integer> data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (segmentTableLock) {
            segmentTable.getAndUpdate(x -> {
                if (x.getVersion().equals(data.getVersion())) {
                    result.complete(null);
                    return new Data<>(Arrays.copyOf(data.getData(), data.getData().length), data.getVersion() + 1);
                } else {
                    result.completeExceptionally(new WriteConflictException("segmentTable"));
                    return x;
                }
            });
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createIndexTable(Data<Integer> data) {
        synchronized (indexTableLock) {
            indexTable.set(new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getIndexTable() {
        if (this.indexTable.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(copy(indexTable.get()));
    }

    @Override
    CompletableFuture<Void> updateIndexTable(Data<Integer> updated) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (indexTableLock) {
            indexTable.getAndUpdate(x -> {
                if (x.getVersion().equals(updated.getVersion())) {
                    result.complete(null);
                    return new Data<>(Arrays.copyOf(updated.getData(), updated.getData().length), updated.getVersion() + 1);
                } else {
                    result.completeExceptionally(new WriteConflictException("segmentTable"));
                    return x;
                }
            });
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createHistoryTable(Data<Integer> data) {
        synchronized (historyTableLock) {
            historyTable.set(new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateHistoryTable(Data<Integer> updated) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (historyTableLock) {
            historyTable.getAndUpdate(x -> {
                if (x.getVersion().equals(updated.getVersion())) {
                    result.complete(null);
                    return new Data<>(Arrays.copyOf(updated.getData(), updated.getData().length), updated.getVersion() + 1);
                } else {
                    result.completeExceptionally(new WriteConflictException("segmentTable"));
                    return x;
                }
            });
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTable() {
        if (this.historyTable.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(copy(historyTable.get()));
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTableFromStore() {
        return getHistoryTable();
    }

    @Override
    CompletableFuture<Void> createEpochNode(int epoch) {
        Preconditions.checkArgument(epochTxnMap.size() <= 2);
        activeEpoch.compareAndSet(epoch - 1, epoch);
        synchronized (epochTxnLock) {
            epochTxnMap.putIfAbsent(epoch, new HashSet<>());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> deleteEpochNode(int epoch) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (epochTxnLock) {
            if (epochTxnMap.getOrDefault(epoch, Collections.emptySet()).isEmpty()) {
                epochTxnMap.remove(epoch);
                result.complete(null);
            } else {
                result.completeExceptionally(new DataExistsException(""));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Integer> createNewTransaction(UUID txId, long timestamp, long leaseExpiryTime, long maxExecutionExpiryTime, long scaleGracePeriod) {
        final CompletableFuture<Integer> result = new CompletableFuture<>();
        final Data<Integer> txnData = new Data<>(
                new ActiveTxnRecord(timestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, TxnStatus.OPEN)
                .toByteArray(), 0);
        activeTxns.putIfAbsent(txId.toString(), txnData);
        int epoch = activeEpoch.get();
        synchronized (epochTxnLock) {
            epochTxnMap.computeIfPresent(epoch, (x, y) -> {
                y.add(txId.toString());
                result.complete(epoch);
                return y;
            });
            if (!epochTxnMap.containsKey(epoch)) {
                result.completeExceptionally(new DataNotFoundException("epoch"));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Integer> getTransactionEpoch(UUID txId) {
        Optional<Integer> epoch = epochTxnMap.entrySet().stream().filter(x -> x.getValue().contains(txId.toString())).findFirst()
                .map(Map.Entry::getKey);
        if (epoch.isPresent()) {
            return CompletableFuture.completedFuture(epoch.get());
        } else {
            throw new DataNotFoundException(txId.toString());
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(int epoch, UUID txId) throws DataNotFoundException {
        if (!activeTxns.containsKey(txId.toString())) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(copy(activeTxns.get(txId.toString())));
    }

    @Override
    CompletableFuture<Void> updateActiveTx(int epoch, UUID txId, byte[] data) throws DataNotFoundException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (activeTxnsLock) {
            activeTxns.computeIfPresent(txId.toString(), (x, y) -> {
                result.complete(null);
                return new Data<>(y.getData(), y.getVersion() + 1);
            });

            if (!activeTxns.containsKey(txId.toString())) {
                result.completeExceptionally(new DataNotFoundException("active txn"));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Void> sealActiveTx(int epoch, UUID txId, boolean commit, ActiveTxnRecord txnRecord, int version) throws DataNotFoundException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (activeTxnsLock) {
            activeTxns.computeIfPresent(txId.toString(), (x, y) -> {
                if (version != y.getVersion()) {
                    result.completeExceptionally(new WriteConflictException(txId.toString()));
                }
                ActiveTxnRecord previous = ActiveTxnRecord.parse(y.getData());
                ActiveTxnRecord updated = new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                        previous.getLeaseExpiryTime(),
                        previous.getMaxExecutionExpiryTime(),
                        previous.getScaleGracePeriod(),
                        commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING);
                result.complete(null);
                return new Data<>(updated.toByteArray(), y.getVersion() + 1);
            });
            if (!activeTxns.containsKey(txId.toString())) {
                result.completeExceptionally(new DataNotFoundException("txn"));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(UUID txId) throws DataNotFoundException {
        if (!completedTxns.containsKey(txId.toString())) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(copy(completedTxns.get(txId.toString())));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(int epoch, UUID txId) {
        synchronized (activeTxnsLock) {
            activeTxns.remove(txId.toString());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, TxnStatus complete, long timestamp) {
        completedTxns.putIfAbsent(txId.toString(), new Data<>(new CompletedTxnRecord(timestamp, complete).toByteArray(), 0));
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
            markers.computeIfPresent(segmentNumber, (x, y) -> {
                if (y.getVersion().equals(data.getVersion())) {
                    result.complete(null);
                    return new Data<>(Arrays.copyOf(data.getData(), data.getData().length), data.getVersion() + 1);
                } else {
                    result.completeExceptionally(new WriteConflictException(""));
                    return y;
                }
            });
            if (!markers.containsKey(segmentNumber)) {
                result.completeExceptionally(new DataNotFoundException("segment number"));
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
        if (!markers.containsKey(segmentNumber)) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(copy(markers.get(segmentNumber)));
    }

    @Override
    CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        Map<String, Data<Integer>> map = activeTxns.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> copy(x.getValue())));
        return CompletableFuture.completedFuture(Collections.unmodifiableMap(map));
    }

    @Override
    CompletableFuture<Void> checkScopeExists() throws StoreException {
        return CompletableFuture.completedFuture(null);
    }

    private Data<Integer> copy(Data<Integer> input) {
        return new Data<>(Arrays.copyOf(input.getData(), input.getData().length), input.getVersion());
    }
}
