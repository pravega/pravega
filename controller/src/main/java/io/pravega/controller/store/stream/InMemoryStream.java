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

    private final AtomicLong creationTime = new AtomicLong(Long.MIN_VALUE);
    private final Object lock = new Object();

    @GuardedBy("lock")
    private StreamConfiguration configuration;
    @GuardedBy("lock")
    private volatile Data<Integer> state;
    @GuardedBy("lock")
    private volatile Data<Integer> segmentTable;
    @GuardedBy("lock")
    private volatile Data<Integer> historyTable;
    @GuardedBy("lock")
    private volatile Data<Integer> indexTable;

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
        Preconditions.checkNotNull(create);
        Preconditions.checkNotNull(create.getCreationTime());
        creationTime.compareAndSet(Long.MIN_VALUE, create.getCreationTime());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createConfiguration(Create create) {
        Preconditions.checkNotNull(create);
        Preconditions.checkNotNull(create.getConfiguration());

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (configuration != null) {
                result.completeExceptionally(new DataExistsException("configuration"));
            } else {
                configuration = create.getConfiguration();
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> setConfigurationData(StreamConfiguration configuration) {
        Preconditions.checkNotNull(configuration);

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (this.configuration == null) {
                result.completeExceptionally(new DataNotFoundException("configuration"));
            } else {
                this.configuration = configuration;
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<StreamConfiguration> getConfigurationData() {
        if (this.configuration == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(this.configuration);
    }

    @Override
    CompletableFuture<Void> createState(State state) {
        Preconditions.checkNotNull(state);

        synchronized (lock) {
            if (this.state == null) {
                this.state = new Data<>(SerializationUtils.serialize(state), 0);
            } else {
                return FutureHelpers.failedFuture(new DataExistsException("state"));
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
                result.completeExceptionally(new WriteConflictException("state"));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getStateData() {
        if (this.state == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(copy(state));
    }

    @Override
    CompletableFuture<Void> createSegmentTable(Create create) {
        Preconditions.checkNotNull(create);
        Preconditions.checkNotNull(create.getConfiguration());

        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (segmentTable == null) {
                segmentTable = new Data<>(TableHelper.updateSegmentTable(startingSegmentNumber,
                        new byte[0],
                        newRanges,
                        create.getCreationTime()), 0);
                result.complete(null);
            } else {
                result.completeExceptionally(new DataExistsException("segmentTable"));
            }
        }

        return result;
    }

    @Override
    CompletableFuture<Segment> getSegmentRow(int number) {
        return getSegmentTable()
                .thenApply(x -> TableHelper.getSegment(number, x.getData()));
    }

    @Override
    CompletableFuture<Data<Integer>> getSegmentTable() {
        if (this.segmentTable == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(copy(this.segmentTable));
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
                result.completeExceptionally(new DataNotFoundException("segmentTable"));
            }
            if (segmentTable.getVersion().equals(data.getVersion())) {
                segmentTable = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), data.getVersion() + 1);
                result.complete(null);
            } else {
                result.completeExceptionally(new WriteConflictException("segmentTable"));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createIndexTable(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        synchronized (lock) {
            if (indexTable != null) {
                return FutureHelpers.failedFuture(new DataExistsException("index"));
            } else {
                indexTable = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getIndexTable() {
        if (this.indexTable == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(copy(indexTable));
    }

    @Override
    CompletableFuture<Void> updateIndexTable(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        final CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (lock) {
            if (indexTable == null) {
                result.completeExceptionally(new DataNotFoundException("index"));
            }
            if (indexTable.getVersion().equals(updated.getVersion())) {
                indexTable = new Data<>(Arrays.copyOf(updated.getData(), updated.getData().length), updated.getVersion() + 1);
                result.complete(null);
            } else {
                result.completeExceptionally(new WriteConflictException("segmentTable"));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createHistoryTable(Data<Integer> data) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(data.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();

        synchronized (lock) {
            if (historyTable != null) {
                result.completeExceptionally(new DataExistsException("historyTable"));
            } else {
                historyTable = new Data<>(Arrays.copyOf(data.getData(), data.getData().length), 0);
                result.complete(null);
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> updateHistoryTable(Data<Integer> updated) {
        Preconditions.checkNotNull(updated);
        Preconditions.checkNotNull(updated.getData());

        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (lock) {
            if (historyTable == null) {
                result.completeExceptionally(new DataNotFoundException("history"));
            } else {
                if (historyTable.getVersion().equals(updated.getVersion())) {
                    historyTable = new Data<>(Arrays.copyOf(updated.getData(), updated.getData().length), updated.getVersion() + 1);
                    result.complete(null);
                } else {
                    result.completeExceptionally(new WriteConflictException("segmentTable"));
                }
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTable() {
        if (this.historyTable == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(copy(historyTable));
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
    CompletableFuture<Integer> createNewTransaction(UUID txId, long timestamp, long leaseExpiryTime, long maxExecutionExpiryTime,
                                                    long scaleGracePeriod) {
        Preconditions.checkNotNull(txId);

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
        Preconditions.checkNotNull(data);

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
        Preconditions.checkNotNull(txId);

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
        Preconditions.checkNotNull(txId);

        if (!completedTxns.containsKey(txId.toString())) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(copy(completedTxns.get(txId.toString())));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(int epoch, UUID txId) {
        Preconditions.checkNotNull(txId);

        synchronized (activeTxnsLock) {
            activeTxns.remove(txId.toString());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(UUID txId, TxnStatus complete, long timestamp) {
        Preconditions.checkNotNull(txId);

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
