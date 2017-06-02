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

import java.util.AbstractMap;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InMemoryStream extends PersistentStreamBase<Integer> {

    private long creationTime = Long.MIN_VALUE;
    private StreamConfiguration configuration;
    private final AtomicReference<Data<Integer>> state = new AtomicReference<>();
    private final AtomicReference<Data<Integer>> segmentTable = new AtomicReference<>();
    private final AtomicReference<Data<Integer>> historyTable = new AtomicReference<>();
    private final AtomicReference<Data<Integer>> indexTable = new AtomicReference<>();
    private final ConcurrentHashMap<String, Data<Integer>> activeTxns = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Data<Integer>> completedTxns = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Data<Integer>> markers = new ConcurrentHashMap<>();
    private final Object epochLock = new Object();
    private final ConcurrentHashMap<Long, Set<String>> epochTxnMap = new ConcurrentHashMap<>();
    private final AtomicLong activeEpoch = new AtomicLong();

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

        if (creationTime != Long.MIN_VALUE && create.getCreationTime() != creationTime) {
            result.completeExceptionally(StoreException.create(StoreException.Type.NODE_EXISTS, getName()));
        } else {
            result.complete(null);
        }

        return result;
    }

    @Override
    CompletableFuture<Void> storeCreationTime(Create create) {
        creationTime = create.getCreationTime();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createConfiguration(Create create) {
        configuration = create.getConfiguration();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> setConfigurationData(StreamConfiguration configuration) {
        this.configuration = configuration;
        return CompletableFuture.completedFuture(null);
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
        this.state.set(new Data<>(SerializationUtils.serialize(state), 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> setStateData(Data<Integer> newState) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        this.state.updateAndGet(x -> {
            if (Objects.equals(x.getVersion(), newState.getVersion())) {
                result.complete(null);
                return new Data<>(newState.getData(), newState.getVersion() + 1);
            } else {
                result.completeExceptionally(new WriteConflictException("state"));
                return x;
            }
        });

        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getStateData() {
        if (this.state.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(state.get());
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

        segmentTable.set(new Data<>(TableHelper.updateSegmentTable(startingSegmentNumber,
                new byte[0],
                newRanges,
                create.getCreationTime()), 0));

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

        return CompletableFuture.completedFuture(segmentTable.get());
    }

    @Override
    CompletableFuture<Void> setSegmentTable(Data<Integer> data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        segmentTable.getAndUpdate(x -> {
            if (x.getVersion().equals(data.getVersion())) {
                result.complete(null);
                return new Data<>(data.getData(), data.getVersion() + 1);
            } else {
                result.completeExceptionally(new WriteConflictException("segmentTable"));
                return x;
            }
        });
        return result;
    }

    @Override
    CompletableFuture<Void> createIndexTable(Data<Integer> data) {
        indexTable.set(new Data<>(data.getData(), 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getIndexTable() {
        if (this.indexTable.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(indexTable.get());
    }

    @Override
    CompletableFuture<Void> updateIndexTable(Data<Integer> updated) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        indexTable.getAndUpdate(x -> {
            if (x.getVersion().equals(updated.getVersion())) {
                result.complete(null);
                return new Data<>(updated.getData(), updated.getVersion() + 1);
            } else {
                result.completeExceptionally(new WriteConflictException("segmentTable"));
                return x;
            }
        });
        return result;
    }

    @Override
    CompletableFuture<Void> createHistoryTable(Data<Integer> data) {
        historyTable.set(new Data<>(data.getData(), 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateHistoryTable(Data<Integer> updated) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        historyTable.getAndUpdate(x -> {
            if (x.getVersion().equals(updated.getVersion())) {
                result.complete(null);
                return new Data<>(updated.getData(), updated.getVersion() + 1);
            } else {
                result.completeExceptionally(new WriteConflictException("segmentTable"));
                return x;
            }
        });
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getHistoryTable() {
        if (this.historyTable.get() == null) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(historyTable.get());
    }

    @Override
    CompletableFuture<Void> createEpochNode(long epoch) {
        activeEpoch.compareAndSet(epoch - 1, epoch);
        epochTxnMap.putIfAbsent(epoch, new HashSet<>());
        assert epochTxnMap.size() <= 2;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> deleteEpochNode(long epoch) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (epochLock) {
            if (epochTxnMap.getOrDefault(epoch, Collections.emptySet()).size() == 0) {
                epochTxnMap.remove(epoch);
                result.complete(null);
            } else {
                result.completeExceptionally(new DataExistsException(""));
            }
        }
        return result;
    }

    @Override
    CompletableFuture<Void> createNewTransaction(UUID txId, long timestamp, long leaseExpiryTime, long maxExecutionExpiryTime, long scaleGracePeriod) {
        Data<Integer> txnData = new Data<>(new ActiveTxnRecord(timestamp, leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, TxnStatus.OPEN)
                .toByteArray(), 0);
        activeTxns.putIfAbsent(txId.toString(), txnData);
        synchronized (epochLock) {
            epochTxnMap.computeIfPresent(activeEpoch.get(), (x, y) -> {
                y.add(txId.toString());
                return y;
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(UUID txId) throws DataNotFoundException {
        if (!activeTxns.containsKey(txId.toString())) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }

        return CompletableFuture.completedFuture(activeTxns.get(txId.toString()));
    }

    @Override
    CompletableFuture<Void> updateActiveTx(UUID txId, byte[] data) throws DataNotFoundException {
        activeTxns.computeIfPresent(txId.toString(), (x, y) -> new Data<>(y.getData(), y.getVersion() + 1));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(UUID txId, boolean commit, Optional<Integer> version) throws DataNotFoundException {
        CompletableFuture<Void> result = new CompletableFuture<>();
        activeTxns.computeIfPresent(txId.toString(), (x, y) -> {
            if (version.isPresent() && version.get().intValue() != y.getVersion()) {
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
        return result;
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(UUID txId) throws DataNotFoundException {
        if (!completedTxns.containsKey(txId.toString())) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(completedTxns.get(txId.toString()));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(UUID txId) {
        activeTxns.remove(txId.toString());
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

        markers.putIfAbsent(segmentNumber, new Data<>(b, 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<Integer> data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        markers.computeIfPresent(segmentNumber, (x, y) -> {
            if (y.getVersion().equals(data.getVersion())) {
                result.complete(null);
                return new Data<>(data.getData(), data.getVersion() + 1);
            } else {
                result.completeExceptionally(new WriteConflictException(""));
                return y;
            }
        });
        return result;
    }

    @Override
    CompletableFuture<Void> removeMarkerData(int segmentNumber) {
        markers.remove(segmentNumber);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getMarkerData(int segmentNumber) {
        if (!markers.containsKey(segmentNumber)) {
            return FutureHelpers.failedFuture(new DataNotFoundException(getName()));
        }
        return CompletableFuture.completedFuture(markers.get(segmentNumber));
    }

    @Override
    CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        return CompletableFuture.completedFuture(Collections.unmodifiableMap(activeTxns));
    }

    @Override
    CompletableFuture<Void> checkScopeExists() throws StoreException {
        return CompletableFuture.completedFuture(null);
    }
}
