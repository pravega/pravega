/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.Data;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Utilities;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.SerializationUtils;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class InMemoryPersistentStream extends PersistentStreamBase<Integer> {

    private Long creationTime;
    private Data<Integer> configuration;
    private Data<Integer> state;
    private final Map<Integer, Data<Integer>> segmentTableMap;
    private Data<Integer> historyTable;
    private final Object historyTableLock;
    private Data<Integer> indexTable;
    private final Object indexTableLock;
    private final Map<String, Data<Integer>> activeTxns;
    private final Map<String, Data<Integer>> completedTxns;
    private final Map<Integer, Data<Integer>> markers;

    public InMemoryPersistentStream(final String scopeName, final String streamName) {
        super(scopeName, streamName);
        activeTxns = new HashMap<>();
        completedTxns = new HashMap<>();
        segmentTableMap = new HashMap<>();
        markers = new HashMap<>();
        historyTableLock = new Object();
        indexTableLock = new Object();
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        return CompletableFuture.completedFuture(activeTxns.size());
    }

    @Override
    public void refresh() {
    }

    @Override
    public CompletableFuture<Void> deleteStream() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> storeCreationTime(final Create create) {
        creationTime = create.getEventTime();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createConfiguration(final Create create) {
        configuration = new Data<>(SerializationUtils.serialize(create.getConfiguration()), null);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createState(final State state) {
        this.state = new Data<>(SerializationUtils.serialize(state), null);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> checkStreamExists(final Create create) throws StreamAlreadyExistsException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (creationTime != null && creationTime != create.getEventTime()) {
            future.completeExceptionally(new DataExistsException(getName()));
        } else {
            future.complete(null);
        }

        return future;
    }

    public CompletableFuture<Void> checkScopeExists() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final Data<Integer> data) {
        synchronized (segmentTableMap) {
            return createEntryInMap(segmentTableMap, chunkNumber, data.getData(), "segment chunk");
        }
    }

    @Override
    public CompletableFuture<Void> createIndexTable(final Create create) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        synchronized (indexTableLock) {
            if (indexTable == null) {
                final byte[] indexTableData = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
                indexTable = new Data<>(indexTableData, 0);
                future.complete(null);
            } else {
                future.completeExceptionally(new DataExistsException("indexTable"));
            }
        }

        return future;
    }

    @Override
    CompletableFuture<Void> updateIndexTable(final Data<Integer> updated) {
        CompletableFuture<Void> retval = new CompletableFuture<>();
        synchronized (indexTableLock) {
            if (updated.getVersion().equals(indexTable.getVersion())) {
                indexTable = new Data<>(updated.getData(), updated.getVersion() + 1);
                retval.complete(null);
            } else {
                retval.completeExceptionally(new WriteConflictException("indexTable"));
            }
        }

        return retval;
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Create create) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        synchronized (historyTableLock) {
            if (historyTable == null) {
                final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
                final byte[] historyTableData = TableHelper.updateHistoryTable(new byte[0],
                        create.getEventTime(),
                        IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

                historyTable = new Data<>(historyTableData, 0);
                future.complete(null);
            } else {
                future.completeExceptionally(new DataExistsException("history table"));
            }
        }

        return future;
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final Data<Integer> updated) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        synchronized (historyTableLock) {
            if (updated.getVersion().equals(historyTable.getVersion())) {
                historyTable = new Data<>(updated.getData(), updated.getVersion() + 1);
                future.complete(null);
            } else {
                future.completeExceptionally(new WriteConflictException("indexTable"));
            }
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> createSegmentFile(final Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final int chunkNumber = 0;
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        final int toCreate = newRanges.size();

        final byte[] segmentTableChunkData = TableHelper.updateSegmentTable(startingSegmentNumber,
                new byte[0],
                toCreate,
                newRanges,
                create.getEventTime()
        );

        synchronized (segmentTableMap) {
            return createEntryInMap(segmentTableMap, chunkNumber, segmentTableChunkData, "segment chunk");
        }
    }

    @Override
    public CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp) {
        synchronized (markers) {
            return createEntryInMap(markers, segmentNumber, Utilities.toByteArray(timestamp), "marker");
        }
    }

    @Override
    CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<Integer> data) {
        synchronized (markers) {
            return updateEntryInMap(markers, segmentNumber, data, "marker");
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getMarkerData(int segmentNumber) {
        if (markers.containsKey(segmentNumber)) {
            return CompletableFuture.completedFuture(markers.get(segmentNumber));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    CompletableFuture<Void> removeMarkerData(int segmentNumber) {
        markers.remove(segmentNumber);

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        return CompletableFuture.completedFuture(activeTxns);
    }

    @Override
    CompletableFuture<Void> createNewTransaction(final UUID txId, final long timestamp, final long leaseExpiryTime,
                                                 final long maxExecutionExpiryTime,
                                                 final long scaleGracePeriod) {
        synchronized (activeTxns) {
            return createEntryInMap(activeTxns, txId.toString(), new ActiveTxRecord(timestamp,
                    leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, TxnStatus.OPEN).toByteArray(), "active txns");
        }
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final UUID txId) {
        return CompletableFuture.completedFuture(activeTxns.get(txId.toString()));
    }

    @Override
    CompletableFuture<Void> updateActiveTx(final UUID txId, final byte[] data) {

        Integer version = activeTxns.get(txId.toString()).getVersion();

        synchronized (activeTxns) {
            return updateEntryInMap(activeTxns, txId.toString(), new Data<>(data, version), "txn update");
        }
    }

    @Override
    CompletableFuture<Void> sealActiveTx(final UUID txId, final boolean commit, final Optional<Integer> version) {
        return getActiveTx(txId)
                .thenCompose(x -> {
                    if (version.isPresent() && version.get().intValue() != x.getVersion()) {
                        throw new WriteConflictException(txId.toString());
                    }

                    ActiveTxRecord previous = ActiveTxRecord.parse(x.getData());
                    ActiveTxRecord updated = new ActiveTxRecord(previous.getTxCreationTimestamp(),
                            previous.getLeaseExpiryTime(),
                            previous.getMaxExecutionExpiryTime(),
                            previous.getScaleGracePeriod(),
                            commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING);
                    synchronized (activeTxns) {
                        return updateEntryInMap(activeTxns, txId.toString(), new Data<>(updated.toByteArray(), x.getVersion()), "seal txn");
                    }
                });
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId) {
        return CompletableFuture.completedFuture(completedTxns.get(txId.toString()));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final UUID txId) {
        activeTxns.remove(txId.toString());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp) {
        synchronized (completedTxns) {
            return createEntryInMap(completedTxns, txId.toString(),
                    new CompletedTxRecord(timestamp, complete).toByteArray(), "completed tx");
        }
    }

    @Override
    public CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration) {
        this.configuration = new Data<>(SerializationUtils.serialize(configuration), null);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        if (configuration == null) {
            CompletableFuture<StreamConfiguration> future = new CompletableFuture<>();
            future.completeExceptionally(new DataNotFoundException(""));
            return future;
        }

        return CompletableFuture.completedFuture((StreamConfiguration) SerializationUtils.deserialize(configuration.getData()));
    }

    @Override
    CompletableFuture<Void> setStateData(final State state) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (this.state == null) {
            future.completeExceptionally(new DataNotFoundException("state"));
        } else {
            this.state = new Data<>(SerializationUtils.serialize(state), null);
            future.complete(null);
        }

        return future;
    }

    @Override
    CompletableFuture<State> getStateData() {
        if (state == null) {
            CompletableFuture<State> stateCompletableFuture = new CompletableFuture<>();
            stateCompletableFuture.completeExceptionally(new DataNotFoundException(""));
            return stateCompletableFuture;
        }
        return CompletableFuture.completedFuture((State) SerializationUtils.deserialize(state.getData()));
    }

    @Override
    public CompletableFuture<Segment> getSegmentRow(final int number) {
        // compute the file name based on segment number
        final int chunkNumber = number / SegmentRecord.SEGMENT_CHUNK_SIZE;

        return getSegmentTableChunk(chunkNumber)
                .thenApply(x -> TableHelper.getSegment(number, x.getData()));
    }

    @Override
    public CompletableFuture<Data<Integer>> getSegmentTableChunk(final int chunkNumber) {
        return CompletableFuture.completedFuture(segmentTableMap.get(chunkNumber));
    }

    @Override
    CompletableFuture<Void> setSegmentTableChunk(final int chunkNumber, final Data<Integer> data) {
        synchronized (segmentTableMap) {
            return updateEntryInMap(segmentTableMap, chunkNumber, data, "segmentChunk");
        }
    }

    @Override
    public CompletableFuture<List<String>> getSegmentChunks() {
        return CompletableFuture.completedFuture(segmentTableMap.keySet().stream().map(Object::toString).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Data<Integer>> getHistoryTable() {
        return CompletableFuture.completedFuture(historyTable);
    }

    @Override
    public CompletableFuture<Data<Integer>> getIndexTable() {
        return CompletableFuture.completedFuture(indexTable);
    }

    // endregion

    // region private helpers

    private <T, U> CompletableFuture<Void> createEntryInMap(final Map<T, Data<Integer>> map, T key, byte[] data, String errorMsg) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (!map.containsKey(key)) {
            map.put(key, new Data<>(data, 0));
            future.complete(null);
        } else {
            future.completeExceptionally(new DataExistsException(errorMsg));
        }

        return future;
    }

    private <T> CompletableFuture<Void> updateEntryInMap(final Map<T, Data<Integer>> map, T key, Data<Integer> data, String errorMsg) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (map.containsKey(key)) {
            if (map.get(key).getVersion().equals(data.getVersion())) {
                map.put(key, new Data<>(data.getData(), data.getVersion() + 1));
                future.complete(null);
            } else {
                future.completeExceptionally(new WriteConflictException(errorMsg));
            }
        } else {
            future.completeExceptionally(new DataNotFoundException(errorMsg));
        }

        return future;
    }

    // endregion
}
