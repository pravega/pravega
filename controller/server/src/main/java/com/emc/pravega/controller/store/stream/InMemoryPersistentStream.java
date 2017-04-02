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
import lombok.Synchronized;
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
    private Data<Integer> indexTable;
    private final Map<String, Data<Integer>> activeTxns;
    private final Map<String, Data<Integer>> completedTxns;
    private final Map<Integer, Data<Integer>> markers;

    public InMemoryPersistentStream(final String scopeName, final String streamName) {
        super(scopeName, streamName);
        activeTxns = new HashMap<>();
        completedTxns = new HashMap<>();
        segmentTableMap = new HashMap<>();
        markers = new HashMap<>();
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
        if (creationTime != null && creationTime != create.getEventTime()) {
            throw new DataExistsException(getName());
        }

        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> checkScopeExists() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final Data<Integer> data) {
        if (!segmentTableMap.containsKey(chunkNumber)) {
            segmentTableMap.put(chunkNumber, data);
        } else {
            throw new DataExistsException("segment chunk");
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createIndexTable(final Create create) {
        if (indexTable == null) {
            final byte[] indexTableData = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
            indexTable = new Data<>(indexTableData, 0);
        } else {
            throw new DataExistsException("indexTable");
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final byte[] historyTableData = TableHelper.updateHistoryTable(new byte[0],
                create.getEventTime(),
                IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

        historyTable = new Data<>(historyTableData, 0);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final Data<Integer> updated) {
        if (updated.getVersion().equals(historyTable.getVersion())) {
            historyTable = new Data<>(updated.getData(), updated.getVersion() + 1);
        } else {
            throw new WriteConflictException("indexTable");
        }

        return CompletableFuture.completedFuture(null);
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

        segmentTableMap.put(chunkNumber, new Data<>(segmentTableChunkData, 0));

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp) {
        markers.put(segmentNumber, new Data<>(Utilities.toByteArray(timestamp), 0));

        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<Integer> data) {
        markers.put(segmentNumber, data);

        return CompletableFuture.completedFuture(null);
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
        activeTxns.put(txId.toString(), new Data<>(new ActiveTxRecord(timestamp,
                leaseExpiryTime, maxExecutionExpiryTime, scaleGracePeriod, TxnStatus.OPEN).toByteArray(), 0));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final UUID txId) {
        return CompletableFuture.completedFuture(activeTxns.get(txId.toString()));
    }

    @Override
    CompletableFuture<Void> updateActiveTx(final UUID txId, final byte[] data) {
        Integer version = activeTxns.get(txId.toString()).getVersion();
        activeTxns.put(txId.toString(), new Data<>(data, version));

        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(final UUID txId, final boolean commit, final Optional<Integer> version) {

        return getActiveTx(txId)
                .thenAccept(x -> {
                    if (version.isPresent() && version.get().intValue() != x.getVersion()) {
                        throw new WriteConflictException(txId.toString());
                    }
                    ActiveTxRecord previous = ActiveTxRecord.parse(x.getData());
                    ActiveTxRecord updated = new ActiveTxRecord(previous.getTxCreationTimestamp(),
                            previous.getLeaseExpiryTime(),
                            previous.getMaxExecutionExpiryTime(),
                            previous.getScaleGracePeriod(),
                            commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING);
                    activeTxns.put(txId.toString(), new Data<>(updated.toByteArray(), x.getVersion()));
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
        completedTxns.put(txId.toString(), new Data<>(new CompletedTxRecord(timestamp, complete).toByteArray(), 0));
        return CompletableFuture.completedFuture(null);
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
        this.state = new Data<>(SerializationUtils.serialize(state), null);

        return CompletableFuture.completedFuture(null);
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
            segmentTableMap.put(chunkNumber, data);
        }
        return CompletableFuture.completedFuture(null);
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

    @Override
    @Synchronized
    CompletableFuture<Void> updateIndexTable(final Data<Integer> updated) {
        if (updated.getVersion().equals(indexTable.getVersion())) {
            indexTable = new Data<>(updated.getData(), updated.getVersion() + 1);
        } else {
            throw new WriteConflictException("indexTable");
        }
        return CompletableFuture.completedFuture(null);
    }

    // endregion

    // region private helpers
}
