/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.Data;
import com.emc.pravega.controller.store.stream.tables.HistoryRecord;
import com.emc.pravega.controller.store.stream.tables.IndexRecord;
import com.emc.pravega.controller.store.stream.tables.Scale;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.AbstractMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class PersistentStreamBase<T> implements Stream {
    private final String name;


    protected PersistentStreamBase(final String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    /***
     * Creates a new stream record in the stream store.
     * Create a new task of type Create.
     * If create task already exists, use that and bring it to completion
     * If no task exists, fall through all create steps. They are all idempotent
     * <p>
     * Create Steps:
     * 1. Create new store configuration
     * 2. Create new segment table.
     * 3. Create new history table.
     * 4. Create new index
     *
     * @param configuration stream configuration.
     * @return : future of whether it was done or not
     */
    @Override
    public CompletableFuture<Boolean> create(final StreamConfiguration configuration, long createTimestamp) {
        final Create create = new Create(createTimestamp, configuration);
        return checkStreamExists(create)
                .thenCompose(x -> storeCreationTime(create))
                .thenCompose(x -> createConfiguration(create))
                .thenCompose(x -> createSegmentTable(create))
                .thenCompose(x -> createSegmentFile(create))
                .thenCompose(x -> createHistoryTable(create))
                .thenCompose(x -> createIndexTable(create))
                .thenApply(x -> true);
    }

    /**
     * Update configuration at configurationPath.
     *
     * @param configuration new stream configuration.
     * @return : future of boolean
     */
    @Override
    public CompletableFuture<Boolean> updateConfiguration(final StreamConfiguration configuration) {
        // replace the configurationPath with new configurationPath
        return setConfigurationData(configuration).thenApply(x -> true);
    }

    /**
     * Fetch configuration at configurationPath.
     *
     * @return : future of stream configuration
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getConfigurationData();
    }

    /**
     * Compute correct name for the segment chunk that contains entry for this segment.
     * Fetch the segment table chunk and retrieve the segment
     *
     * @param number segment number.
     * @return : future of segment
     */
    @Override
    public CompletableFuture<Segment> getSegment(final int number) {
        return getSegmentRow(number);
    }

    /**
     * Given segment number, find its successor candidates and then compute overlaps with its keyrange
     * to find successors.
     *
     * @param number segment number.
     * @return : future of list of successor segment numbers
     */
    @Override
    public CompletableFuture<List<Integer>> getSuccessors(final int number) {

        final CompletableFuture[] futures = new CompletableFuture[3];

        futures[0] = getSegment(number);
        futures[1] = getIndexTable();
        futures[2] = getHistoryTable();

        return CompletableFuture.allOf(futures).thenCompose(x -> {

            try {
                final Segment segment = (Segment) futures[0].get();
                final Data<T> indexTable = (Data<T>) futures[1].get();
                final Data<T> historyTable = (Data<T>) futures[2].get();
                return FutureCollectionHelper.sequence(
                        TableHelper.findSegmentSuccessorCandidates(segment,
                                indexTable.getData(),
                                historyTable.getData())
                                .stream()
                                .map(this::getSegment)
                                .collect(Collectors.toList()))
                        .thenApply(successorCandidates -> new ImmutablePair<>(segment, successorCandidates));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> TableHelper.getOverlaps(x.getKey(), x.getValue()));
    }

    /**
     * Find predecessor candidates and find overlaps with given segment's key range.
     *
     * @param number segment number.
     * @return : future of list of predecessor segment numbers
     */
    @Override
    public CompletableFuture<List<Integer>> getPredecessors(final int number) {
        final CompletableFuture[] futures = new CompletableFuture[3];

        futures[0] = getSegment(number);
        futures[1] = getIndexTable();
        futures[2] = getHistoryTable();

        return CompletableFuture.allOf(futures).thenCompose(x -> {
            try {
                final Segment segment = (Segment) futures[0].get();
                final Data<T> indexTable = (Data<T>) futures[1].get();
                final Data<T> historyTable = (Data<T>) futures[2].get();
                return FutureCollectionHelper.sequence(
                        TableHelper.findSegmentPredecessorCandidates(segment,
                                indexTable.getData(),
                                historyTable.getData())
                                .stream()
                                .map(this::getSegment)
                                .collect(Collectors.toList()))
                        .thenApply(predecessorCandidates -> new ImmutablePair<>(segment, predecessorCandidates));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> TableHelper.getOverlaps(x.getKey(), x.getValue()));
    }

    @Override
    public CompletableFuture<List<Integer>> getActiveSegments() {
        return getHistoryTable().thenApply(x -> TableHelper.getActiveSegments(x.getData()));
    }

    /**
     * if timestamp is < create time of stream, we will return empty list.
     * 1. perform binary searchIndex on index table to find timestamp
     * 2. fetch the record from history table for the pointer in index.
     * Note: index may be stale so we may need to fall through
     * 3. parse the row and return the list of integers
     *
     * @param timestamp point in time.
     * @return : list of active segment numbers at given time stamp
     */
    @Override
    public CompletableFuture<List<Integer>> getActiveSegments(final long timestamp) {
        final CompletableFuture<Data<T>> indexFuture = getIndexTable();

        final CompletableFuture<Data<T>> historyFuture = getHistoryTable();

        return indexFuture.thenCombine(historyFuture,
                (indexTable, historyTable) -> TableHelper.getActiveSegments(timestamp,
                        indexTable.getData(),
                        historyTable.getData()));
    }

    /**
     * Scale and create are two tasks where we update the table. For scale to be legitimate, it has to be
     * preceeded by create. Which means all appropriate tables exist.
     * Scale Steps:
     * 1. Add new segment information in segment table.
     * Segments could spillover into a new chunk.
     * 2. Add entry into the history table.
     * 3. Add entry into the index table.
     *
     * @param sealedSegments segments to be sealed
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<List<Segment>> scale(final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp) {
        final Scale scale = new Scale(sealedSegments, newRanges, scaleTimestamp);

        return getSegmentChunks()
                .thenCompose(this::getLatestChunk)
                .thenCompose(latestSegmentData -> addNewSegments(scale, latestSegmentData))
                .thenCompose(startingSegmentNumber ->
                        addHistoryRecord(sealedSegments, scale, startingSegmentNumber)
                                .thenApply(historyOffset -> new ImmutablePair<>(startingSegmentNumber, historyOffset)))
                .thenCompose(pair -> {
                    final int historyOffset = pair.right;
                    final int startingSegmentNumber = pair.left;

                    return addIndexRecord(scale, historyOffset).thenApply(y -> startingSegmentNumber);
                })
                .thenCompose(startingSegmentNumber -> getSegments(newRanges.size(), startingSegmentNumber));
    }

    @Override
    public CompletableFuture<UUID> createTransaction() {
        final UUID txId = UUID.randomUUID();
        return createNewTransaction(txId, System.currentTimeMillis()).thenApply(x -> txId);
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId) {

        final CompletableFuture<TxnStatus> activeTx = getActiveTx(txId)
                .handle((ok, ex) -> {
                    if (ok == null ||
                            (ex != null && ex instanceof DataNotFoundException)) {
                        return TxnStatus.UNKNOWN;
                    } else if (ex != null) {
                        throw new RuntimeException(ex);
                    } else {
                        return ActiveTxRecord.parse(ok.getData()).getTxnStatus();
                    }
                });

        return activeTx
                .thenCompose(x -> {
                    if (x.equals(TxnStatus.UNKNOWN)) {
                        return getCompletedTx(txId)
                                .handle((ok, ex) -> {
                                    if (ok == null ||
                                            (ex != null && ex instanceof DataNotFoundException)) {
                                        return TxnStatus.UNKNOWN;
                                    } else if (ex != null) {
                                        throw new RuntimeException(ex);
                                    } else {
                                        return CompletedTxRecord.parse(ok.getData()).getCompletionStatus();
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(x);
                    }
                });
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final UUID txId) {
        return checkTransactionStatus(txId)
                .thenCompose(x -> {
                    switch (x) {
                        case SEALED:
                            return CompletableFuture.completedFuture(TxnStatus.SEALED);
                        case OPEN:
                            return sealActiveTx(txId).thenApply(y -> TxnStatus.SEALED);
                        case ABORTED:
                        case COMMITTED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                });
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final UUID txId) {

        return checkTransactionStatus(txId)
                .thenApply(x -> {
                    switch (x) {
                        // Only sealed transactions can be committed
                        case COMMITTED:
                        case SEALED:
                            return x;
                        case OPEN:
                        case ABORTED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "commit");
                        case UNKNOWN:
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                })
                .thenCompose(x -> {
                    if (x.equals(TxnStatus.SEALED)) {
                        return createCompletedTxEntry(txId, TxnStatus.COMMITTED, System.currentTimeMillis());
                    } else {
                        return CompletableFuture.completedFuture(null); // already committed, do nothing
                    }
                })
                .thenCompose(x -> removeActiveTxEntry(txId))
                .thenApply(x -> TxnStatus.COMMITTED);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final UUID txId) {
        return checkTransactionStatus(txId)
                .thenApply(x -> {
                    switch (x) {
                        case OPEN:
                        case SEALED:
                        case ABORTED:
                            return x;
                        case COMMITTED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "aborted");
                        case UNKNOWN:
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                })
                .thenCompose(x -> createCompletedTxEntry(txId, TxnStatus.ABORTED, System.currentTimeMillis())
                        .thenCompose(y -> removeActiveTxEntry(txId))
                        .thenApply(y -> TxnStatus.ABORTED));
    }

    private CompletionStage<List<Segment>> getSegments(final int count,
                                                       final int startingSegmentNumber) {
        final List<CompletableFuture<Segment>> segments = IntStream.range(startingSegmentNumber,
                startingSegmentNumber + count)
                .boxed()
                .map(this::getSegment)
                .collect(Collectors.<CompletableFuture<Segment>>toList());
        return FutureCollectionHelper.sequence(segments);
    }

    /**
     * Add new segments, return the starting segment number.
     *
     * @param scale              scale
     * @param currentSegmentData current segment data
     * @return : return starting segment number
     */
    private CompletableFuture<Integer> addNewSegments(
            final Scale scale,
            final ImmutablePair<Integer, Data<T>> currentSegmentData) {
        final int currentChunk = currentSegmentData.left;
        final Data<T> currentChunkData = currentSegmentData.right;

        final int startingSegmentNumber = currentChunk * SegmentRecord.SEGMENT_CHUNK_SIZE +
                (currentChunkData.getData().length / SegmentRecord.SEGMENT_RECORD_SIZE);

        // idempotent check
        final Segment lastSegment = TableHelper.getSegment(startingSegmentNumber - 1, currentChunkData.getData());
        if (lastSegment.getStart() == scale.getScaleTimestamp()) {
            return CompletableFuture.completedFuture(lastSegment.getNumber() - scale.getNewRanges().size() + 1);
        }

        final int maxSegmentNumberForChunk = (currentChunk + 1) * SegmentRecord.SEGMENT_CHUNK_SIZE - 1;

        final int toCreate = Integer.min(maxSegmentNumberForChunk - startingSegmentNumber + 1,
                scale.getNewRanges().size());

        final byte[] updated = TableHelper.updateSegmentTable(startingSegmentNumber,
                currentChunkData.getData(),
                toCreate,
                scale.getNewRanges(),
                scale.getScaleTimestamp()
        );

        final Data<T> updatedChunkData = new Data<>(updated, currentChunkData.getVersion());

        return setSegmentTableChunk(currentChunk, updatedChunkData)
                .thenCompose(y -> {
                    final int chunkNumber = TableHelper.getSegmentChunkNumber(startingSegmentNumber + scale.getNewRanges().size());
                    final int remaining = Integer.max(scale.getNewRanges().size() - toCreate, 0);

                    if (remaining > 0) {
                        final byte[] newSegmentChunk = TableHelper.updateSegmentTable(chunkNumber * SegmentRecord.SEGMENT_CHUNK_SIZE,
                                new byte[0], // new chunk
                                remaining,
                                scale.getNewRanges(),
                                scale.getScaleTimestamp());
                        final Data<T> newChunk = new Data<>(newSegmentChunk, null);

                        return createSegmentChunk(chunkNumber, newChunk);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenApply(x -> startingSegmentNumber);
    }

    /**
     * update history table if not already updated:
     * fetch last record from history table.
     * if eventTime is >= scale.scaleTimeStamp do nothing, else create record
     *
     * @param sealedSegments
     * @param scale
     * @return : future of history table offset for last entry
     */
    private CompletableFuture<Integer> addHistoryRecord(final List<Integer> sealedSegments,
                                                        final Scale scale,
                                                        final int startingSegmentNumber) {
        return getHistoryTable()
                .thenCompose(historyTable -> {
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable.getData());

                    // scale task is not allowed unless create is done which means at least one
                    // record in history table
                    assert lastRecordOpt.isPresent();

                    final HistoryRecord lastRecord = lastRecordOpt.get();

                    // idempotent check
                    if (lastRecord.getEventTime() >= scale.getScaleTimestamp()) {
                        assert lastRecord.getSegments().contains(startingSegmentNumber);

                        return CompletableFuture.completedFuture(lastRecord.getStartOfRowPointer());
                    }

                    final List<Integer> newActiveSegments = getNewActiveSegments(sealedSegments,
                            scale,
                            startingSegmentNumber,
                            lastRecord);

                    byte[] updatedTable = TableHelper.updateHistoryTable(historyTable.getData(),
                            scale.getScaleTimestamp(),
                            newActiveSegments);
                    final Data<T> updated = new Data<>(updatedTable, historyTable.getVersion());

                    return updateHistoryTable(updated).thenApply(y -> lastRecord.getEndOfRowPointer() + 1);
                });
    }

    private List<Integer> getNewActiveSegments(final List<Integer> sealedSegments,
                                               final Scale scale,
                                               final int startingSegmentNumber,
                                               final HistoryRecord lastRecord) {
        final List<Integer> segments = lastRecord.getSegments();
        segments.removeAll(sealedSegments);
        segments.addAll(
                IntStream.range(startingSegmentNumber,
                        startingSegmentNumber + scale.getNewRanges().size())
                        .boxed()
                        .collect(Collectors.toList()));
        return segments;
    }

    private CompletionStage<Void> addIndexRecord(final Scale scale,
                                                 final int historyOffset) {
        return getIndexTable()
                .thenCompose(indexTable -> {
                    final Optional<IndexRecord> lastRecord = IndexRecord.readLatestRecord(indexTable.getData());
                    // check idempotent
                    if (lastRecord.isPresent() && lastRecord.get().getEventTime() >= scale.getScaleTimestamp()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    final byte[] updatedTable = TableHelper.updateIndexTable(indexTable.getData(),
                            scale.getScaleTimestamp(),
                            historyOffset);
                    final Data<T> updated = new Data<>(updatedTable, indexTable.getVersion());
                    return updateIndexTable(updated);
                });
    }

    private CompletionStage<ImmutablePair<Integer, Data<T>>> getLatestChunk(final List<String> segmentChunks) {
        assert segmentChunks.size() > 0;

        final int latestChunkNumber = segmentChunks.size() - 1;

        return getSegmentTableChunk(latestChunkNumber)
                .thenApply(segmentTableChunk -> new ImmutablePair<>(latestChunkNumber, segmentTableChunk));
    }

    abstract CompletableFuture<Void> checkStreamExists(final Create create) throws StreamAlreadyExistsException;

    abstract CompletableFuture<Void> storeCreationTime(final Create create);

    abstract CompletableFuture<Void> createConfiguration(final Create create);

    abstract CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration);

    abstract CompletableFuture<StreamConfiguration> getConfigurationData();

    abstract CompletableFuture<Void> createSegmentTable(final Create create);

    abstract CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final Data<T> data);

    abstract CompletableFuture<List<String>> getSegmentChunks();

    abstract CompletableFuture<Segment> getSegmentRow(final int number);

    abstract CompletableFuture<Data<T>> getSegmentTableChunk(final int chunkNumber);

    abstract CompletableFuture<Void> setSegmentTableChunk(final int chunkNumber, final Data<T> data);

    abstract CompletableFuture<Void> createIndexTable(final Create create);

    abstract CompletableFuture<Data<T>> getIndexTable();

    abstract CompletableFuture<Void> updateIndexTable(final Data<T> updated);

    abstract CompletableFuture<Void> createHistoryTable(final Create create);

    abstract CompletableFuture<Void> updateHistoryTable(final Data<T> updated);

    abstract CompletableFuture<Data<T>> getHistoryTable();

    abstract CompletableFuture<Void> createSegmentFile(final Create create);

    abstract CompletableFuture<Void> createNewTransaction(final UUID txId, final long timestamp);

    abstract CompletableFuture<Data<T>> getActiveTx(final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> sealActiveTx(final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Data<T>> getCompletedTx(final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> removeActiveTxEntry(final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp);
}
