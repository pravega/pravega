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
import com.emc.pravega.controller.store.stream.tables.HistoryRecord;
import com.emc.pravega.controller.store.stream.tables.IndexRecord;
import com.emc.pravega.controller.store.stream.tables.Scale;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxStatus;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.AbstractMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class PersistentStreamBase implements Stream {
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
     * 0. Take distributed mutex
     * 1. Create task/Fetch existing task
     * 2. Create a new znode to store configuration
     * 3. Create a new znode for segment table.
     * 4. Create a new znode for history table.
     * 5. Create a new znode for index
     * 6. delete task
     * 7. release mutex
     *
     * @param configuration stream configuration.
     * @return : future of whether it was done or not
     */
    @Override
    public CompletableFuture<Boolean> create(final StreamConfiguration configuration, long createTimestamp) {
        final Create create = new Create(createTimestamp, configuration);
        return createStream(create)
                .thenCompose(x -> createConfiguration(create))
                .thenCompose(x -> createSegmentTable(create))
                .thenCompose(x -> createSegmentFile(create))
                .thenCompose(x -> createHistoryTable(create))
                .thenCompose(x -> createIndexTable(create))
                .thenApply(x -> true);
    }

    /**
     * Update configuration in zk at configurationPath
     *
     * @param configuration new stream configuration.
     * @return : future of boolean
     */
    @Override
    public CompletableFuture<Boolean> updateConfiguration(final StreamConfiguration configuration) {
        // replace the configurationPath zknode with new configurationPath
        return setConfigurationData(configuration).thenApply(x -> true);
    }

    /**
     * Fetch configuration from zk at configurationPath
     *
     * @return : future of stream configuration
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getConfigurationData();
    }

    /**
     * Compute correct znode name for the segment chunk that contains entry for this segment.
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
     * to find successors
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
                final byte[] indexTable = (byte[]) futures[1].get();
                final byte[] historyTable = (byte[]) futures[2].get();
                return FutureCollectionHelper.sequence(
                        TableHelper.findSegmentSuccessorCandidates(segment,
                                indexTable,
                                historyTable)
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
     * Find predecessor candidates and find overlaps with given segment's key range
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
                final byte[] indexTable = (byte[]) futures[1].get();
                final byte[] historyTable = (byte[]) futures[2].get();
                return FutureCollectionHelper.sequence(
                        TableHelper.findSegmentPredecessorCandidates(segment,
                                indexTable,
                                historyTable)
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
        return getHistoryTable().thenApply(TableHelper::getActiveSegments);
    }

    /**
     * if timestamp is < create time of stream, we will return empty list
     * 1. perform binary searchIndex on index table to find timestamp
     * 2. fetch the record from history table for the pointer in index.
     * Note: index may be stale so we may need to fall through
     * 3. parse the row and return the list of integers
     *
     * @param timestamp point in time.
     * @return : list of active segment numbers at given time stamp
     */
    @Override
    public CompletableFuture<List<Integer>> getActiveSegments(long timestamp) {
        final CompletableFuture<byte[]> indexFuture = getIndexTable();

        final CompletableFuture<byte[]> historyFuture = getHistoryTable();

        return indexFuture.thenCombine(historyFuture,
                (byte[] indexTable, byte[] historyTable) -> TableHelper.getActiveSegments(timestamp,
                        indexTable,
                        historyTable));
    }

    /**
     * Scale and create are two tasks where we update the table. For scale to be legitimate, it has to be
     * preceeded by create. Which means all appropriate tables exist.
     * Scale Steps:
     * 0. Take distributed mutex
     * 1. Scale task/Fetch existing task
     * 2. Verify if new scale input is same as existing scale task.
     * If not, existing takes precedence. TODO: Notify caller!
     * 3. Add new segment information in segment table.
     * Segments could spillover into a new chunk.
     * 4. Add entry into the history table.
     * 5. Add entry into the index table.
     * 6. delete task
     * 7. release mutex
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
        UUID txId = UUID.randomUUID();
        return createNewTransaction(txId, System.currentTimeMillis()).thenApply(x -> txId);
    }

    @Override
    public CompletableFuture<TxStatus> checkTransactionStatus(UUID txId) {

        CompletableFuture<TxStatus> activeTx = getActiveTx(txId)
                .handle((ok, ex) -> {
                    if (ok == null || ok.getTxStatus().equals(TxStatus.UNKNOWN) ||
                            (ex != null && ex instanceof DataNotFoundException)) {
                        return TxStatus.UNKNOWN;
                    }
                    else if(ex != null) throw new RuntimeException(ex);
                    else return ok.getTxStatus();
                });

        return activeTx
                .thenCompose(x -> {
                    if (x.equals(TxStatus.UNKNOWN)) {
                        return getCompletedTx(txId)
                                .handle((ok, ex) -> {
                                    if (ok == null || ok.getCompletionStatus().equals(TxStatus.UNKNOWN) ||
                                            (ex != null && ex instanceof DataNotFoundException)) {
                                        return TxStatus.UNKNOWN;
                                    }
                                    else if(ex != null) throw new RuntimeException(ex);
                                    else return ok.getCompletionStatus();
                                });
                    } else
                        return CompletableFuture.completedFuture(x);
                });
    }

    @Override
    public CompletableFuture<TxStatus> sealTransaction(UUID txId) {
        return checkTransactionStatus(txId)
                .thenCompose(x -> {
                    switch (x) {
                        case SEALED:
                            return CompletableFuture.completedFuture(TxStatus.SEALED);
                        case OPEN:
                            return sealActiveTx(txId).thenApply(y -> TxStatus.SEALED);
                        case DROPPED:
                        case COMMITTED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                });
    }

    @Override
    public CompletableFuture<TxStatus> commitTransaction(UUID txId) {

        return checkTransactionStatus(txId)
                .thenApply(x -> {
                    switch (x) {
                        // Only sealed transactions can be committed
                        case COMMITTED:
                        case SEALED:
                            return x;
                        case OPEN:
                        case DROPPED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "commit");
                        case UNKNOWN:
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                })
                .thenCompose(x -> {
                    if (x.equals(TxStatus.SEALED))
                        return createCompletedTxEntry(txId, TxStatus.COMMITTED, System.currentTimeMillis());
                    else return CompletableFuture.completedFuture(null); // already committed, do nothing
                })
                .thenCompose(x -> removeActiveTxEntry(txId))
                .thenApply(x -> TxStatus.COMMITTED);
    }

    @Override
    public CompletableFuture<TxStatus> dropTransaction(UUID txId) {
        return checkTransactionStatus(txId)
                .thenApply(x -> {
                    switch (x) {
                        case OPEN:
                        case SEALED:
                        case DROPPED:
                            return x;
                        case COMMITTED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "dropped");
                        case UNKNOWN:
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                })
                .thenCompose(x -> createCompletedTxEntry(txId, TxStatus.DROPPED, System.currentTimeMillis())
                        .thenCompose(y -> removeActiveTxEntry(txId))
                        .thenApply(y -> TxStatus.DROPPED));
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
     * @param scale
     * @param currentSegmentData
     * @return : return starting segment number
     */
    private CompletableFuture<Integer> addNewSegments(
            final Scale scale,
            final ImmutablePair<Integer, byte[]> currentSegmentData) {
        final int currentChunk = currentSegmentData.left;
        final byte[] currentChunkData = currentSegmentData.right;

        final int startingSegmentNumber = currentChunk * SegmentRecord.SEGMENT_CHUNK_SIZE +
                (currentChunkData.length / SegmentRecord.SEGMENT_RECORD_SIZE);

        // idempotent check
        final Segment lastSegment = TableHelper.getSegment(startingSegmentNumber - 1, currentChunkData);
        if (lastSegment.getStart() == scale.getScaleTimestamp()) {
            return CompletableFuture.completedFuture(lastSegment.getNumber() - scale.getNewRanges().size() + 1);
        }

        final int maxSegmentNumberForChunk = (currentChunk + 1) * SegmentRecord.SEGMENT_CHUNK_SIZE - 1;

        final int toCreate = Integer.min(maxSegmentNumberForChunk - startingSegmentNumber + 1,
                scale.getNewRanges().size());

        final byte[] updatedChunkData = TableHelper.updateSegmentTable(startingSegmentNumber,
                currentChunkData,
                toCreate,
                scale.getNewRanges(),
                scale.getScaleTimestamp()
        );

        return setSegmentTableChunk(currentChunk, updatedChunkData)
                .thenCompose(y -> {
                    final int chunkNumber = TableHelper.getSegmentChunkNumber(startingSegmentNumber + scale.getNewRanges().size());
                    final int remaining = Integer.max(scale.getNewRanges().size() - toCreate, 0);

                    if (remaining > 0) {
                        byte[] newChunk = TableHelper.updateSegmentTable(chunkNumber * SegmentRecord.SEGMENT_CHUNK_SIZE,
                                new byte[0], // new chunk
                                remaining,
                                scale.getNewRanges(),
                                scale.getScaleTimestamp()
                        );

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
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable);

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

                    final byte[] updated = TableHelper.updateHistoryTable(historyTable,
                            scale.getScaleTimestamp(),
                            newActiveSegments);

                    return updateHistoryTable(updated).thenApply(y -> lastRecord.getEndOfRowPointer() + 1);
                });
    }

    private List<Integer> getNewActiveSegments(List<Integer> sealedSegments,
                                               Scale scale,
                                               int startingSegmentNumber,
                                               HistoryRecord lastRecord) {
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
                    final Optional<IndexRecord> lastRecord = IndexRecord.readLatestRecord(indexTable);
                    // check idempotent
                    if (lastRecord.isPresent() && lastRecord.get().getEventTime() >= scale.getScaleTimestamp())
                        return CompletableFuture.completedFuture(null);

                    final byte[] updated = TableHelper.updateIndexTable(indexTable,
                            scale.getScaleTimestamp(),
                            historyOffset);
                    return updateIndexTable(updated);
                });
    }

    private CompletionStage<ImmutablePair<Integer, byte[]>> getLatestChunk(final List<String> segmentChunks) {
        assert segmentChunks.size() > 0;

        final int latestChunkNumber = segmentChunks.size() - 1;

        return getSegmentTableChunk(latestChunkNumber)
                .thenApply(segmentTableChunk -> new ImmutablePair<>(latestChunkNumber, segmentTableChunk));
    }

    abstract CompletableFuture<Boolean> createStream(Create create) throws StreamAlreadyExistsException;

    abstract CompletableFuture<Void> createConfiguration(Create create);

    abstract CompletableFuture<Void> setConfigurationData(StreamConfiguration configuration);

    abstract CompletableFuture<StreamConfiguration> getConfigurationData();

    abstract CompletableFuture<Void> createSegmentTable(Create create);

    abstract CompletableFuture<Void> createSegmentChunk(int chunkNumber, byte[] data);

    abstract CompletableFuture<List<String>> getSegmentChunks();

    abstract CompletableFuture<Segment> getSegmentRow(int number);

    abstract CompletableFuture<byte[]> getSegmentTableChunk(int chunkNumber);

    abstract CompletableFuture<Void> setSegmentTableChunk(int chunkNumber, byte[] data);

    abstract CompletableFuture<Void> createIndexTable(Create create);

    abstract CompletableFuture<byte[]> getIndexTable();

    abstract CompletableFuture<Void> updateIndexTable(byte[] updated);

    abstract CompletableFuture<Void> createHistoryTable(Create create);

    abstract CompletableFuture<Void> updateHistoryTable(byte[] updated);

    abstract CompletableFuture<byte[]> getHistoryTable();

    abstract CompletableFuture<Void> createSegmentFile(Create create);

    abstract CompletableFuture<Void> createNewTransaction(UUID txId, long timestamp);

    abstract CompletableFuture<ActiveTxRecord> getActiveTx(UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> sealActiveTx(UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<CompletedTxRecord> getCompletedTx(UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> removeActiveTxEntry(UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(UUID txId, TxStatus complete, long timestamp);
}
