/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.Data;
import com.emc.pravega.controller.store.stream.tables.HistoryRecord;
import com.emc.pravega.controller.store.stream.tables.IndexRecord;
import com.emc.pravega.controller.store.stream.tables.Scale;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Utilities;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public abstract class PersistentStreamBase<T> implements Stream {

    private final String scope;
    private final String name;

    PersistentStreamBase(String scope, final String name) {
        this.scope = scope;
        this.name = name;
    }

    @Override
    public String getScope() {
        return this.scope;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getScopeName() {
        return this.scope;
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

        return checkScopeExists()
                .thenCompose(x -> checkStreamExists(create))
                .thenCompose(x -> storeCreationTime(create))
                .thenCompose(x -> createConfiguration(create))
                .thenCompose(x -> createState(State.CREATING))
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
        return verifyLegalState(checkScopeExists()
                .thenApply(x -> setConfigurationData(configuration))
                .thenApply(x -> true));
    }

    /**
     * Fetch configuration at configurationPath.
     *
     * @return : future of stream configuration
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return verifyLegalState(getConfigurationData());
    }

    @Override
    public CompletableFuture<Boolean> updateState(final State state) {
        return setStateData(state).thenApply(x -> true);
    }

    @Override
    public CompletableFuture<State> getState() {
        return getStateData();
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
        return verifyLegalState(getSegmentRow(number));
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

        return verifyLegalState(CompletableFuture.allOf(futures).thenApply(x -> {
            final Segment segment = (Segment) futures[0].getNow(null);
            final Data<T> indexTable = (Data<T>) futures[1].getNow(null);
            final Data<T> historyTable = (Data<T>) futures[2].getNow(null);
            return TableHelper.findSegmentSuccessorCandidates(segment, indexTable.getData(), historyTable.getData());
        }).thenCompose(candidates -> {
            Segment segment = (Segment) futures[0].getNow(null);
            return findOverlapping(segment, candidates);
        }).thenApply(list -> list.stream().map(e -> e.getNumber()).collect(Collectors.toList())));
    }

    private CompletableFuture<List<Segment>> findOverlapping(Segment segment, List<Integer> candidates) {
        return verifyLegalState(FutureHelpers.allOfWithResults(candidates.stream().map(this::getSegment).collect(Collectors.toList()))
                .thenApply(successorCandidates -> successorCandidates.stream()
                        .filter(x -> x.overlaps(segment))
                        .collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessorsWithPredecessors(final int number) {

        final CompletableFuture[] futures = new CompletableFuture[3];

        futures[0] = getSegment(number);
        futures[1] = getIndexTable();
        futures[2] = getHistoryTable();

        return verifyLegalState(CompletableFuture.allOf(futures)
                .thenCompose(x -> {
                    final Segment segment = (Segment) futures[0].getNow(null);
                    final Data<T> indexTable = (Data<T>) futures[1].getNow(null);
                    final Data<T> historyTable = (Data<T>) futures[2].getNow(null);
                    List<Integer> candidates = TableHelper.findSegmentSuccessorCandidates(segment,
                            indexTable.getData(),
                            historyTable.getData());
                    return findOverlapping(segment, candidates);
                }).thenCompose(successors -> {
                    final Data<T> indexTable = (Data<T>) futures[1].getNow(null);
                    final Data<T> historyTable = (Data<T>) futures[2].getNow(null);
                    List<CompletableFuture<Map.Entry<Segment, List<Integer>>>> result = new ArrayList<>();
                    for (Segment successor : successors) {
                        List<Integer> candidates = TableHelper.findSegmentPredecessorCandidates(successor,
                                indexTable.getData(),
                                historyTable.getData());
                        result.add(findOverlapping(successor, candidates).thenApply(list -> new SimpleImmutableEntry<>(
                                successor, candidates)));
                    }
                    return FutureHelpers.allOfWithResults(result);
                }).thenApply(list -> list.stream().collect(Collectors.toMap(e -> e.getKey().getNumber(), Map.Entry::getValue))));
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

        return verifyLegalState(CompletableFuture.allOf(futures).thenCompose(x -> {
            final Segment segment = (Segment) futures[0].getNow(null);
            final Data<T> indexTable = (Data<T>) futures[1].getNow(null);
            final Data<T> historyTable = (Data<T>) futures[2].getNow(null);
            List<Integer> candidates = TableHelper.findSegmentPredecessorCandidates(segment,
                    indexTable.getData(),
                    historyTable.getData());
            return findOverlapping(segment, candidates);
        }).thenApply(list -> list.stream().map(e -> e.getNumber()).collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<List<Integer>> getActiveSegments() {
        return verifyLegalState(getHistoryTable().thenApply(x -> TableHelper.getActiveSegments(x.getData())));
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

        return verifyLegalState(indexFuture.thenCombine(historyFuture,
                (indexTable, historyTable) -> TableHelper.getActiveSegments(timestamp,
                        indexTable.getData(),
                        historyTable.getData())));
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

        return verifyLegalState(getSegmentChunks()
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
                .thenCompose(startingSegmentNumber -> getSegments(newRanges.size(), startingSegmentNumber)));
    }

    @Override
    public CompletableFuture<UUID> createTransaction() {
        final UUID txId = UUID.randomUUID();
        return verifyLegalState(isBlocked().thenCompose(x -> {
            if (!x) {
                return createNewTransaction(txId, System.currentTimeMillis()).thenApply(y -> txId);
            } else {
                throw new TransactionBlockedException(String.format("Transaction Creation disallowed temporarily for stream %s/%s", scope, name));
            }
        }));
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId) {

        final CompletableFuture<TxnStatus> activeTx = getActiveTx(txId)
                .handle((ok, ex) -> {
                    if (ok == null ||
                            (ex != null && ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException)) {
                        return TxnStatus.UNKNOWN;
                    } else if (ex != null) {
                        throw new CompletionException(ex);
                    }
                    return ActiveTxRecord.parse(ok.getData()).getTxnStatus();
                });

        return verifyLegalState(activeTx
                .thenCompose(x -> {
                    if (x.equals(TxnStatus.UNKNOWN)) {
                        return getCompletedTx(txId)
                                .handle((ok, ex) -> {
                                    if (ok == null ||
                                            (ex != null && ex instanceof DataNotFoundException)) {
                                        return TxnStatus.UNKNOWN;
                                    } else if (ex != null) {
                                        throw new CompletionException(ex);
                                    }
                                    return CompletedTxRecord.parse(ok.getData()).getCompletionStatus();
                                });
                    } else {
                        return CompletableFuture.completedFuture(x);
                    }
                }));
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final UUID txId, final boolean commit) {
        return verifyLegalState(checkTransactionStatus(txId)
                .thenCompose(x -> {
                    if (commit) {
                        switch (x) {
                            case COMMITTING:
                                return CompletableFuture.completedFuture(TxnStatus.COMMITTING);
                            case OPEN:
                                return sealActiveTx(txId, true).thenApply(y -> TxnStatus.COMMITTING);
                            case ABORTING:
                            case ABORTED:
                            case COMMITTED:
                                throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                            default:
                                throw new TransactionNotFoundException(txId.toString());
                        }
                    } else {
                        switch (x) {
                            case ABORTING:
                                return CompletableFuture.completedFuture(TxnStatus.ABORTING);
                            case OPEN:
                                return sealActiveTx(txId, false).thenApply(y -> TxnStatus.ABORTING);
                            case ABORTED:
                            case COMMITTING:
                            case COMMITTED:
                                throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                            default:
                                throw new TransactionNotFoundException(txId.toString());
                        }
                    }
                }));
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final UUID txId) {

        return verifyLegalState(checkTransactionStatus(txId)
                .thenApply(x -> {
                    switch (x) {
                        // Only sealed transactions can be committed
                        case COMMITTED:
                        case COMMITTING:
                            return x;
                        case OPEN:
                        case ABORTING:
                        case ABORTED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "commit");
                        case UNKNOWN:
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                })
                .thenCompose(x -> {
                    if (x.equals(TxnStatus.COMMITTING)) {
                        return createCompletedTxEntry(txId, TxnStatus.COMMITTED, System.currentTimeMillis());
                    } else {
                        return CompletableFuture.completedFuture(null); // already committed, do nothing
                    }
                })
                .thenCompose(x -> removeActiveTxEntry(txId))
                .thenApply(x -> TxnStatus.COMMITTED));
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final UUID txId) {
        return verifyLegalState(checkTransactionStatus(txId)
                .thenApply(x -> {
                    switch (x) {
                        case ABORTING:
                        case ABORTED:
                            return x;
                        case OPEN:
                        case COMMITTING:
                        case COMMITTED:
                            throw new OperationOnTxNotAllowedException(txId.toString(), "aborted");
                        case UNKNOWN:
                        default:
                            throw new TransactionNotFoundException(txId.toString());
                    }
                })
                .thenCompose(x -> {
                    if (x.equals(TxnStatus.ABORTING)) {
                        return createCompletedTxEntry(txId, TxnStatus.ABORTED, System.currentTimeMillis());
                    } else {
                        return CompletableFuture.completedFuture(null); // already committed, do nothing
                    }
                })
                .thenCompose(y -> removeActiveTxEntry(txId))
                .thenApply(y -> TxnStatus.ABORTED));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns() {
        return verifyLegalState(getCurrentTxns()
                .thenApply(x -> x.entrySet().stream()
                        .collect(Collectors.toMap(k -> UUID.fromString(k.getKey()),
                                v -> ActiveTxRecord.parse(v.getValue().getData())))));
    }

    @Override
    public CompletableFuture<Void> setMarker(int segmentNumber, long timestamp) {

        return verifyLegalState(getMarkerData(segmentNumber)
                .thenCompose(x -> {
                    if (x.isPresent()) {
                        final Data<T> data = new Data<>(Utilities.toByteArray(timestamp), x.get().getVersion());
                        return updateMarkerData(segmentNumber, data);
                    } else {
                        return createMarkerData(segmentNumber, timestamp);
                    }
                }));
    }

    @Override
    public CompletableFuture<Optional<Long>> getMarker(int segmentNumber) {
        return verifyLegalState(getMarkerData(segmentNumber)
                .thenApply(x -> x.map(y -> Utilities.toLong(y.getData()))));
    }

    @Override
    public CompletableFuture<Void> removeMarker(int segmentNumber) {
        return verifyLegalState(removeMarkerData(segmentNumber));
    }

    @Override
    public CompletableFuture<Void> blockTransactions() {
        return verifyLegalState(setBlockFlag());
    }

    @Override
    public CompletableFuture<Void> unblockTransactions() {
        return verifyLegalState(unsetBlockFlag());
    }

    private <U> CompletableFuture<U> verifyLegalState(CompletableFuture<U> future) {
        return getState()
                .thenApply(state -> state != null &&
                        !state.equals(State.UNKNOWN) &&
                        !state.equals(State.CREATING))
                .thenCompose(created -> {
                    if (created) {
                        return future;
                    } else {
                        throw new IllegalStateException("stream state unknown");
                    }
                });
    }

    private CompletionStage<List<Segment>> getSegments(final int count,
                                                       final int startingSegmentNumber) {
        final List<CompletableFuture<Segment>> segments = IntStream.range(startingSegmentNumber,
                startingSegmentNumber + count)
                .boxed()
                .map(this::getSegment)
                .collect(Collectors.toList());
        return FutureHelpers.allOfWithResults(segments);
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
     * @param sealedSegments        sealed segments
     * @param scale                 scale input
     * @param startingSegmentNumber starting segment number among new segments created
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

    abstract CompletableFuture<Void> createState(final State state);

    abstract CompletableFuture<Void> setStateData(final State state);

    abstract CompletableFuture<State> getStateData();

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

    abstract CompletableFuture<Void> sealActiveTx(final UUID txId, final boolean commit) throws DataNotFoundException;

    abstract CompletableFuture<Data<T>> getCompletedTx(final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> removeActiveTxEntry(final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp);

    abstract CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp);

    abstract CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<T> data);

    abstract CompletableFuture<Void> removeMarkerData(int segmentNumber);

    abstract CompletableFuture<Optional<Data<T>>> getMarkerData(int segmentNumber);

    abstract CompletableFuture<Void> setBlockFlag();

    abstract CompletableFuture<Void> unsetBlockFlag();

    abstract CompletableFuture<Boolean> isBlocked();

    abstract CompletableFuture<Map<String, Data<T>>> getCurrentTxns();

    abstract CompletableFuture<Void> checkScopeExists() throws StoreException;
}
