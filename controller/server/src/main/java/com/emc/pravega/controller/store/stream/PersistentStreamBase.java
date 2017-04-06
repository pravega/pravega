/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.Data;
import com.emc.pravega.controller.store.stream.tables.HistoryRecord;
import com.emc.pravega.controller.store.stream.tables.IndexRecord;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Utilities;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
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

import static com.emc.pravega.controller.store.stream.tables.TableHelper.findSegmentPredecessorCandidates;
import static com.emc.pravega.controller.store.stream.tables.TableHelper.findSegmentSuccessorCandidates;

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
                .thenCompose(x -> {
                    final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
                    final byte[] historyTable = TableHelper.createHistoryTable(create.getEventTime(),
                            IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

                    return createHistoryTable(new Data<>(historyTable, null));
                })
                .thenCompose(x -> createIndexTable(new Data<>(TableHelper.createIndexTable(create.getEventTime(), 0), null)))
                .thenApply(x -> true);
    }

    @Override
    public CompletableFuture<Void> delete() {
        return deleteStream();
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
        return getConfigurationData();
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

    @Override
    public CompletableFuture<Integer> getSegmentCount() {
        return verifyLegalState(getHistoryTable().thenApply(x -> TableHelper.getSegmentCount(x.getData())));
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
        return verifyLegalState(
                getSuccessorsForSegment(number).thenApply(list ->
                        list.stream().map(Segment::getNumber).collect(Collectors.toList())));
    }

    private CompletableFuture<List<Segment>> findOverlapping(Segment segment, List<Integer> candidates) {
        return verifyLegalState(FutureHelpers.allOfWithResults(candidates.stream().map(this::getSegment).collect(Collectors.toList()))
                .thenApply(successorCandidates -> successorCandidates.stream()
                        .filter(x -> x.overlaps(segment))
                        .collect(Collectors.toList())));
    }

    private CompletableFuture<List<Segment>> getSuccessorsForSegment(final int number) {
        val segmentFuture = getSegment(number);
        val indexTableFuture = getIndexTable();
        val historyTableFuture = getHistoryTable();
        CompletableFuture<Void> all = CompletableFuture.allOf(segmentFuture, indexTableFuture, historyTableFuture);

        return all.thenCompose(x -> {
            final Segment segment = segmentFuture.getNow(null);
            List<Integer> candidates = findSegmentSuccessorCandidates(segment,
                    indexTableFuture.getNow(null).getData(),
                    historyTableFuture.getNow(null).getData());
            return findOverlapping(segment, candidates);
        });
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessorsWithPredecessors(final int number) {
        val indexTableFuture = getIndexTable();
        val historyTableFuture = getHistoryTable();
        CompletableFuture<List<Segment>> segments = getSuccessorsForSegment(number);

        CompletableFuture<Void> all = CompletableFuture.allOf(segments, indexTableFuture, historyTableFuture);

        CompletableFuture<Map<Integer, List<Integer>>> result = all.thenCompose(v -> {
            List<CompletableFuture<Map.Entry<Segment, List<Integer>>>> resultFutures = new ArrayList<>();
            List<Segment> successors = segments.getNow(null);
            for (Segment successor : successors) {
                List<Integer> candidates = findSegmentPredecessorCandidates(successor,
                        indexTableFuture.getNow(null).getData(),
                        historyTableFuture.getNow(null).getData());
                resultFutures.add(findOverlapping(successor, candidates).thenApply(
                        list -> new SimpleImmutableEntry<>(successor, candidates)));
            }
            return FutureHelpers.allOfWithResults(resultFutures);
        }).thenApply(list -> list.stream().collect(Collectors.toMap(e -> e.getKey().getNumber(), Map.Entry::getValue)));
        return verifyLegalState(result);
    }

    /**
     * Find predecessor candidates and find overlaps with given segment's key range.
     *
     * @param number segment number.
     * @return : future of list of predecessor segment numbers
     */
    @Override
    public CompletableFuture<List<Integer>> getPredecessors(final int number) {
        val segmentFuture = getSegment(number);
        val indexTableFuture = getIndexTable();
        val historyTableFuture = getHistoryTable();
        CompletableFuture<Void> all = CompletableFuture.allOf(segmentFuture, indexTableFuture, historyTableFuture);

        return verifyLegalState(all.thenCompose(x -> {
            final Segment segment = segmentFuture.getNow(null);
            List<Integer> candidates = TableHelper.findSegmentPredecessorCandidates(segment,
                    indexTableFuture.getNow(null).getData(),
                    historyTableFuture.getNow(null).getData());
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
     * preceded by create. Which means all appropriate tables exist.
     * Scale Steps:
     * 1. Add new segment information in segment table.
     * Segments could spillover into a new chunk.
     * 2. Set the state of stream to SCALING.
     *
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<List<Segment>> startScale(final List<Integer> sealedSegments,
                                                       final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                       final long scaleTimestamp) {
        return verifyLegalState(getSegmentChunks()
                .thenCompose(this::getLatestChunk)
                .thenCompose(latestSegmentData -> addNewSegments(newRanges, scaleTimestamp, latestSegmentData))
                .thenCompose(startingSegmentNumber -> updateState(State.SCALING).thenApply(x -> startingSegmentNumber))
                .thenCompose(startingSegmentNumber -> getSegments(IntStream.range(startingSegmentNumber,
                        startingSegmentNumber + newRanges.size())
                        .boxed()
                        .collect(Collectors.toList()))));
    }

    /**
     * Segments created with pravega, update the history table with this fact so they are available as successors
     * 3. Add entry into the history table.
     *
     * @param sealedSegments segments to be sealed
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> continueScale(final List<Integer> sealedSegments,
                                                 final List<Integer> newSegments,
                                                 final long scaleTimestamp) {
        return verifyLegalState(FutureHelpers.toVoid(addPartialHistoryRecord(sealedSegments, newSegments)));
    }

    /**
     * Remainder of scale metadata update. Also set the state back to active.
     * 4. complete entry into the history table.
     * 5. Add entry into the index table.
     *
     * @param sealedSegments segments to be sealed
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> completeScale(final List<Integer> sealedSegments,
                                                 final List<Integer> newSegments,
                                                 final long scaleTimestamp) {
        return verifyLegalState(FutureHelpers.toVoid(
                completeHistoryRecord(scaleTimestamp, sealedSegments, newSegments)
                        .thenCompose(this::addIndexRecord)
                        .thenCompose(v -> updateState(State.ACTIVE))));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final long lease, final long maxExecutionTime,
                                                                         final long scaleGracePeriod) {
        final UUID txId = UUID.randomUUID();
        final long current = System.currentTimeMillis();
        return verifyLegalState(createNewTransaction(txId, current, current + lease, current + maxExecutionTime, scaleGracePeriod)
                .thenApply(x ->
                        new VersionedTransactionData(txId, 0, TxnStatus.OPEN, current,
                                current + maxExecutionTime, scaleGracePeriod)));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final UUID txId, final long lease) {
        return getActiveTx(txId)
                .thenCompose(data -> {
                    ActiveTxRecord activeTxRecord = ActiveTxRecord.parse(data.getData());
                    if (activeTxRecord.getTxnStatus() == TxnStatus.OPEN) {
                        // Update txn record with new lease value and return versioned tx data.
                        ActiveTxRecord newData = new ActiveTxRecord(activeTxRecord.getTxCreationTimestamp(),
                                System.currentTimeMillis() + lease, activeTxRecord.getMaxExecutionExpiryTime(),
                                activeTxRecord.getScaleGracePeriod(), activeTxRecord.getTxnStatus());

                        return updateActiveTx(txId, newData.toByteArray())
                                .thenApply(x ->
                                        new VersionedTransactionData(txId, data.getVersion() + 1,
                                                TxnStatus.OPEN, activeTxRecord.getTxCreationTimestamp(),
                                                activeTxRecord.getMaxExecutionExpiryTime(),
                                                activeTxRecord.getScaleGracePeriod()));
                    } else {
                        return FutureHelpers.failedFuture(new IllegalStateException(txId.toString()));
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId) {
        return getActiveTx(txId)
                .thenApply(data -> {
                    ActiveTxRecord activeTxRecord = ActiveTxRecord.parse(data.getData());
                    return new VersionedTransactionData(txId, data.getVersion(),
                            activeTxRecord.getTxnStatus(), activeTxRecord.getTxCreationTimestamp(),
                            activeTxRecord.getMaxExecutionExpiryTime(), activeTxRecord.getScaleGracePeriod());
                });
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
    public CompletableFuture<TxnStatus> sealTransaction(final UUID txId, final boolean commit,
                                                        final Optional<Integer> version) {
        return verifyLegalState(checkTransactionStatus(txId)
                .thenCompose(x -> {
                    if (commit) {
                        switch (x) {
                            case COMMITTING:
                                return CompletableFuture.completedFuture(TxnStatus.COMMITTING);
                            case OPEN:
                                return sealActiveTx(txId, true, version).thenApply(y -> TxnStatus.COMMITTING);
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
                                return sealActiveTx(txId, false, version).thenApply(y -> TxnStatus.ABORTING);
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
    public CompletableFuture<Void> setColdMarker(int segmentNumber, long timestamp) {

        return verifyLegalState(getMarkerData(segmentNumber)
                .thenCompose(x -> {
                    if (x != null) {
                        final Data<T> data = new Data<>(Utilities.toByteArray(timestamp), x.getVersion());
                        return updateMarkerData(segmentNumber, data);
                    } else {
                        return createMarkerData(segmentNumber, timestamp);
                    }
                }));
    }

    @Override
    public CompletableFuture<Long> getColdMarker(int segmentNumber) {
        return verifyLegalState(getMarkerData(segmentNumber)
                .thenApply(x -> (x != null) ? BitConverter.readLong(x.getData(), 0) : 0L));
    }

    @Override
    public CompletableFuture<Void> removeColdMarker(int segmentNumber) {
        return verifyLegalState(removeMarkerData(segmentNumber));
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
                        throw new IllegalStateException("stream state unknown or stream is still being created");
                    }
                });
    }

    private CompletableFuture<List<Segment>> getSegments(final List<Integer> segments) {
        return FutureHelpers.allOfWithResults(segments.stream().map(this::getSegment)
                .collect(Collectors.toList()));
    }

    /**
     * Add new segments, return the starting segment number.
     *
     * @param currentSegmentData current segment data
     * @return : return starting segment number
     */
    private CompletableFuture<Integer> addNewSegments(
            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
            final long segmentCreationTs,
            final ImmutablePair<Integer, Data<T>> currentSegmentData) {
        final int currentChunk = currentSegmentData.left;
        final Data<T> currentChunkData = currentSegmentData.right;
        final int nextSegmentNumber = TableHelper.getNextSegmentNumber(currentChunk, currentChunkData.getData());

        // idempotent check
        final Segment lastSegment = TableHelper.getSegment(nextSegmentNumber - 1, currentChunkData.getData());
        if (lastSegment.getStart() == segmentCreationTs) {
            return CompletableFuture.completedFuture(lastSegment.getNumber() - newRanges.size() + 1);
        }

        final int maxSegmentNumberForChunk = (currentChunk + 1) * SegmentRecord.SEGMENT_CHUNK_SIZE - 1;

        final int toCreate = Integer.min(maxSegmentNumberForChunk - nextSegmentNumber + 1,
                newRanges.size());

        final byte[] updated = TableHelper.updateSegmentTable(nextSegmentNumber,
                currentChunkData.getData(),
                newRanges.subList(0, toCreate),
                segmentCreationTs
        );

        final Data<T> updatedChunkData = new Data<>(updated, currentChunkData.getVersion());

        return setSegmentTableChunk(currentChunk, updatedChunkData)
                .thenCompose(y -> {
                    final int chunkNumber = TableHelper.getSegmentChunkNumber(nextSegmentNumber + newRanges.size());
                    final int remaining = Integer.max(newRanges.size() - toCreate, 0);

                    if (remaining > 0) {
                        final byte[] newSegmentChunk = TableHelper.updateSegmentTable(chunkNumber * SegmentRecord.SEGMENT_CHUNK_SIZE,
                                new byte[0], // new chunk
                                newRanges.subList(toCreate + 1, newRanges.size()),
                                segmentCreationTs);
                        final Data<T> newChunk = new Data<>(newSegmentChunk, null);

                        return createSegmentChunk(chunkNumber, newChunk);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenApply(x -> nextSegmentNumber);
    }

    /**
     * update history table if not already updated:
     * fetch last record from history table.
     * if eventTime is >= scale.scaleTimeStamp do nothing, else create record
     *
     * @return : future of history table offset for last entry
     */
    private CompletableFuture<Void> addPartialHistoryRecord(final List<Integer> sealedSegments,
                                                            final List<Integer> createdSegments) {
        return getHistoryTable()
                .thenCompose(historyTable -> {
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable.getData(), false);

                    // scale task is not allowed unless create is done which means at least one
                    // record in history table.
                    assert lastRecordOpt.isPresent();

                    final HistoryRecord lastRecord = lastRecordOpt.get();

                    // idempotent check
                    if (lastRecord.getSegments().containsAll(createdSegments)) {
                        HistoryRecord previous = HistoryRecord.fetchPrevious(lastRecord, historyTable.getData()).get();

                        assert previous.getSegments().stream().noneMatch(createdSegments::contains);
                        return CompletableFuture.completedFuture(null);
                    }

                    final List<Integer> newActiveSegments = getNewActiveSegments(createdSegments, sealedSegments, lastRecord);

                    byte[] updatedTable = TableHelper.addPartialRecordToHistoryTable(historyTable.getData(), newActiveSegments);
                    final Data<T> updated = new Data<>(updatedTable, historyTable.getVersion());

                    final HistoryRecord newRecord = HistoryRecord.readLatestRecord(updatedTable, false).get();
                    return updateHistoryTable(updated);
                });
    }

    private CompletableFuture<HistoryRecord> completeHistoryRecord(long scaleTimestamp, List<Integer> sealedSegments, List<Integer> newSegments) {
        return getHistoryTable()
                .thenCompose(historyTable -> {
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable.getData(), false);

                    assert lastRecordOpt.isPresent();

                    final HistoryRecord lastRecord = lastRecordOpt.get();

                    // idempotent check
                    if (!lastRecord.isPartial()) {
                        assert lastRecord.getSegments().stream().noneMatch(sealedSegments::contains);
                        assert newSegments.stream().allMatch(x -> lastRecord.getSegments().contains(x));

                        return CompletableFuture.completedFuture(lastRecord);
                    }

                    assert lastRecord.isPartial();
                    byte[] updatedTable = TableHelper.completePartialRecordInHistoryTable(historyTable.getData(), lastRecord,
                            Math.max(System.currentTimeMillis(), scaleTimestamp));
                    final Data<T> updated = new Data<>(updatedTable, historyTable.getVersion());

                    final HistoryRecord newRecord = HistoryRecord.readLatestRecord(updatedTable, false).get();
                    return updateHistoryTable(updated).thenApply(y -> newRecord);
                });
    }

    private List<Integer> getNewActiveSegments(final List<Integer> createdSegments,
                                               final List<Integer> sealedSegments,
                                               final HistoryRecord lastRecord) {
        final List<Integer> segments = lastRecord.getSegments();
        segments.removeAll(sealedSegments);
        segments.addAll(createdSegments);
        return segments;
    }

    private CompletableFuture<Void> addIndexRecord(final HistoryRecord historyRecord) {
        return getIndexTable()
                .thenCompose(indexTable -> {
                    final Optional<IndexRecord> lastRecord = IndexRecord.readLatestRecord(indexTable.getData());
                    // check idempotent
                    if (lastRecord.isPresent()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    final byte[] updatedTable = TableHelper.updateIndexTable(indexTable.getData(),
                            historyRecord.getEventTime(),
                            historyRecord.getOffset());
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

    abstract CompletableFuture<Void> deleteStream();

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

    abstract CompletableFuture<Void> createIndexTable(final Data<T> data);

    abstract CompletableFuture<Data<T>> getIndexTable();

    abstract CompletableFuture<Void> updateIndexTable(final Data<T> updated);

    abstract CompletableFuture<Void> createHistoryTable(final Data<T> data);

    abstract CompletableFuture<Void> updateHistoryTable(final Data<T> updated);

    abstract CompletableFuture<Data<T>> getHistoryTable();

    abstract CompletableFuture<Void> createSegmentFile(final Create create);

    abstract CompletableFuture<Void> createNewTransaction(final UUID txId, final long timestamp,
                                                          final long leaseExpiryTime,
                                                          final long maxExecutionExpiryTime,
                                                          final long scaleGracePeriod);

    abstract CompletableFuture<Data<Integer>> getActiveTx(final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> updateActiveTx(final UUID txId, final byte[] data) throws DataNotFoundException;

    abstract CompletableFuture<Void> sealActiveTx(final UUID txId, final boolean commit,
                                                  final Optional<Integer> version) throws DataNotFoundException;

    abstract CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> removeActiveTxEntry(final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp);

    abstract CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp);

    abstract CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<T> data);

    abstract CompletableFuture<Void> removeMarkerData(int segmentNumber);

    abstract CompletableFuture<Data<T>> getMarkerData(int segmentNumber);

    abstract CompletableFuture<Map<String, Data<T>>> getCurrentTxns();

    abstract CompletableFuture<Void> checkScopeExists() throws StoreException;
}
