/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Create;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.IndexRecord;
import io.pravega.controller.store.stream.tables.SegmentRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

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
                .thenCompose(x -> {
                    final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
                    final byte[] historyTable = TableHelper.createHistoryTable(create.getCreationTime(),
                            IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

                    return createHistoryTable(new Data<>(historyTable, null));
                })
                .thenCompose(x -> createIndexTable(new Data<>(TableHelper.createIndexTable(create.getCreationTime(), 0), null)))
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
            List<Integer> candidates = TableHelper.findSegmentSuccessorCandidates(segment,
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
                List<Integer> candidates = TableHelper.findSegmentPredecessorCandidates(successor,
                        indexTableFuture.getNow(null).getData(),
                        historyTableFuture.getNow(null).getData());
                resultFutures.add(findOverlapping(successor, candidates).thenApply(
                        list -> new SimpleImmutableEntry<>(successor, list.stream().map(Segment::getNumber).collect(Collectors.toList()))));
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
    public CompletableFuture<Void> scaleNewSegmentsCreated(final List<Integer> sealedSegments,
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
    public CompletableFuture<Void> scaleOldSegmentsSealed(final List<Integer> sealedSegments,
                                                          final List<Integer> newSegments,
                                                          final long scaleTimestamp) {
        return verifyLegalState(FutureHelpers.toVoid(
                completeHistoryRecord(scaleTimestamp, sealedSegments, newSegments)
                        .thenCompose(this::addIndexRecord)));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final UUID txnId,
                                                                         final long lease,
                                                                         final long maxExecutionTime,
                                                                         final long scaleGracePeriod) {
        final long current = System.currentTimeMillis();
        return verifyLegalState(createNewTransaction(txnId, current, current + lease, current + maxExecutionTime, scaleGracePeriod)
                .thenApply(x ->
                        new VersionedTransactionData(x, txnId, 0, TxnStatus.OPEN, current,
                                current + maxExecutionTime, scaleGracePeriod)));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final UUID txId, final long lease) {
        return getTransactionEpoch(txId).thenCompose(epoch -> getActiveTx(epoch, txId)
                .thenCompose(data -> {
                    ActiveTxnRecord activeTxnRecord = ActiveTxnRecord.parse(data.getData());
                    if (activeTxnRecord.getTxnStatus() == TxnStatus.OPEN) {
                        // Update txn record with new lease value and return versioned tx data.
                        ActiveTxnRecord newData = new ActiveTxnRecord(activeTxnRecord.getTxCreationTimestamp(),
                                System.currentTimeMillis() + lease, activeTxnRecord.getMaxExecutionExpiryTime(),
                                activeTxnRecord.getScaleGracePeriod(), activeTxnRecord.getTxnStatus());

                        return updateActiveTx(epoch, txId, newData.toByteArray())
                                .thenApply(x ->
                                        new VersionedTransactionData(epoch, txId, data.getVersion() + 1,
                                                TxnStatus.OPEN, activeTxnRecord.getTxCreationTimestamp(),
                                                activeTxnRecord.getMaxExecutionExpiryTime(),
                                                activeTxnRecord.getScaleGracePeriod()));
                    } else {
                        return FutureHelpers.failedFuture(new IllegalStateException(txId.toString()));
                    }
                }));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId) {
        return getTransactionEpoch(txId).thenCompose(epoch -> getActiveTx(epoch, txId)
                .thenApply(data -> {
                    ActiveTxnRecord activeTxnRecord = ActiveTxnRecord.parse(data.getData());
                    return new VersionedTransactionData(epoch, txId, data.getVersion(),
                            activeTxnRecord.getTxnStatus(), activeTxnRecord.getTxCreationTimestamp(),
                            activeTxnRecord.getMaxExecutionExpiryTime(), activeTxnRecord.getScaleGracePeriod());
                }));
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId) {
        CompletableFuture<Integer> epochFuture = getTransactionEpoch(txId).handle((epoch, ex) -> {
            if (epoch == null ||
                    (ex != null && ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException)) {
                return null;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return epoch;
        });

        return verifyLegalState(epochFuture.thenCompose(x -> {
            if (x == null) {
                return getCompletedTxnStatus(txId);
            } else {
                return checkTransactionStatus(x, txId);
            }
        }));
    }

    private CompletableFuture<TxnStatus> checkTransactionStatus(final Integer epoch, final UUID txId) {
        if (epoch == null) {
            return getCompletedTxnStatus(txId);
        }
        final CompletableFuture<TxnStatus> activeTx = getActiveTx(epoch, txId).handle((ok, ex) -> {
            if (ok == null ||
                    (ex != null && ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException)) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return ActiveTxnRecord.parse(ok.getData()).getTxnStatus();
        });

        return verifyLegalState(activeTx.thenCompose(x -> {
            if (x.equals(TxnStatus.UNKNOWN)) {
                return getCompletedTxnStatus(txId);
            } else {
                return CompletableFuture.completedFuture(x);
            }
        }));
    }

    private CompletableFuture<TxnStatus> getCompletedTxnStatus(UUID txnId) {
        return getCompletedTx(txnId).handle((ok, ex) -> {
            if (ok == null ||
                    (ex != null && ex instanceof DataNotFoundException)) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return CompletedTxnRecord.parse(ok.getData()).getCompletionStatus();
        });
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final UUID txId, final boolean commit,
                                                        final Optional<Integer> version) {
        return verifyLegalState(getTransactionEpoch(txId).thenCompose(epoch -> {
                    if (epoch == null) {
                        return checkTransactionStatus(null, txId).thenApply(status -> {
                            if (commit) {
                                switch (status) {
                                    case COMMITTED:
                                        return status;
                                    case ABORTED:
                                        throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                                    case OPEN:
                                    case ABORTING:
                                    case COMMITTING:
                                        // Control cannot reach this point, as epoch will not be null if txn is
                                        // OPEN, ABORTING, or COMMITTING.
                                        throw new IllegalStateException(txId.toString());
                                    default:
                                        throw new TransactionNotFoundException(txId.toString());
                                }
                            } else {
                                switch (status) {
                                    case ABORTED:
                                        return status;
                                    case COMMITTED:
                                        throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                                    case OPEN:
                                    case ABORTING:
                                    case COMMITTING:
                                        // Control cannot reach this point, as epoch will not be null if txn is
                                        // OPEN, ABORTING, or COMMITTING.
                                        throw new IllegalStateException(txId.toString());
                                    default:
                                        throw new TransactionNotFoundException(txId.toString());
                                }
                            }
                        });
                    } else {
                        return getActiveTx(epoch, txId).thenCompose(data -> {
                            ActiveTxnRecord txnRecord = ActiveTxnRecord.parse(data.getData());
                            int dataVersion = version.isPresent() ? version.get() : data.getVersion();
                            TxnStatus status = txnRecord.getTxnStatus();
                            if (commit) {
                                switch (status) {
                                    case OPEN:
                                        return sealActiveTx(epoch, txId, true, txnRecord, dataVersion).thenApply(y -> TxnStatus.COMMITTING);
                                    case COMMITTING:
                                    case COMMITTED:
                                        return CompletableFuture.completedFuture(status);
                                    case ABORTING:
                                    case ABORTED:
                                        throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                                    default:
                                        throw new TransactionNotFoundException(txId.toString());
                                }
                            } else {
                                switch (status) {
                                    case OPEN:
                                        return sealActiveTx(epoch, txId, false, txnRecord, dataVersion).thenApply(y -> TxnStatus.ABORTING);
                                    case ABORTING:
                                    case ABORTED:
                                        return CompletableFuture.completedFuture(status);
                                    case COMMITTING:
                                    case COMMITTED:
                                        throw new OperationOnTxNotAllowedException(txId.toString(), "seal");
                                    default:
                                        throw new TransactionNotFoundException(txId.toString());
                                }
                            }
                        });
                    }
                }));
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final UUID txId) {

        return verifyLegalState(getTransactionEpoch(txId)
                .thenCompose(epoch -> checkTransactionStatus(epoch, txId).thenApply(x -> {
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
                .thenCompose(x -> epoch != null ? removeActiveTxEntry(epoch, txId) : CompletableFuture.completedFuture(null))
                .thenApply(x -> TxnStatus.COMMITTED)));
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final UUID txId) {
        return verifyLegalState(getTransactionEpoch(txId)
                .thenCompose(epoch -> checkTransactionStatus(txId).thenApply(x -> {
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
                .thenCompose(y -> epoch != null ? removeActiveTxEntry(epoch, txId) : CompletableFuture.completedFuture(null))
                .thenApply(y -> TxnStatus.ABORTED)));
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
        return verifyLegalState(getCurrentTxns()
                .thenApply(x -> x.entrySet().stream()
                        .collect(Collectors.toMap(k -> UUID.fromString(k.getKey()),
                                v -> ActiveTxnRecord.parse(v.getValue().getData())))));
    }

    @Override
    public CompletableFuture<Void> setColdMarker(int segmentNumber, long timestamp) {

        return verifyLegalState(getMarkerData(segmentNumber)
                .thenCompose(x -> {
                    if (x != null) {
                        byte[] b = new byte[Long.BYTES];
                        BitConverter.writeLong(b, 0, timestamp);
                        final Data<T> data = new Data<>(b, x.getVersion());
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

                    long scaleEventTime = Math.max(System.currentTimeMillis(), scaleTimestamp);
                    final Optional<HistoryRecord> previousOpt = HistoryRecord.fetchPrevious(lastRecord, historyTable.getData());
                    if (previousOpt.isPresent()) {
                        // To ensure that we always have ascending time in history records irrespective of controller clock mismatches.
                        scaleEventTime = Math.max(scaleEventTime, previousOpt.get().getScaleTime() + 1);
                    }

                    byte[] updatedTable = TableHelper.completePartialRecordInHistoryTable(historyTable.getData(), lastRecord, scaleEventTime);
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
                    if (lastRecord.isPresent() && lastRecord.get().getHistoryOffset() == historyRecord.getOffset()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    final byte[] updatedTable = TableHelper.updateIndexTable(indexTable.getData(),
                            historyRecord.getScaleTime(),
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

    abstract CompletableFuture<Integer> createNewTransaction(final UUID txId,
                                                          final long timestamp,
                                                          final long leaseExpiryTime,
                                                          final long maxExecutionExpiryTime,
                                                          final long scaleGracePeriod);

    abstract CompletableFuture<Integer> getTransactionEpoch(UUID txId);

    abstract CompletableFuture<Pair<Integer, List<Integer>>> getLatestEpoch();

    abstract CompletableFuture<Pair<Integer, List<Integer>>> getActiveEpoch();

    abstract CompletableFuture<Data<Integer>> getActiveTx(final int epoch, final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> updateActiveTx(final int epoch,
                                                    final UUID txId, final byte[] data) throws DataNotFoundException;

    abstract CompletableFuture<Void> sealActiveTx(final int epoch,
                                                  final UUID txId, final boolean commit,
                                                  final ActiveTxnRecord txnRecord,
                                                  final int version) throws DataNotFoundException;

    abstract CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId) throws DataNotFoundException;

    abstract CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp);

    abstract CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp);

    abstract CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<T> data);

    abstract CompletableFuture<Void> removeMarkerData(int segmentNumber);

    abstract CompletableFuture<Data<T>> getMarkerData(int segmentNumber);

    abstract CompletableFuture<Map<String, Data<T>>> getCurrentTxns();

    abstract CompletableFuture<Void> checkScopeExists() throws StoreException;
}
