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

import com.google.common.collect.Lists;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.StoreException.DataNotFoundException;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.IndexRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.TableHelper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

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
    public CompletableFuture<CreateStreamResponse> create(final StreamConfiguration configuration, long createTimestamp) {

        return checkScopeExists()
                .thenCompose((Void v) -> checkStreamExists(configuration, createTimestamp))
                .thenCompose(createStreamResponse -> storeCreationTimeIfAbsent(createStreamResponse.getTimestamp())
                        .thenCompose((Void v) -> createConfigurationIfAbsent(createStreamResponse.getConfiguration()))
                        .thenCompose((Void v) -> createStateIfAbsent(State.CREATING))
                        .thenCompose((Void v) -> createNewSegmentTable(createStreamResponse.getConfiguration(), createStreamResponse.getTimestamp()))
                        .thenCompose((Void v) -> getState())
                        .thenCompose(state -> {
                            if (state.equals(State.CREATING)) {
                                return createNewEpoch(0);
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        })
                        .thenCompose((Void v) -> {
                            final int numSegments = createStreamResponse.getConfiguration().getScalingPolicy().getMinNumSegments();
                            final byte[] historyTable = TableHelper.createHistoryTable(createStreamResponse.getTimestamp(),
                                    IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

                            return createHistoryTableIfAbsent(new Data<>(historyTable, null));
                        })
                        .thenCompose((Void v) -> createIndexTableIfAbsent(new Data<>(TableHelper.createIndexTable(createStreamResponse.getTimestamp(), 0), null)))
                        .thenApply((Void v) -> createStreamResponse));
    }

    private CompletableFuture<Void> createNewSegmentTable(final StreamConfiguration configuration, long timestamp) {
        final int numSegments = configuration.getScalingPolicy().getMinNumSegments();
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        final byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                new byte[0],
                newRanges,
                timestamp
        );

        return createSegmentTableIfAbsent(new Data<>(segmentTable, null));
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
        return verifyState(() -> updateState(State.UPDATING)
                .thenApply(x -> setConfigurationData(configuration))
                .thenApply(x -> true),
                Lists.newArrayList(State.ACTIVE, State.UPDATING));
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
        return getStateData()
                .thenCompose(currState -> {
                    if (State.isTransitionAllowed((State) SerializationUtils.deserialize(currState.getData()), state)) {
                        return setStateData(new Data<>(SerializationUtils.serialize(state), currState.getVersion()))
                                .thenApply(x -> true);
                    } else {
                        return FutureHelpers.failedFuture(StoreException.create(
                                StoreException.Type.OPERATION_NOT_ALLOWED,
                                "Stream: " + getName() + " State: " + state.name()));
                    }
                });
    }

    @Override
    public CompletableFuture<State> getState() {
        return getStateData()
                .thenApply(x -> (State) SerializationUtils.deserialize(x.getData()));
    }

    /**
     * Fetch the segment table and retrieve the segment.
     *
     * @param number segment number.
     * @return : future of segment
     */
    @Override
    public CompletableFuture<Segment> getSegment(final int number) {
        return verifyLegalState().thenCompose(v -> getSegmentRow(number));
    }

    @Override
    public CompletableFuture<Integer> getSegmentCount() {
        return verifyLegalState().thenCompose(v -> getSegmentTable()).thenApply(x -> TableHelper.getSegmentCount(x.getData()));
    }

    @Override
    public CompletableFuture<List<ScaleMetadata>> getScaleMetadata() {
        return verifyLegalState().thenCompose(v -> getHistoryTable())
                .thenApply(x -> TableHelper.getScaleMetadata(x.getData()))
                .thenCompose(listOfScaleRecords ->
                        FutureHelpers.allOfWithResults(listOfScaleRecords.stream().map(record -> {
                            long scaleTs = record.getLeft();
                            CompletableFuture<List<Segment>> list = FutureHelpers.allOfWithResults(
                                    record.getRight().stream().map(this::getSegment)
                                    .collect(Collectors.toList()));

                            return list.thenApply(segments -> new ScaleMetadata(scaleTs, segments));
                        }).collect(Collectors.toList())));
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
        return verifyLegalState().thenCompose(v -> getSuccessorsForSegment(number))
                                 .thenApply(list -> list.stream().map(Segment::getNumber).collect(Collectors.toList()));
    }

    private CompletableFuture<List<Segment>> findOverlapping(Segment segment, List<Integer> candidates) {
        return verifyLegalState().thenCompose(v -> FutureHelpers.allOfWithResults(candidates.stream()
                                                                                            .map(this::getSegment)
                                                                                            .collect(Collectors.toList())))
                                 .thenApply(successorCandidates -> successorCandidates.stream()
                                                                                      .filter(x -> x.overlaps(segment))
                                                                                      .collect(Collectors.toList()));
    }

    private CompletableFuture<List<Segment>> getSuccessorsForSegment(final int number) {
        return getHistoryTable()
                .thenApply(historyTable -> {
                    CompletableFuture<Segment> segmentFuture = getSegment(number);
                    CompletableFuture<Data<T>> indexTableFuture = getIndexTable();
                    return new ImmutableTriple<>(historyTable, segmentFuture, indexTableFuture);
                })
                .thenCompose(triple -> CompletableFuture.allOf(triple.getMiddle(), triple.getRight())
                        .thenCompose(x -> {
                            final Segment segment = triple.getMiddle().join();
                            List<Integer> candidates = TableHelper.findSegmentSuccessorCandidates(segment,
                                    triple.getRight().join().getData(),
                                    triple.getLeft().getData());
                            return findOverlapping(segment, candidates);
                        }));
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessorsWithPredecessors(final int number) {
        // Ensure the order, we should first get history table followed by segment table because during scale we first write to
        // segment table followed by history table. So if a record exists in history table, then we are guaranteed to find it in
        // segment table.
        return verifyLegalState()
                .thenCompose(legal -> getHistoryTable()
                        .thenApply(historyTable -> new ImmutableTriple<>(historyTable, getIndexTable(), getSuccessorsForSegment(number)))
                        .thenCompose(triple -> CompletableFuture.allOf(triple.getMiddle(), triple.getRight())
                                .thenCompose(x -> {
                                    List<CompletableFuture<Map.Entry<Segment, List<Integer>>>> resultFutures = new ArrayList<>();
                                    List<Segment> successors = triple.getRight().join();
                                    Data<T> indexTable = triple.getMiddle().join();
                                    Data<T> historyTable = triple.getLeft();

                                    for (Segment successor : successors) {
                                        List<Integer> candidates = TableHelper.findSegmentPredecessorCandidates(successor,
                                                indexTable.getData(),
                                                historyTable.getData());
                                        resultFutures.add(findOverlapping(successor, candidates).thenApply(
                                                list -> new SimpleImmutableEntry<>(successor, list.stream().map(Segment::getNumber)
                                                        .collect(Collectors.toList()))));
                                    }
                                    return FutureHelpers.allOfWithResults(resultFutures);
                                })
                                .thenApply(list -> list.stream().collect(Collectors.toMap(e -> e.getKey().getNumber(), Map.Entry::getValue)))));
    }

    /**
     * Find predecessor candidates and find overlaps with given segment's key range.
     *
     * @param number segment number.
     * @return : future of list of predecessor segment numbers
     */
    @Override
    public CompletableFuture<List<Integer>> getPredecessors(final int number) {
        return verifyLegalState().thenCompose(x -> getHistoryTable()
                .thenApply(historyTable -> new ImmutableTriple<>(historyTable, getSegment(number), getIndexTable())))
                .thenCompose(triple -> CompletableFuture.allOf(triple.getMiddle(), triple.getRight()).thenCompose(x -> {
                    final Segment segment = triple.getMiddle().join();
                    Data<T> indexTable = triple.getRight().join();
                    Data<T> historyTable = triple.getLeft();
                    List<Integer> candidates = TableHelper.findSegmentPredecessorCandidates(segment,
                            indexTable.getData(),
                            historyTable.getData());
                    return findOverlapping(segment, candidates);
                }).thenApply(list -> list.stream().map(Segment::getNumber).collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<List<Integer>> getActiveSegments() {
        return verifyLegalState().thenCompose(v -> getHistoryTable()).thenApply(x -> TableHelper.getActiveSegments(x.getData()));
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
        final CompletableFuture<Data<T>> indexFuture = verifyLegalState().thenCompose(v -> getIndexTable());

        final CompletableFuture<Data<T>> historyFuture = getHistoryTable();

        return indexFuture.thenCombine(historyFuture,
                (indexTable, historyTable) -> TableHelper.getActiveSegments(timestamp,
                                                                            indexTable.getData(),
                                                                            historyTable.getData()));
    }

    @Override
    public CompletableFuture<List<Integer>> getActiveSegments(final int epoch) {
        return getHistoryTable().thenApply(table -> TableHelper.getSegmentsInEpoch(table.getData(), epoch));
    }

    /**
     * Scale and create are two tasks where we update the table. For scale to be legitimate, it has to be
     * preceded by create. Which means all appropriate tables exist.
     * Scale Steps:
     * 1. Add new segment information in segment table.
     *
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @param runOnlyIfStarted run only if the scale operation was started.
     * @return : list of newly created segments with current epoch
     */
    @Override
    public CompletableFuture<StartScaleResponse> startScale(final List<Integer> sealedSegments,
                                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                            final long scaleTimestamp,
                                                            boolean runOnlyIfStarted) {
        return getHistoryTable()
                .thenCompose(historyTable -> getSegmentTable().thenApply(segmentTable -> new ImmutablePair<>(historyTable, segmentTable)))
                .thenCompose(pair -> {
                    final Data<T> segmentTable = pair.getRight();
                    final Data<T> historyTable = pair.getLeft();
                    final int activeEpoch = TableHelper.getActiveEpoch(historyTable.getData()).getKey();

                    if (TableHelper.isScaleOngoing(historyTable.getData(), segmentTable.getData())) {
                        return isScaleRerun(sealedSegments, newRanges, segmentTable, historyTable, activeEpoch);
                    } else {
                        if (!TableHelper.isScaleInputValid(sealedSegments, newRanges, segmentTable.getData())) {
                            log.error("scale input invalid {} {}", sealedSegments, newRanges);
                            throw new ScaleOperationExceptions.ScaleInputInvalidException();
                        }

                        // check input is valid and satisfies preconditions
                        if (!TableHelper.canScaleFor(sealedSegments, historyTable.getData())) {
                            // invalid input, log and ignore
                            log.warn("scale precondition failed {}", sealedSegments);
                            throw new ScaleOperationExceptions.ScalePreConditionFailureException();
                        }

                        if (runOnlyIfStarted) {
                            log.info("scale not started, retry later.");
                            throw new ScaleOperationExceptions.ScaleStartException();
                        }

                        log.info("Scale {}/{} for segments started. Creating new segments. SegmentsToSeal {}", scope, name, sealedSegments);
                        // fresh run
                        // Ensure that segment.creation time is monotonically increasing after each new scale
                        long lastScaleTime = HistoryRecord.readLatestRecord(historyTable.getData(), true).map(HistoryRecord::getScaleTime).orElse(0L);

                        long scaleEventTime = Math.max(System.currentTimeMillis(), scaleTimestamp);

                        long segmentCreationTimestamp = Math.max(scaleEventTime, lastScaleTime + 1);

                        return scaleCreateNewSegments(newRanges, segmentCreationTimestamp, segmentTable, activeEpoch);
                    }
                })
                .thenCompose(epochStartSegmentpair -> getSegments(IntStream.range(epochStartSegmentpair.getRight(),
                        epochStartSegmentpair.getRight() + newRanges.size())
                        .boxed()
                        .collect(Collectors.toList()))
                        .thenApply(newSegments -> new StartScaleResponse(epochStartSegmentpair.getLeft(), newSegments)));
    }

    private CompletableFuture<ImmutablePair<Integer, Integer>> scaleCreateNewSegments(final List<SimpleEntry<Double, Double>> newRanges,
                                                                                    final long scaleTimestamp, Data<T> segmentTable,
                                                                                    final int activeEpoch) {
        final int nextSegmentNumber = TableHelper.getSegmentCount(segmentTable.getData());
        final byte[] updated = TableHelper.updateSegmentTable(nextSegmentNumber, segmentTable.getData(),
                newRanges, scaleTimestamp);

        final Data<T> updatedData = new Data<>(updated, segmentTable.getVersion());

        return setSegmentTable(updatedData)
                .thenApply(z -> new ImmutablePair<>(activeEpoch, nextSegmentNumber))
                .thenApply(response -> {
                    log.debug("scale {}/{} new segments created successfully", scope, name);
                    return response;
                });
    }

    private CompletableFuture<ImmutablePair<Integer, Integer>> isScaleRerun(final List<Integer> sealedSegments,
                                                                          final List<SimpleEntry<Double, Double>> newRanges,
                                                                          final Data<T> segmentTable, final Data<T> historyTable,
                                                                          final int activeEpoch) {
        int nextSegmentNumber;
        if (TableHelper.isRerunOf(sealedSegments, newRanges, historyTable.getData(), segmentTable.getData())) {
            log.debug("rerunning scale for stream {}/{} with segments to seal {}", scope, name, sealedSegments);
            // rerun means segment table is already updated. No need to do anything
            nextSegmentNumber = TableHelper.getSegmentCount(segmentTable.getData()) - newRanges.size();
            return CompletableFuture.completedFuture(new ImmutablePair<>(activeEpoch, nextSegmentNumber));
        } else {
            log.debug("scale conflict for stream {}/{} with segments to seal {}", scope, name, sealedSegments);

            return FutureHelpers.failedFuture(new ScaleOperationExceptions.ScaleStartException());
        }
    }

    /**
     * Segments created with pravega, update the history table with this fact so they are available as successors
     * 3. Add entry into the history table.
     *
     * @param sealedSegments segments to be sealed
     * @param epoch
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated(final List<Integer> sealedSegments,
                                                           final List<Integer> newSegments,
                                                           final int epoch,
                                                           final long scaleTimestamp) {
        return verifyState(() -> FutureHelpers.toVoid(addPartialHistoryRecord(sealedSegments, newSegments, epoch)),
                Lists.newArrayList(State.SCALING));
    }

    /**
     * If scale is ongoing, try to delete the epoch node.
     *
     * @param epoch epoch
     * @return true if we are able to delete the epoch, false otherwise.
     */
    @Override
    public CompletableFuture<Boolean> scaleTryDeleteEpoch(final int epoch) {
        return getHistoryTableFromStore()
                .thenCompose(historyTable -> getSegmentTableFromStore().thenApply(segmentTable -> new ImmutablePair<>(historyTable, segmentTable)))
                .thenCompose(pair -> {
                    Data<T> segmentTable = pair.getRight();
                    Data<T> historyTable = pair.getLeft();
                    CompletableFuture<Boolean> result = new CompletableFuture<>();

                    if (TableHelper.isScaleOngoing(historyTable.getData(), segmentTable.getData())) {
                        deleteEpochNode(epoch)
                                .whenComplete((r, e) -> {
                                    if (e != null) {
                                        Throwable ex = ExceptionHelpers.getRealException(e);
                                        if (ex instanceof StoreException.DataNotEmptyException) {
                                            // cant delete as there are transactions still running under epoch node
                                            log.debug("stream {}/{} epoch {} not empty", scope, name, epoch);
                                            result.complete(false);
                                        } else {
                                            log.warn("stream {}/{} deleting epoch {} threw exception {}", scope, name, epoch, ex.getClass().getName());

                                            result.completeExceptionally(ex);
                                        }
                                    } else {
                                        log.debug("stream {}/{} deleted epoch {} ", scope, name, epoch);

                                        result.complete(true);
                                    }
                                });
                    } else {
                        result.complete(false);
                    }
                    return result;
                });
    }

    private CompletableFuture<Void> clearMarkers(final List<Integer> segments) {
        return FutureHelpers.toVoid(FutureHelpers.allOfWithResults(segments.stream().parallel()
                .map(this::removeColdMarker).collect(Collectors.toList())));
    }

    /**
     * Remainder of scale metadata update. Also set the state back to active.
     * 4. complete entry into the history table.
     * 5. Add entry into the index table.
     *
     * @param sealedSegments segments to be sealed
     * @param activeEpoch
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> scaleOldSegmentsSealed(final List<Integer> sealedSegments,
                                                          final List<Integer> newSegments,
                                                          final int activeEpoch,
                                                          final long scaleTimestamp) {
        return verifyState(() -> FutureHelpers.toVoid(clearMarkers(sealedSegments)
                .thenCompose(x -> completeScale(scaleTimestamp, sealedSegments, activeEpoch, newSegments))),
                Lists.newArrayList(State.SCALING));
    }

    @Override
    public CompletableFuture<Pair<List<Integer>, List<Integer>>> latestScaleData() {
        return verifyLegalState().thenCompose(v -> getHistoryTable()).thenApply(history -> {
            byte[] historyTable = history.getData();
            return TableHelper.getLatestScaleData(historyTable);
        });
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final UUID txnId,
                                                                         final long lease,
                                                                         final long maxExecutionTime,
                                                                         final long scaleGracePeriod) {
        final long current = System.currentTimeMillis();
        final long leaseTimestamp = current + lease;
        final long maxExecTimestamp = current + maxExecutionTime;
        return verifyLegalState().thenCompose(v -> createNewTransaction(txnId, current, leaseTimestamp, maxExecTimestamp, scaleGracePeriod))
                .thenApply(epoch -> new VersionedTransactionData(epoch, txnId, 0, TxnStatus.OPEN, current,
                        current + maxExecutionTime, scaleGracePeriod));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final VersionedTransactionData txnData,
                                                                       final long lease) {
        // Update txn record with new lease value and return versioned tx data.
        final int epoch = txnData.getEpoch();
        final UUID txnId = txnData.getId();
        final int version = txnData.getVersion();
        final long creationTime = txnData.getCreationTime();
        final long maxExecutionExpiryTime = txnData.getMaxExecutionExpiryTime();
        final long scaleGracePeriod = txnData.getScaleGracePeriod();
        final TxnStatus status = txnData.getStatus();
        final ActiveTxnRecord newData = new ActiveTxnRecord(creationTime, System.currentTimeMillis() + lease,
                maxExecutionExpiryTime, scaleGracePeriod, status);
        final Data<Integer> data = new Data<>(newData.toByteArray(), version);

        return updateActiveTx(epoch, txnId, data).thenApply(x -> new VersionedTransactionData(epoch, txnId,
                version + 1, status, creationTime, maxExecutionExpiryTime, scaleGracePeriod));
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
        return verifyLegalState().thenCompose(v -> getTransactionEpoch(txId).handle((epoch, ex) -> {
            if (ex != null && ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException) {
                return null;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return epoch;
        }).thenCompose(x -> {
            if (x == null) {
                return getCompletedTxnStatus(txId);
            } else {
                return checkTransactionStatus(x, txId);
            }
        }));
    }

    private CompletableFuture<TxnStatus> checkTransactionStatus(final int epoch, final UUID txId) {
        return verifyLegalState().thenCompose(v -> getActiveTx(epoch, txId).handle((ok, ex) -> {
            if (ex != null && ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return ActiveTxnRecord.parse(ok.getData()).getTxnStatus();
        }).thenCompose(x -> {
            if (x.equals(TxnStatus.UNKNOWN)) {
                return getCompletedTxnStatus(txId);
            } else {
                return CompletableFuture.completedFuture(x);
            }
        }));
    }

    private CompletableFuture<TxnStatus> getCompletedTxnStatus(UUID txId) {
        return getCompletedTx(txId).handle((ok, ex) -> {
            if (ex != null && ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return CompletedTxnRecord.parse(ok.getData()).getCompletionStatus();
        });
    }

    @Override
    public CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final UUID txId, final boolean commit,
                                                                              final Optional<Integer> version) {
        val legal = verifyLegalState();
        return legal.thenCompose(v -> getTransactionEpoch(txId).thenCompose(epoch -> sealActiveTxn(epoch, txId, commit, version))
                                                               .exceptionally(ex -> new SimpleEntry<>(handleDataNotFoundException(ex), null)))
                    .thenCompose(pair -> {
                        if (pair.getKey() == TxnStatus.UNKNOWN) {
                            return validateCompletedTxn(txId, commit, "seal").thenApply(status -> new SimpleEntry<>(status, null));
                        } else {
                            return CompletableFuture.completedFuture(pair);
                        }
                    });
    }

    /**
     * Seal a transaction in OPEN/COMMITTING/ABORTING state. This method does CAS on the transaction data node if
     * the transaction is in OPEN state, optionally checking version of transaction data node, if required.
     *
     * @param epoch   transaction epoch.
     * @param txId    transaction identifier.
     * @param commit  boolean indicating whether to commit or abort the transaction.
     * @param version optional expected version of transaction node to validate before updating it.
     * @return        a pair containing transaction status and its epoch.
     */
    private CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealActiveTxn(final int epoch,
                                                                             final UUID txId,
                                                                             final boolean commit,
                                                                             final Optional<Integer> version) {
        return getActiveTx(epoch, txId).thenCompose(data -> {
            ActiveTxnRecord txnRecord = ActiveTxnRecord.parse(data.getData());
            int dataVersion = version.isPresent() ? version.get() : data.getVersion();
            TxnStatus status = txnRecord.getTxnStatus();
            switch (status) {
                case OPEN:
                    return sealActiveTx(epoch, txId, commit, txnRecord, dataVersion).thenApply(y ->
                            new SimpleEntry<>(commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING, epoch));
                case COMMITTING:
                case COMMITTED:
                    if (commit) {
                        return CompletableFuture.completedFuture(new SimpleEntry<>(status, epoch));
                    } else {
                        throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                                "Stream: " + getName() + " Transaction: " + txId.toString() +
                                        " State: " + status.name());
                    }
                case ABORTING:
                case ABORTED:
                    if (commit) {
                        throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                                "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " +
                                        status.name());
                    } else {
                        return CompletableFuture.completedFuture(new SimpleEntry<>(status, epoch));
                    }
                default:
                    throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                            "Stream: " + getName() + " Transaction: " + txId.toString());
            }
        });
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final int epoch, final UUID txId) {
        return verifyLegalState().thenCompose(v -> checkTransactionStatus(epoch, txId)).thenApply(x -> {
            switch (x) {
                // Only sealed transactions can be committed
                case COMMITTED:
                case COMMITTING:
                    return x;
                case OPEN:
                case ABORTING:
                case ABORTED:
                    throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                            "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " + x.toString());
                case UNKNOWN:
                default:
                    throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                            "Stream: " + getName() + " Transaction: " + txId.toString());
            }
        }).thenCompose(x -> {
            if (x.equals(TxnStatus.COMMITTING)) {
                return createCompletedTxEntry(txId, TxnStatus.COMMITTED, System.currentTimeMillis());
            } else {
                return CompletableFuture.completedFuture(null); // already committed, do nothing
            }
        }).thenCompose(x -> removeActiveTxEntry(epoch, txId)).thenApply(x -> TxnStatus.COMMITTED);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final int epoch, final UUID txId) {
        return verifyLegalState().thenCompose(v -> checkTransactionStatus(txId)).thenApply(x -> {
            switch (x) {
                case ABORTING:
                case ABORTED:
                    return x;
                case OPEN:
                case COMMITTING:
                case COMMITTED:
                    throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                            "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " + x.name());
                case UNKNOWN:
                default:
                    throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                            "Stream: " + getName() + " Transaction: " + txId.toString());
            }
        }).thenCompose(x -> {
            if (x.equals(TxnStatus.ABORTING)) {
                return createCompletedTxEntry(txId, TxnStatus.ABORTED, System.currentTimeMillis());
            } else {
                return CompletableFuture.completedFuture(null); // already aborted, do nothing
            }
        }).thenCompose(y -> removeActiveTxEntry(epoch, txId)).thenApply(y -> TxnStatus.ABORTED);
    }

    @SneakyThrows
    private TxnStatus handleDataNotFoundException(Throwable ex) {
        if (ExceptionHelpers.getRealException(ex) instanceof DataNotFoundException) {
            return TxnStatus.UNKNOWN;
        } else {
            throw ex;
        }
    }

    private CompletableFuture<TxnStatus> validateCompletedTxn(UUID txId, boolean commit, String operation) {
        return getCompletedTxnStatus(txId).thenApply(status -> {
            if ((commit && status == TxnStatus.COMMITTED) || (!commit && status == TxnStatus.ABORTED)) {
                return status;
            } else if (status == TxnStatus.UNKNOWN) {
                throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString());
            } else {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " Transaction: " + txId.toString() + " State: " + status.name());
            }
        });
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
        return verifyLegalState().thenCompose(v -> getCurrentTxns())
                                 .thenApply(x -> x.entrySet()
                                                  .stream()
                                                  .collect(toMap(k -> UUID.fromString(k.getKey()),
                                                                 v -> ActiveTxnRecord.parse(v.getValue().getData()))));
    }

    @Override
    public CompletableFuture<Pair<Integer, List<Integer>>> getActiveEpoch(boolean ignoreCached) {
        CompletableFuture<Data<T>> historyTableFuture = ignoreCached ? getHistoryTableFromStore() :
                getHistoryTable();

        return historyTableFuture.thenApply(table -> TableHelper.getActiveEpoch(table.getData()));
    }

    @Override
    public CompletableFuture<Pair<Integer, List<Integer>>> getLatestEpoch() {
        return getHistoryTable().thenApply(table -> TableHelper.getLatestEpoch(table.getData()));
    }

    @Override
    public CompletableFuture<Void> setColdMarker(int segmentNumber, long timestamp) {
        return verifyLegalState().thenCompose(v -> getMarkerData(segmentNumber)).thenCompose(x -> {
            if (x != null) {
                byte[] b = new byte[Long.BYTES];
                BitConverter.writeLong(b, 0, timestamp);
                final Data<T> data = new Data<>(b, x.getVersion());
                return updateMarkerData(segmentNumber, data);
            } else {
                return createMarkerData(segmentNumber, timestamp);
            }
        });
    }

    @Override
    public CompletableFuture<Long> getColdMarker(int segmentNumber) {
        return verifyLegalState().thenCompose(v -> getMarkerData(segmentNumber))
                                 .thenApply(x -> (x != null) ? BitConverter.readLong(x.getData(), 0) : 0L);
    }

    @Override
    public CompletableFuture<Void> removeColdMarker(int segmentNumber) {
        return verifyLegalState().thenCompose(v -> removeMarkerData(segmentNumber));
    }

    private <U> CompletableFuture<U> verifyState(Supplier<CompletableFuture<U>> future, List<State> states) {
        return getState()
                .thenCompose(state -> {
                    if (state != null && states.contains(state)) {
                        return future.get();
                    } else {
                        throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                                "Stream: " + getName() + " State: " + state.name());
                    }
                });
    }

    private CompletableFuture<Void> verifyLegalState() {
        return getState().thenApply(state -> {
            if (state == null || state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    private CompletableFuture<List<Segment>> getSegments(final List<Integer> segments) {
        return FutureHelpers.allOfWithResults(segments.stream().map(this::getSegment)
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> createNewEpoch(int epoch) {
        return createEpochNodeIfAbsent(epoch);
    }

    /**
     * update history table if not already updated:
     * fetch last record from history table.
     * if eventTime is >= scale.scaleTimeStamp do nothing, else create record
     *
     * @return : future of history table offset for last entry
     */
    private CompletableFuture<Void> addPartialHistoryRecord(final List<Integer> sealedSegments,
                                                            final List<Integer> createdSegments,
                                                            final int epoch) {
        return getHistoryTable()
                .thenCompose(historyTable -> {
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable.getData(), false);

                    // scale task is not allowed unless create is done which means at least one
                    // record in history table.
                    assert lastRecordOpt.isPresent();

                    final HistoryRecord lastRecord = lastRecordOpt.get();

                    // idempotent check
                    if (lastRecord.getEpoch() > epoch) {
                        boolean idempotent = lastRecord.isPartial() && lastRecord.getSegments().containsAll(createdSegments);
                        if (idempotent) {
                            HistoryRecord previous = HistoryRecord.fetchPrevious(lastRecord, historyTable.getData()).get();

                            idempotent = previous.getSegments().stream().noneMatch(createdSegments::contains);
                        }

                        if (idempotent) {
                            log.debug("{}/{} scale op for epoch {} - history record already added", scope, name, epoch);
                            return CompletableFuture.completedFuture(null);
                        } else {
                            log.warn("{}/{} scale op for epoch {}. Scale already completed.", scope, name, epoch);

                            throw new ScaleOperationExceptions.ScaleConditionInvalidException();
                        }
                    }

                    final List<Integer> newActiveSegments = getNewActiveSegments(createdSegments, sealedSegments, lastRecord);

                    byte[] updatedTable = TableHelper.addPartialRecordToHistoryTable(historyTable.getData(), newActiveSegments);
                    final Data<T> updated = new Data<>(updatedTable, historyTable.getVersion());
                    int latestEpoch = TableHelper.getLatestEpoch(updatedTable).getKey();

                    return createNewEpoch(latestEpoch).thenCompose(v -> updateHistoryTable(updated))
                            .whenComplete((r, e) -> {
                                if (e == null) {
                                    log.debug("{}/{} scale op for epoch {}. Creating new epoch and updating history table.", scope, name, epoch);
                                } else {
                                    log.warn("{}/{} scale op for epoch {}. Failed to update history table. {}", scope, name, epoch, e.getClass().getName());
                                }
                            });
                });
    }

    private CompletableFuture<Void> completeScale(final long scaleTimestamp,
                                                  final List<Integer> sealedSegments,
                                                  final int activeEpoch,
                                                  final List<Integer> newSegments) {
        return getHistoryTable()
                .thenCompose(historyTable -> {
                    final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable.getData(), false);

                    assert lastRecordOpt.isPresent();

                    final HistoryRecord lastRecord = lastRecordOpt.get();

                    // idempotent check
                    if (!lastRecord.isPartial()) {
                        if (lastRecord.getSegments().stream().noneMatch(sealedSegments::contains) &&
                                newSegments.stream().allMatch(x -> lastRecord.getSegments().contains(x))) {
                            log.debug("{}/{} scale already completed for epoch {}.", scope, name, activeEpoch);

                            return CompletableFuture.completedFuture(null);
                        } else {
                            log.debug("{}/{} scale complete attempt invalid for epoch {}.", scope, name, activeEpoch);

                            throw new ScaleOperationExceptions.ScaleConditionInvalidException();
                        }
                    }

                    long scaleEventTime = Math.max(System.currentTimeMillis(), scaleTimestamp);
                    final Optional<HistoryRecord> previousOpt = HistoryRecord.fetchPrevious(lastRecord, historyTable.getData());
                    if (previousOpt.isPresent()) {
                        // To ensure that we always have ascending time in history records irrespective of controller clock mismatches.
                        scaleEventTime = Math.max(scaleEventTime, previousOpt.get().getScaleTime() + 1);

                        if (previousOpt.get().getEpoch() > activeEpoch) {
                            throw new ScaleOperationExceptions.ScaleConditionInvalidException();
                        }
                    }

                    byte[] updatedTable = TableHelper.completePartialRecordInHistoryTable(historyTable.getData(), lastRecord, scaleEventTime);
                    final Data<T> updated = new Data<>(updatedTable, historyTable.getVersion());

                    final HistoryRecord newRecord = HistoryRecord.readLatestRecord(updatedTable, false).get();
                    return addIndexRecord(newRecord)
                            .thenCompose(x -> updateHistoryTable(updated))
                            .thenCompose(x -> FutureHelpers.toVoid(updateState(State.ACTIVE)))
                            .whenComplete((r, e) -> {
                                if (e != null) {
                                    log.warn("{}/{} attempt to complete scale for epoch {}. {}", scope, name, activeEpoch, e.getClass().getName());
                                } else {
                                    log.debug("{}/{} scale complete, index and history tables updated for epoch {}.", scope, name, activeEpoch);
                                }
                            });
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

    abstract CompletableFuture<Void> deleteStream();

    abstract CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime);

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfiguration configuration);

    abstract CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration);

    abstract CompletableFuture<StreamConfiguration> getConfigurationData();

    abstract CompletableFuture<Void> createStateIfAbsent(final State state);

    abstract CompletableFuture<Void> setStateData(final Data<T> state);

    abstract CompletableFuture<Data<T>> getStateData();

    abstract CompletableFuture<Void> createSegmentTableIfAbsent(final Data<T> data);

    abstract CompletableFuture<Segment> getSegmentRow(final int number);

    abstract CompletableFuture<Data<T>> getSegmentTable();

    abstract CompletableFuture<Data<T>> getSegmentTableFromStore();

    abstract CompletableFuture<Void> setSegmentTable(final Data<T> data);

    abstract CompletableFuture<Void> createIndexTableIfAbsent(final Data<T> data);

    abstract CompletableFuture<Data<T>> getIndexTable();

    abstract CompletableFuture<Void> updateIndexTable(final Data<T> updated);

    abstract CompletableFuture<Void> createHistoryTableIfAbsent(final Data<T> data);

    abstract CompletableFuture<Void> updateHistoryTable(final Data<T> updated);

    abstract CompletableFuture<Data<T>> getHistoryTable();

    abstract CompletableFuture<Data<T>> getHistoryTableFromStore();

    abstract CompletableFuture<Void> createEpochNodeIfAbsent(int epoch);

    abstract CompletableFuture<Void> deleteEpochNode(int epoch);

    abstract CompletableFuture<Integer> createNewTransaction(final UUID txId,
                                                             final long timestamp,
                                                             final long leaseExpiryTime,
                                                             final long maxExecutionExpiryTime,
                                                             final long scaleGracePeriod);

    abstract CompletableFuture<Integer> getTransactionEpoch(UUID txId);

    abstract CompletableFuture<Data<Integer>> getActiveTx(final int epoch, final UUID txId);

    abstract CompletableFuture<Void> updateActiveTx(final int epoch,
                                                    final UUID txId,
                                                    final Data<Integer> data);

    abstract CompletableFuture<Void> sealActiveTx(final int epoch,
                                                  final UUID txId, final boolean commit,
                                                  final ActiveTxnRecord txnRecord,
                                                  final int version);

    abstract CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId);

    abstract CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp);

    abstract CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp);

    abstract CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<T> data);

    abstract CompletableFuture<Void> removeMarkerData(int segmentNumber);

    abstract CompletableFuture<Data<T>> getMarkerData(int segmentNumber);

    abstract CompletableFuture<Map<String, Data<T>>> getCurrentTxns();

    abstract CompletableFuture<Void> checkScopeExists() throws StoreException;
}
