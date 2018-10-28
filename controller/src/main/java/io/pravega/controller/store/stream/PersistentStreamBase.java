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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.StoreException.DataNotFoundException;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryIndexRecord;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.RetentionRecord;
import io.pravega.controller.store.stream.tables.SealedSegmentsRecord;
import io.pravega.controller.store.stream.tables.SegmentRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StateRecord;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Lombok;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;
import static java.util.stream.Collectors.toMap;

@Slf4j
public abstract class PersistentStreamBase implements Stream {

    private final String scope;
    private final String name;

    PersistentStreamBase(final String scope, final String name) {
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
     * 
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
    public CompletableFuture<CreateStreamResponse> create(final StreamConfiguration configuration, long createTimestamp, int startingSegmentNumber) {

        return checkScopeExists()
                .thenCompose((Void v) -> checkStreamExists(configuration, createTimestamp, startingSegmentNumber))
                .thenCompose(createStreamResponse -> storeCreationTimeIfAbsent(createStreamResponse.getTimestamp())
                        .thenCompose((Void v) -> createConfigurationIfAbsent(
                                StreamConfigurationRecord.complete(createStreamResponse.getConfiguration()).toByteArray()))
                        .thenCompose((Void v) -> createTruncationDataIfAbsent(StreamTruncationRecord.EMPTY.toByteArray()))
                        .thenCompose((Void v) -> createEpochTransitionDataIfAbsent(EpochTransitionRecord.EMPTY.toByteArray()))
                        .thenCompose((Void v) -> createCommittingTxnRecord(CommittingTransactionsRecord.EMPTY.toByteArray()))
                        .thenCompose((Void v) -> createStateIfAbsent(StateRecord.create(State.CREATING).toByteArray()))
                        .thenCompose((Void v) -> createNewSegmentTableWithIndex(createStreamResponse.getConfiguration(),
                                createStreamResponse.getTimestamp(), createStreamResponse.getStartingSegmentNumber()))
                        .thenCompose((Void v) -> createHistoryIndexIfAbsent(TableHelper.createHistoryIndex()))
                        .thenCompose((Void v) -> {
                            final int numSegments = createStreamResponse.getConfiguration().getScalingPolicy().getMinNumSegments();
                            final byte[] historyTable = TableHelper.createHistoryTable(createStreamResponse.getTimestamp(),
                                    IntStream.range(createStreamResponse.getStartingSegmentNumber(), createStreamResponse.getStartingSegmentNumber() + numSegments)
                                             .boxed()
                                             .map(x -> computeSegmentId(x, 0))
                                             .collect(Collectors.toList()));
                            return createHistoryTableIfAbsent(historyTable);
                        })
                        .thenCompose((Void v) -> createSealedSegmentsRecord(new SealedSegmentsRecord(Collections.emptyMap()).toByteArray()))
                        .thenCompose((Void v) -> createRetentionSet(new RetentionRecord(Collections.emptyList()).toByteArray()))
                        .thenApply((Void v) -> createStreamResponse));
    }

    private CompletableFuture<Void> createNewSegmentTableWithIndex(final StreamConfiguration configuration, long timestamp, int startingSegmentNumber) {
        final int numSegments = configuration.getScalingPolicy().getMinNumSegments();
        final double keyRangeChunk = 1.0 / numSegments;

        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        final Pair<byte[], byte[]> segmentTableAndIndex = TableHelper.createSegmentTableAndIndex(newRanges, timestamp, startingSegmentNumber);

        return createSegmentIndexIfAbsent(segmentTableAndIndex.getKey())
                .thenCompose((Void v) -> createSegmentTableIfAbsent(segmentTableAndIndex.getValue()));
    }

    @Override
    public CompletableFuture<Void> delete() {
        return deleteStream();
    }

    @Override
    public CompletableFuture<Void> startTruncation(final Map<Long, Long> streamCut) {
        return Futures.allOfWithResults(streamCut.keySet().stream().map(x -> getSegment(x).thenApply(segment ->
                new SimpleEntry<>(segment.getKeyStart(), segment.getKeyEnd())))
                .collect(Collectors.toList()))
                .thenAccept(TableHelper::validateStreamCut)
                .thenCompose(valid -> getTruncationData(true)
                        .thenCompose(truncationData -> {
                            Preconditions.checkNotNull(truncationData);
                            StreamTruncationRecord previous = StreamTruncationRecord.parse(truncationData.getData());
                            Exceptions.checkArgument(!previous.isUpdating(), "TruncationRecord", "Truncation record conflict");

                            return computeTruncationRecord(previous, streamCut)
                                    .thenCompose(prop -> setTruncationData(
                                            new Data(prop.toByteArray(), truncationData.getVersion())));
                        }));
    }

    private CompletableFuture<StreamTruncationRecord> computeTruncationRecord(StreamTruncationRecord truncationRecord,
                                                                              Map<Long, Long> streamCut) {
        log.debug("computing truncation for stream {}/{}", scope, name);
        return getHistoryIndexFromStore()
                .thenCompose(historyIndex -> getHistoryTableFromStore()
                        .thenCompose(history -> getSegmentIndexFromStore()
                                .thenCompose(segmentIndex -> getSegmentTableFromStore()
                                        .thenApply(segmentTable -> TableHelper.computeTruncationRecord(historyIndex.getData(), history.getData(),
                                                segmentIndex.getData(), segmentTable.getData(), streamCut, truncationRecord)))));
    }

    @Override
    public CompletableFuture<Void> completeTruncation(VersionedMetadata<StreamTruncationRecord> record) {
        Preconditions.checkNotNull(record);
        Preconditions.checkArgument(record.getObject().isUpdating());
        StreamTruncationRecord current = record.getObject();
        StreamTruncationRecord completedProp = StreamTruncationRecord.complete(current);

        return setTruncationData(new Data(completedProp.toByteArray(), record.getVersion()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationRecord() {
        return getTruncationData(true)
                .thenApply(data -> {
                    StreamTruncationRecord truncationRecord = StreamTruncationRecord.parse(data.getData());
                    return new VersionedMetadata<>(truncationRecord, data.getVersion());
                });
    }

    /**
     * Update configuration at configurationPath.
     *
     * @param newConfiguration new stream configuration.
     * @return future of operation.
     */
    @Override
    public CompletableFuture<Void> startUpdateConfiguration(final StreamConfiguration newConfiguration) {
        return getConfigurationData(true)
                .thenCompose(configData -> {
                    StreamConfigurationRecord previous = StreamConfigurationRecord.parse(configData.getData());
                    Preconditions.checkNotNull(previous);
                    Preconditions.checkArgument(!previous.isUpdating());
                    StreamConfigurationRecord update = StreamConfigurationRecord.update(newConfiguration);
                    return setConfigurationData(new Data(update.toByteArray(), configData.getVersion()));
                });
    }

    /**
     * Update configuration at configurationPath.
     *
     * @param existing versioned record
     *
     * @return future of operation
     */
    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<StreamConfigurationRecord> existing) {
        StreamConfigurationRecord current = existing.getObject();
        Preconditions.checkNotNull(current);
        Preconditions.checkArgument(current.isUpdating());
        StreamConfigurationRecord newProperty = StreamConfigurationRecord.complete(current.getStreamConfiguration());
        log.debug("Completing update configuration for stream {}/{}", scope, name);
        return setConfigurationData(new Data(newProperty.toByteArray(), existing.getVersion()));
    }

    /**
     * Fetch configuration at configurationPath.
     *
     * @return Future of stream configuration
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getConfigurationData(false).thenApply(data -> StreamConfigurationRecord.parse(data.getData()).getStreamConfiguration());
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getVersionedConfigurationRecord() {
        return getConfigurationData(true)
                .thenApply(data -> new VersionedMetadata<>(StreamConfigurationRecord.parse(data.getData()), data.getVersion()));
    }

    @Override
    public CompletableFuture<Boolean> updateState(final State state) {
        return getStateData(true)
                .thenCompose(currState -> {
                    if (State.isTransitionAllowed(StateRecord.parse(currState.getData()).getState(), state)) {
                        return setStateData(new Data(StateRecord.builder().state(state).build().toByteArray(), currState.getVersion()))
                                .thenApply(x -> true);
                    } else {
                        return Futures.failedFuture(StoreException.create(
                                StoreException.Type.OPERATION_NOT_ALLOWED,
                                "Stream: " + getName() + " State: " + state.name() + " current state = " + StateRecord.parse(currState.getData()).getState()));
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<State>> getVersionedState() {
        return getStateData(true)
                .thenApply(x -> new VersionedMetadata<>(StateRecord.parse(x.getData()).getState(), x.getVersion()));
    }
    
    @Override
    public CompletableFuture<VersionedMetadata<State>> updateVersionedState(final VersionedMetadata<State> previous, final State newState) {
        if (State.isTransitionAllowed(previous.getObject(), newState)) {
            return setStateData(new Data(StateRecord.builder().state(newState).build().toByteArray(), previous.getVersion()))
                    .thenApply(updatedVersion -> new VersionedMetadata<>(newState, updatedVersion));
        } else {
            return Futures.failedFuture(StoreException.create(
                    StoreException.Type.OPERATION_NOT_ALLOWED,
                    "Stream: " + getName() + " State: " + newState.name() + " current state = " +
                            previous.getObject()));
        }
    }

    @Override
    public CompletableFuture<State> getState(boolean ignoreCached) {
        return getStateData(ignoreCached)
                .thenApply(x -> StateRecord.parse(x.getData()).getState());
    }

    /**
     * Fetch the segment table and retrieve the segment.
     *
     * @param segmentId segment id.
     * @return : future of segment
     */
    @Override 
    public CompletableFuture<Segment> getSegment(final long segmentId) {
        return verifyLegalState().thenCompose(v -> getSegmentRow(segmentId));
    }

    @Override
    public CompletableFuture<List<ScaleMetadata>> getScaleMetadata() {
        return verifyLegalState()
                .thenCompose(v -> getHistoryIndex())
                    .thenCompose(historyIndex -> getHistoryTable()
                        .thenApply(historyTable -> TableHelper.getScaleMetadata(historyIndex.getData(), historyTable.getData()))
                .thenCompose(listOfScaleRecords ->
                        Futures.allOfWithResults(listOfScaleRecords.stream().map(record -> {
                            long scaleTs = record.getLeft();
                            CompletableFuture<List<Segment>> list = Futures.allOfWithResults(
                                    record.getRight().stream().map(this::getSegment)
                                            .collect(Collectors.toList()));
                            return list.thenApply(segments -> new ImmutablePair<>(scaleTs, segments));
                        }).collect(Collectors.toList())))
                .thenApply(this::mapToScaleMetadata));
    }

    private List<ScaleMetadata> mapToScaleMetadata(List<ImmutablePair<Long, List<Segment>>> scalePair) {
        final AtomicReference<List<Segment>> previous = new AtomicReference<>();
        return scalePair.stream()
                .map(pair -> {
                    long splits = 0;
                    long merges = 0;
                    if (previous.get() != null) {
                        splits = findSegmentSplitsMerges(previous.get(), pair.right);
                        merges = findSegmentSplitsMerges(pair.right, previous.get());
                    }
                    previous.set(pair.getRight());
                    return new ScaleMetadata(pair.left, pair.right, splits, merges);
        }).collect(Collectors.toList());
    }

    /**
     * Method to calculate number of splits and merges.
     *
     * Principle to calculate the number of splits and merges:
     * 1- An event has occurred if a reference range is present (overlaps) in at least two consecutive target ranges.
     * 2- If the direction of the check in 1 is forward, then it is a split, otherwise it is a merge.
     *
     * @param referenceSegmentsList Reference segment list.
     * @param targetSegmentsList Target segment list.
     * @return Number of splits/merges.
     */
    private long findSegmentSplitsMerges(List<Segment> referenceSegmentsList, List<Segment> targetSegmentsList) {
        return referenceSegmentsList.stream().filter(
                segment -> targetSegmentsList.stream().filter(target -> target.overlaps(segment)).count() > 1 ).count();
    }

    private CompletableFuture<List<Segment>> findOverlapping(Segment segment, List<Long> candidates) {
        return verifyLegalState().thenCompose(v -> Futures.allOfWithResults(candidates.stream()
                                                                                      .map(this::getSegment)
                                                                                      .collect(Collectors.toList())))
                                 .thenApply(successorCandidates -> successorCandidates.stream()
                                                                                      .filter(x -> x.overlaps(segment))
                                                                                      .collect(Collectors.toList()));
    }

    private CompletableFuture<List<Segment>> getSuccessorsForSegment(final long segmentId) {
        return getHistoryIndex()
                .thenCompose(historyIndex -> getHistoryTable()
                        .thenCompose(historyTable -> getSegment(segmentId)
                                .thenCompose(segment -> {
                                    List<Long> candidates = TableHelper.findSegmentSuccessorCandidates(segment,
                                            historyIndex.getData(),
                                            historyTable.getData());
                                    return findOverlapping(segment, candidates);
                                })));
    }

    @Override
    public CompletableFuture<Map<Long, List<Long>>> getSuccessorsWithPredecessors(final long segmentId) {
        // Ensure the order, we should first get history table followed by segment table because during scale we first write to
        // segment table followed by history table. So if a record exists in history table, then we are guaranteed to find it in
        // segment table.
        return verifyLegalState()
                .thenCompose(legal -> getHistoryIndex()
                        .thenCompose(historyIndex -> getHistoryTable()
                                .thenCompose(historyTable -> getSuccessorsForSegment(segmentId)
                                        .thenCompose(successors -> {
                                            List<CompletableFuture<Map.Entry<Segment, List<Long>>>> resultFutures = new ArrayList<>();

                                            for (Segment successor : successors) {
                                                List<Long> candidates = TableHelper.findSegmentPredecessorCandidates(successor,
                                                        historyIndex.getData(),
                                                        historyTable.getData());
                                                resultFutures.add(findOverlapping(successor, candidates).thenApply(
                                                        list -> new SimpleImmutableEntry<>(successor, list.stream().map(Segment::segmentId)
                                                                .collect(Collectors.toList()))));
                                            }
                                            return Futures.allOfWithResults(resultFutures);
                                        })
                                        .thenApply(list -> list.stream().collect(Collectors.toMap(e -> e.getKey().segmentId(), Map.Entry::getValue)))
                                )));
    }

    @Override
    public CompletableFuture<List<Long>> getActiveSegments() {
        return verifyLegalState()
                .thenCompose(v -> getHistoryIndex()
                        .thenCompose(historyIndex -> getHistoryTable()
                                .thenApply(historyTable -> TableHelper.getActiveSegments(historyIndex.getData(), historyTable.getData()))));
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
    public CompletableFuture<Map<Long, Long>> getActiveSegments(final long timestamp) {
        return getTruncationData(false)
                .thenApply(data -> StreamTruncationRecord.parse(data.getData()))
                .thenCompose(truncationRecord -> getHistoryIndex()
                        .thenCompose(historyIndex -> getHistoryTable()
                                .thenCompose(historyTable -> getSegmentIndex()
                                        .thenCompose(segmentIndex -> getSegmentTable()
                                                .thenApply(segmentTable ->
                                                        TableHelper.getActiveSegments(timestamp,
                                                                historyIndex.getData(),
                                                                historyTable.getData(),
                                                                segmentIndex.getData(),
                                                                segmentTable.getData(),
                                                                truncationRecord))
                                        ))));
    }

    @Override
    public CompletableFuture<List<Long>> getActiveSegments(final int epoch) {
        return getHistoryIndex()
                .thenCompose(historyIndex -> getHistoryTable()
                        .thenApply(historyTable -> TableHelper.getSegmentsInEpoch(historyIndex.getData(), historyTable.getData(), epoch)));
    }

    @Override
    public CompletableFuture<List<Segment>> getSegmentsBetweenStreamCuts(Map<Long, Long> from, Map<Long, Long> to) {
        return getHistoryIndex()
                .thenCompose(historyIndex -> getHistoryTable()
                        .thenCompose(historyTable -> getSegmentIndex()
                                .thenCompose(segmentIndex -> getSegmentTable()
                                        .thenApply(segmentTable ->
                                                TableHelper.findSegmentsBetweenStreamCuts(historyIndex.getData(), historyTable.getData(),
                                                        segmentIndex.getData(), segmentTable.getData(), from, to)))));
    }

    @Override
    public CompletableFuture<Boolean> isStreamCutValid(Map<Long, Long> streamCut) {
        return Futures.allOfWithResults(streamCut.keySet().stream().map(x -> getSegment(x).thenApply(segment ->
                new SimpleEntry<>(segment.getKeyStart(), segment.getKeyEnd())))
                .collect(Collectors.toList()))
                .thenAccept(TableHelper::validateStreamCut)
                .handle((r, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof IllegalArgumentException) {
                            return false;
                        } else {
                            log.warn("Exception while trying to validate a stream cut for stream {}/{}", scope, name);
                            throw Lombok.sneakyThrow(e);
                        }
                    } else {
                        return true;
                    }
                });
    }

    /**
     * This method attempts to start a new scale workflow. For this it first computes epoch transition and stores it in the metadastore.
     * This method can be called by manual scale or during the processing of auto-scale event. Which means there could be
     * concurrent calls to this method.
     *
     * @param segmentsToSeal segments that will be sealed at the end of this scale operation.
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @param existing       existing epoch transition record
     * @return : list of newly created segments with current epoch
     */
    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> submitScale(final List<Long> segmentsToSeal,
                                                                                   final List<SimpleEntry<Double, Double>> newRanges,
                                                                                   final long scaleTimestamp,
                                                                                   final VersionedMetadata<EpochTransitionRecord> existing) {
        return verifyNotSealed().thenCompose(v -> {
            if (existing == null) {
                return getEpochTransition();
            } else {
                return CompletableFuture.completedFuture(existing);
            }
        }).thenCompose(record -> {
            if (!record.getObject().equals(EpochTransitionRecord.EMPTY)) {
                // verify that it's the same as the supplied input (--> segments to be sealed
                // and new ranges are identical). else throw scale conflict exception
                if (!verifyRecordMatchesInput(segmentsToSeal, newRanges, record.getObject())) {
                    log.debug("scale conflict, another scale operation is ongoing");
                    throw new EpochTransitionOperationExceptions.ConflictException();
                }
                return CompletableFuture.completedFuture(record);
            }

            return getHistoryIndexFromStore()
                    .thenCompose(historyIndex -> getHistoryTableFromStore()
                            .thenCompose(historyTable -> getSegmentIndexFromStore()
                                    .thenCompose(segmentIndex -> getSegmentTableFromStore()
                                            .thenCompose(segmentTable -> {
                                                if (!TableHelper.isScaleInputValid(segmentsToSeal, newRanges, segmentIndex.getData(),
                                                        segmentTable.getData())) {
                                                    log.error("scale input invalid {} {}", segmentsToSeal, newRanges);
                                                    throw new EpochTransitionOperationExceptions.InputInvalidException();
                                                }

                                                return submitScale(segmentsToSeal, newRanges, scaleTimestamp, record,
                                                        historyIndex, historyTable, segmentIndex, segmentTable);
                                            }))));
        });
    }

    private CompletableFuture<VersionedMetadata<EpochTransitionRecord>> submitScale(List<Long> segmentsToSeal, 
                                                                                    List<SimpleEntry<Double, Double>> newRanges,
                                                                                    long scaleTimestamp, 
                                                                                    VersionedMetadata<EpochTransitionRecord> record, 
                                                                                    Data historyIndex, Data historyTable, 
                                                                                    Data segmentIndex, Data segmentTable) {
            // check input is valid and satisfies preconditions
            if (!TableHelper.canScaleFor(segmentsToSeal, historyIndex.getData(), historyTable.getData())) {
                // invalid input, log and ignore
                Data updated = new Data(EpochTransitionRecord.EMPTY.toByteArray(), record.getVersion());
                return updateEpochTransitionNode(updated)
                        .thenApply(x -> {
                            log.warn("scale precondition failed {}", segmentsToSeal);
                            throw new EpochTransitionOperationExceptions.PreConditionFailureException();
                        });
            }
            
            EpochTransitionRecord epochTransition = TableHelper.computeEpochTransition(
                    historyIndex.getData(), historyTable.getData(), segmentIndex.getData(),
                    segmentTable.getData(), segmentsToSeal, newRanges, scaleTimestamp);

            return updateEpochTransitionNode(new Data(epochTransition.toByteArray(), record.getVersion()))
                    .thenApply(version -> {
                        log.info("scale for stream {}/{} accepted. Segments to seal = {}", scope, name,
                                epochTransition.getSegmentsToSeal());
                        return new VersionedMetadata<>(epochTransition, version);
                    });
    }

    private boolean verifyRecordMatchesInput(List<Long> segmentsToSeal, List<SimpleEntry<Double, Double>> newRanges, EpochTransitionRecord record) {
        boolean newRangeMatch = newRanges.stream().allMatch(x ->
                record.getNewSegmentsWithRange().values().stream()
                        .anyMatch(y -> y.getKey().equals(x.getKey())
                                && y.getValue().equals(x.getValue())));
        boolean segmentsToSealMatch = segmentsToSeal.containsAll(record.getSegmentsToSeal());

        return newRangeMatch && segmentsToSealMatch;
    }

    private CompletableFuture<Void> verifyNotSealed() {
        return getState(false).thenApply(state -> {
            if (state.equals(State.SEALING) || state.equals(State.SEALED)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> startScale(boolean isManualScale,
                                                                                  VersionedMetadata<EpochTransitionRecord> record,
                                                                                  VersionedMetadata<State> state) {
        return getHistoryIndexFromStore().thenCompose(historyIndex -> getHistoryTableFromStore()
                .thenCompose(historyTable -> getSegmentIndexFromStore().thenCompose(segmentIndex -> getSegmentTableFromStore()
                        .thenCompose(segmentTable -> {
                            if (isManualScale) {
                                return migrateManualScaleToNewEpoch(record, state, historyIndex, historyTable, segmentIndex, segmentTable);
                            } else {
                                // if rolling transactions happened before the scale could be picked up, we will discard
                                // epoch transition record and reset the state to active.
                                return discardInconsistentEpochTransition(historyIndex, historyTable, segmentIndex, segmentTable,
                                        record, state);
                            }
                        }))));
    }

    @Override
    public CompletableFuture<Void> scaleCreateNewSegments(VersionedMetadata<EpochTransitionRecord> record) {
        // Called after start scale to indicate store to create new segments in the segment table. This method takes care of
        // checking for idempotent addition of segments to the table.
        return getHistoryIndexFromStore().thenCompose(historyIndex -> getHistoryTableFromStore()
                .thenCompose(historyTable -> getSegmentIndexFromStore().thenCompose(segmentIndex -> getSegmentTableFromStore()
                     .thenCompose(segmentTable -> {
                            // Idempotent update to index and table.
                            EpochTransitionRecord epochTransition = record.getObject();
                            int newEpoch = epochTransition.getNewEpoch();

                            final SegmentRecord latestSegment = TableHelper.getLatestSegmentRecord(segmentIndex.getData(),
                                    segmentTable.getData());
                            final HistoryRecord activeEpoch = TableHelper.getActiveEpoch(historyIndex.getData(),
                                    historyTable.getData());

                            if (latestSegment.getCreationEpoch() < newEpoch && activeEpoch.getEpoch() == epochTransition.getActiveEpoch()) {
                                log.info("Scale {}/{} for segments started. Creating new segments. SegmentsToSeal {}",
                                        scope, name, epochTransition.getSegmentsToSeal());

                                return createNewSegments(
                                        historyIndex.getData(), historyTable.getData(), segmentIndex, segmentTable,
                                        epochTransition);
                            } else {
                                // idempotent
                                return CompletableFuture.completedFuture(null);
                            }
                        }))));
    }

    private CompletableFuture<VersionedMetadata<EpochTransitionRecord>> discardInconsistentEpochTransition(Data historyIndex, Data historyTable,
                                                                       Data segmentIndex, Data segmentTable,
                                                                       VersionedMetadata<EpochTransitionRecord> epochTransition,
                                                                       VersionedMetadata<State> state) {
        // verify that epoch transition is consistent with segments in the table.
        if (TableHelper.isEpochTransitionConsistent(epochTransition.getObject(), historyIndex.getData(), historyTable.getData(),
                segmentIndex.getData(), segmentTable.getData())) {
            return CompletableFuture.completedFuture(epochTransition);
        } else {
            return updateEpochTransitionNode(new Data(EpochTransitionRecord.EMPTY.toByteArray(), epochTransition.getVersion()))
                    .thenCompose(v -> resetStateConditionally(state, State.SCALING))
                    .thenApply(v -> {
                        log.warn("Scale epoch transition record is inconsistent with data in the table. {}",
                                epochTransition.getObject().getNewEpoch());
                        throw new IllegalStateException("Epoch transition record is inconsistent.");
                    });
        }
    }

    private CompletableFuture<Void> createNewSegments(final byte[] historyIndex,
                                                           final byte[] historyTable,
                                                           final Data segmentIndex,
                                                           final Data segmentTable,
                                                           final EpochTransitionRecord epochTransitionRecord) {
        // Ensure that segment.creation time is monotonically increasing after each new scale.
        // because scale time could be supplied by a controller with a skewed clock, we should:
        // take max(scaleTime, lastScaleTime + 1, System.currentTimeMillis)
        long lastScaleTime = TableHelper.getEpochScaleTime(epochTransitionRecord.getActiveEpoch(), historyIndex, historyTable);
        long scaleEventTime = Math.max(System.currentTimeMillis(), epochTransitionRecord.getTime());
        long segmentCreationTimestamp = Math.max(scaleEventTime, lastScaleTime + 1);

        // Note: if segment index was updated in an earlier attempt but segment was not, we need to overwrite the
        // previous index update!
        // This is because new offsets may be different. we cannot update segment table before index because then
        // we would not be able to read from segment table as we don't know the starting offsets for segments.
        assert !epochTransitionRecord.getNewSegmentsWithRange().isEmpty();
        List<SimpleEntry<Double, Double>> newRanges = epochTransitionRecord.getNewSegmentsWithRange().entrySet()
                .stream().sorted(Comparator.comparingLong(Map.Entry::getKey)).map(Map.Entry::getValue)
                .collect(Collectors.toList());

        int nextSegmentNumber = epochTransitionRecord.getNewSegmentsWithRange().keySet().stream().mapToInt(StreamSegmentNameUtils::getSegmentNumber).min().getAsInt();
        final Pair<byte[], byte[]> updated = TableHelper.addNewSegmentsToSegmentTableAndIndex(nextSegmentNumber,
                epochTransitionRecord.getNewEpoch(), segmentIndex.getData(), segmentTable.getData(), newRanges, segmentCreationTimestamp);

        final Data updatedSegmentIndex = new Data(updated.getKey(), segmentIndex.getVersion());
        final Data updatedSegmentTable = new Data(updated.getValue(), segmentTable.getVersion());

        return updateSegmentIndex(updatedSegmentIndex)
                .thenCompose(v -> updateSegmentTable(updatedSegmentTable))
                .thenAccept(v -> log.info("scale {}/{} new segments created successfully", scope, name));
    }

    private CompletionStage<VersionedMetadata<EpochTransitionRecord>> migrateManualScaleToNewEpoch(VersionedMetadata<EpochTransitionRecord> record,
                                           VersionedMetadata<State> state, Data historyIndex, Data historyTable, Data segmentIndex, Data segmentTable) {
        EpochTransitionRecord epochTransition = record.getObject();
        HistoryRecord activeEpoch = TableHelper.getActiveEpoch(historyIndex.getData(), historyTable.getData());
        HistoryRecord recordActiveEpoch = TableHelper.getEpochRecord(historyIndex.getData(), historyTable.getData(), epochTransition.getActiveEpoch());
        if (epochTransition.getActiveEpoch() == activeEpoch.getEpoch()) {
            // no migration needed
            return CompletableFuture.completedFuture(record);
        } else if (activeEpoch.getEpoch() > epochTransition.getActiveEpoch() && activeEpoch.getReferenceEpoch() == recordActiveEpoch.getReferenceEpoch()) {
            List<Long> duplicateSegmentsToSeal = epochTransition.getSegmentsToSeal().stream()
                    .map(x -> computeSegmentId(getSegmentNumber(x), activeEpoch.getEpoch()))
                    .collect(Collectors.toList());

            EpochTransitionRecord updatedRecord = TableHelper.computeEpochTransition(
                    historyIndex.getData(), historyTable.getData(), segmentIndex.getData(),
                    segmentTable.getData(), duplicateSegmentsToSeal, epochTransition.getNewSegmentsWithRange().values().asList(),
                    epochTransition.getTime());
            return updateEpochTransitionNode(new Data(updatedRecord.toByteArray(), record.getVersion()))
                    .thenApply(x -> new VersionedMetadata<>(updatedRecord, x));
        } else {
            return updateEpochTransitionNode(new Data(EpochTransitionRecord.EMPTY.toByteArray(), record.getVersion()))
                    .thenCompose(v -> resetStateConditionally(state, State.SCALING))
                    .thenApply(v -> {
                        log.warn("Scale epoch transition record is inconsistent with data in the table. {}",
                                epochTransition.getNewEpoch());
                        throw new IllegalStateException("Epoch transition record is inconsistent.");
                    });
        }
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransition() {
        return getEpochTransitionNode()
                .thenApply(x -> new VersionedMetadata<>(EpochTransitionRecord.parse(x.getData()), x.getVersion()));
    }

    /**
     * Segments created with pravega, update the history table with this fact so they are available as successors
     * 3. Add entry into the history table.
     *
     * @param record epoch transition record
     *               
     * @return Future which when complete will have the history record updated in store.
     */
    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated(VersionedMetadata<EpochTransitionRecord> record) {
        return addPartialHistoryRecordAndIndex(record.getObject());
    }

    /**
     * update history table if not already updated:
     * fetch last record from history table.
     * if eventTime is >= scale.scaleTimeStamp do nothing, else create record
     *
     * @return : future of history table offset for last entry
     */
    private CompletableFuture<Void> addPartialHistoryRecordAndIndex(final EpochTransitionRecord epochTransition) {
        final Set<Long> segmentsToSeal = epochTransition.getSegmentsToSeal();
        final Set<Long> createdSegments = epochTransition.getNewSegmentsWithRange().keySet();
        final int activeEpoch = epochTransition.getActiveEpoch();
        final int newEpoch = epochTransition.getNewEpoch();

        return getHistoryIndexFromStore()
                .thenCompose(historyIndex -> getHistoryTableFromStore()
                        .thenCompose(historyTable -> {
                            final HistoryRecord lastRecord = HistoryRecord.readLatestRecord(historyIndex.getData(), historyTable.getData(),
                                    false).get();

                            // idempotent check
                            if (lastRecord.getEpoch() > activeEpoch) {
                                log.debug("{}/{} scale op for epoch {} - history record already added", scope, name, activeEpoch);
                                return CompletableFuture.completedFuture(null);
                            }

                            final List<Long> newActiveSegments = getNewActiveSegments(createdSegments, segmentsToSeal, lastRecord);
                            final int offset = historyTable.getData().length;
                            // now we know the offset at which we want to add.
                            final byte[] updatedTable = TableHelper.addPartialRecordToHistoryTable(historyIndex.getData(),
                                    historyTable.getData(), newActiveSegments);
                            final Data updated = new Data(updatedTable, historyTable.getVersion());

                            return addHistoryIndexRecord(newEpoch, offset)
                                    .thenCompose(v -> updateHistoryTable(updated))
                                    .whenComplete((r, e) -> {
                                        if (e == null) {
                                            log.debug("{}/{} scale op for epoch {}. Creating new epoch and updating history table.",
                                                    scope, name, activeEpoch);
                                        } else {
                                            log.warn("{}/{} scale op for epoch {}. Failed to add partial record to history table. {}",
                                                    scope, name, activeEpoch, e.getClass().getName());
                                        }
                                    });
                        }));
    }

    private CompletableFuture<Void> clearMarkers(final Set<Long> segments) {
        return Futures.toVoid(Futures.allOfWithResults(segments.stream().parallel()
                .map(this::removeColdMarker).collect(Collectors.toList())));
    }

    /**
     * Remainder of scale metadata update. Also set the state back to active.
     * 4. complete entry into the history table.
     * 5. Add entry into the index table.
     * 6. Delete epoch transition record.
     *
     * @param sealedSegmentSizes sealed segments with sizes
     * @param record
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> scaleOldSegmentsSealed(Map<Long, Long> sealedSegmentSizes, VersionedMetadata<EpochTransitionRecord> record) {
        return Futures.toVoid(clearMarkers(record.getObject().getSegmentsToSeal())
                                    .thenCompose(x -> {
                                        EpochTransitionRecord epochTransition = record.getObject();
                                        return completePartialRecordInHistory(sealedSegmentSizes, epochTransition.getActiveEpoch(), 
                                                epochTransition.getNewEpoch(), epochTransition.getTime());
                                    }));
    }

    @Override
    public CompletableFuture<Void> completeScale(VersionedMetadata<EpochTransitionRecord> record) {
        Preconditions.checkNotNull(record);
        Preconditions.checkArgument(!record.getObject().equals(EpochTransitionRecord.EMPTY));
        return Futures.toVoid(updateEpochTransitionNode(new Data(EpochTransitionRecord.EMPTY.toByteArray(), record.getVersion())));
    }

    private CompletableFuture<Void> completePartialRecordInHistory(final Map<Long, Long> sealedSegments, final int activeEpoch,
                                                                   final int newEpoch, final long time) {
        return getHistoryIndexFromStore()
                .thenCompose(historyIndex -> getHistoryTableFromStore()
                        .thenCompose(historyTable -> {
                            final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyIndex.getData(),
                                    historyTable.getData(), false);
                            assert lastRecordOpt.isPresent();
                            final HistoryRecord lastRecord = lastRecordOpt.get();

                            // idempotent check
                            if (lastRecord.getEpoch() > newEpoch || (!lastRecord.isPartial() && lastRecord.getEpoch() == newEpoch)) {
                                log.debug("{}/{} epoch transition already completed for epoch {}.", scope, name, activeEpoch);

                                return CompletableFuture.completedFuture(null);
                            } 

                            long timestamp = Math.max(System.currentTimeMillis(), time);
                            final HistoryRecord previous = HistoryRecord.fetchPrevious(lastRecord, historyIndex.getData(),
                                    historyTable.getData()).get();
                            // To ensure that we always have ascending time in history records irrespective of controller
                            // clock mismatches.
                            timestamp = Math.max(timestamp, previous.getScaleTime() + 1);

                            byte[] updatedTable = TableHelper.completePartialRecordInHistoryTable(historyIndex.getData(), historyTable.getData(),
                                    lastRecord, timestamp);
                            final Data updated = new Data(updatedTable, historyTable.getVersion());

                            return addSealedSegmentsToRecord(sealedSegments)
                                    .thenCompose(x -> updateHistoryTable(updated))
                                    .whenComplete((r, e) -> {
                                        if (e != null) {
                                            log.warn("{}/{} attempt to complete epoch transition for epoch {}. {}", scope, name, activeEpoch,
                                                    e.toString());
                                        } else {
                                            log.debug("{}/{} epoch transition complete, index and history tables updated for epoch {}.",
                                                    scope, name, activeEpoch);
                                        }
                                    });
                        }));
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startRollingTxn(int activeEpoch,
                                                                  VersionedMetadata<CommittingTransactionsRecord> existing) {
        CommittingTransactionsRecord record = existing.getObject();
        if (activeEpoch == record.getActiveEpoch()) {
            return CompletableFuture.completedFuture(existing);
        } else {
            CommittingTransactionsRecord update = record.getRollingTxnRecord(activeEpoch);
            return updateCommittingTxnRecord(new Data(update.toByteArray(), existing.getVersion()))
                    .thenApply(version -> new VersionedMetadata<>(update, version));
        }
    }

    @Override
    public CompletableFuture<Void> rollingTxnCreateDuplicateEpochs(
            Map<Long, Long> sealedTxnEpochSegments, long time, VersionedMetadata<CommittingTransactionsRecord> record) {
        Preconditions.checkArgument(record.getObject().isRollingTransactions());
        return addSealedSegmentsToRecord(sealedTxnEpochSegments)
                .thenCompose(x -> getActiveEpoch(true))
                .thenCompose(activeEpoch -> rollingTxnAddNewDuplicateEpochs(record.getObject().getEpoch(), 
                        record.getObject().getActiveEpoch(), time));
    }

    private CompletableFuture<Void> rollingTxnAddNewDuplicateEpochs(final int transactionEpoch, final int activeEpoch, final long time) {
        return getHistoryIndexFromStore()
                .thenCompose(historyIndex -> getHistoryTableFromStore()
                        .thenCompose(historyTable -> {
                            final HistoryRecord lastRecord = HistoryRecord.readLatestRecord(historyIndex.getData(), historyTable.getData(),
                                    false).get();
                            int newEpoch = activeEpoch + 2;

                            // idempotent check
                            if (lastRecord.getEpoch() > activeEpoch) {
                                log.debug("{}/{} rolling transaction for epoch {} - history record already added", scope, name,
                                        transactionEpoch);
                                return CompletableFuture.completedFuture(null);
                            }

                            final Pair<byte[], byte[]> updatedIndexAndTable = TableHelper.insertDuplicateRecordsInHistoryTable(historyIndex.getData(),
                                    historyTable.getData(), transactionEpoch, time);
                            final Data updatedIndex = new Data(updatedIndexAndTable.getKey(), historyIndex.getVersion());
                            final Data updatedHistory = new Data(updatedIndexAndTable.getValue(), historyTable.getVersion());

                            return updateHistoryIndex(updatedIndex)
                                    .thenCompose(v -> updateHistoryTable(updatedHistory))
                                    .whenComplete((r, e) -> {
                                        if (e == null) {
                                            log.debug("{}/{} rolling transaction for epoch {}. Creating new epoch and updating history table.",
                                                    scope, name, transactionEpoch);
                                        } else {
                                            log.warn("{}/{} rollingTransaction for epoch {}. Failed to add partial record to history table.",
                                                    scope, name, activeEpoch, e);
                                        }
                                    });
                        }));
    }

    @Override
    public CompletableFuture<Void> completeRollingTxn(Map<Long, Long> sealedActiveEpochSegments,
                                                                    long time, VersionedMetadata<CommittingTransactionsRecord> record) {
        Preconditions.checkArgument(record.getObject().isRollingTransactions());

        return addSealedSegmentsToRecord(sealedActiveEpochSegments)
                .thenCompose(x -> clearMarkers(sealedActiveEpochSegments.keySet()))
                .thenCompose(x -> completePartialRecordInHistory(sealedActiveEpochSegments, record.getObject().getActiveEpoch(), 
                        record.getObject().getActiveEpoch() + 2, time));
    }

    private CompletableFuture<VersionedMetadata<State>> resetStateConditionally(VersionedMetadata<State> currState, State state) {
        if (currState.getObject().equals(state)) {
            return updateVersionedState(currState, State.ACTIVE);
        } else {
            return CompletableFuture.completedFuture(currState);
        }
    }

    @Override
    public CompletableFuture<UUID> generateNewTxnId(int msb32Bit, long lsb64Bit) {
        return getActiveEpoch(false)
                .thenApply(epochRecord -> {
                    // always set transaction epoch as refrence epoch so that all transactions on duplicate epochs
                    // are collected.
                    // epochs that are not duplicates will refer to themselves.
                    long msb64Bit = (long) epochRecord.getReferenceEpoch() << 32 | msb32Bit & 0xFFFFFFFFL;
                    return new UUID(msb64Bit, lsb64Bit);
                });
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final UUID txnId,
                                                                         final long lease,
                                                                         final long maxExecutionTime) {
        final long current = System.currentTimeMillis();
        final long leaseTimestamp = current + lease;
        final long maxExecTimestamp = current + maxExecutionTime;
        // extract epoch from txnid
        final int epoch = getTransactionEpoch(txnId);
        ActiveTxnRecord record = ActiveTxnRecord.builder().txnStatus(TxnStatus.OPEN).leaseExpiryTime(leaseTimestamp)
                .txCreationTimestamp(current).maxExecutionExpiryTime(maxExecTimestamp)
                .build();
        return verifyLegalState().thenCompose(v -> createNewTransaction(epoch, txnId, record.toByteArray()))
                .thenApply(v -> new VersionedTransactionData(epoch, txnId, v, TxnStatus.OPEN, current, maxExecTimestamp));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final VersionedTransactionData txnData,
                                                                       final long lease) {
        // Update txn record with new lease value and return versioned tx data.
        final int epoch = txnData.getEpoch();
        final UUID txnId = txnData.getId();
        final Version version = txnData.getVersion();
        final long creationTime = txnData.getCreationTime();
        final long maxExecutionExpiryTime = txnData.getMaxExecutionExpiryTime();
        final TxnStatus status = txnData.getStatus();
        final ActiveTxnRecord newData = new ActiveTxnRecord(creationTime, System.currentTimeMillis() + lease,
                maxExecutionExpiryTime, status);
        final Data data = new Data(newData.toByteArray(), version);

        return updateActiveTx(epoch, txnId, data)
                .thenApply(updatedVersion -> new VersionedTransactionData(epoch, txnId, updatedVersion, status, creationTime, maxExecutionExpiryTime));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId) {
        int epoch = getTransactionEpoch(txId);
        return getActiveTx(epoch, txId)
                .thenApply(data -> {
                    ActiveTxnRecord activeTxnRecord = ActiveTxnRecord.parse(data.getData());
                    return new VersionedTransactionData(epoch, txId, data.getVersion(), activeTxnRecord.getTxnStatus(),
                            activeTxnRecord.getTxCreationTimestamp(), activeTxnRecord.getMaxExecutionExpiryTime());
                });
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId) {
        int epoch = getTransactionEpoch(txId);
        return verifyLegalState().thenCompose(v -> getActiveTx(epoch, txId).handle((ok, ex) -> {
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
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
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return CompletedTxnRecord.parse(ok.getData()).getCompletionStatus();
        });
    }

    @Override
    public CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final UUID txId, final boolean commit,
                                                                              final Optional<Version> version) {
        val legal = verifyLegalState();
        int epoch = getTransactionEpoch(txId);
        return legal.thenCompose(v -> sealActiveTxn(epoch, txId, commit, version))
                                                               .exceptionally(ex -> new SimpleEntry<>(handleDataNotFoundException(ex), null))
                    .thenCompose(pair -> {
                        if (pair.getKey() == TxnStatus.UNKNOWN) {
                            return validateCompletedTxn(txId, commit, "seal").thenApply(status -> new SimpleEntry<>(status, null));
                        } else {
                            return CompletableFuture.completedFuture(pair);
                        }
                    });
    }

    /**
     * Seal a transaction in OPEN/COMMITTING_TXN/ABORTING state. This method does CAS on the transaction data node if
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
                                                                             final Optional<Version> version) {
        return getActiveTx(epoch, txId).thenCompose(data -> {
            ActiveTxnRecord txnRecord = ActiveTxnRecord.parse(data.getData());
            Version dataVersion = version.orElseGet(data::getVersion);
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

    private CompletableFuture<Version> sealActiveTx(final int epoch, final UUID txId, final boolean commit,
                                      final ActiveTxnRecord previous,
                                      final Version version) {
        final ActiveTxnRecord updated = new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                previous.getLeaseExpiryTime(),
                previous.getMaxExecutionExpiryTime(),
                commit ? TxnStatus.COMMITTING : TxnStatus.ABORTING);
        final Data data = new Data(updated.toByteArray(), version);
        return updateActiveTx(epoch, txId, data);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final UUID txId) {
        int epoch = getTransactionEpoch(txId);
        return checkTransactionStatus(txId)
                .thenApply(x -> {
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
    public CompletableFuture<TxnStatus> abortTransaction(final UUID txId) {
        int epoch = getTransactionEpoch(txId);
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

    private CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp) {
        return createCompletedTxEntryData(txId, new CompletedTxnRecord(timestamp, complete).toByteArray());
    }

    @SneakyThrows
    private TxnStatus handleDataNotFoundException(Throwable ex) {
        if (Exceptions.unwrap(ex) instanceof DataNotFoundException) {
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
    public CompletableFuture<HistoryRecord> getActiveEpoch(boolean ignoreCached) {

        return (ignoreCached ? getHistoryIndexFromStore() : getHistoryIndex())
                .thenCompose(historyIndex -> (ignoreCached ? getHistoryTableFromStore() :
                        getHistoryTable())
                        .thenApply(historyTable -> TableHelper.getActiveEpoch(historyIndex.getData(), historyTable.getData())));
    }

    @Override
    public CompletableFuture<HistoryRecord> getEpochRecord(int epoch) {
        return getHistoryIndex()
                .thenCompose(historyIndex -> getHistoryTable()
                        .thenApply(historyTable -> TableHelper.getEpochRecord(historyIndex.getData(), historyTable.getData(), epoch)));
    }

    @Override
    public CompletableFuture<Void> setColdMarker(long segmentId, long timestamp) {
        return verifyLegalState().thenCompose(v -> getMarkerData(segmentId)).thenCompose(x -> {
            if (x != null) {
                byte[] b = new byte[Long.BYTES];
                BitConverter.writeLong(b, 0, timestamp);
                final Data data = new Data(b, x.getVersion());
                return updateMarkerData(segmentId, data);
            } else {
                return createMarkerData(segmentId, timestamp);
            }
        });
    }

    @Override
    public CompletableFuture<Long> getColdMarker(long segmentId) {
        return verifyLegalState().thenCompose(v -> getMarkerData(segmentId))
                                 .thenApply(x -> (x != null) ? BitConverter.readLong(x.getData(), 0) : 0L);
    }

    @Override
    public CompletableFuture<Void> removeColdMarker(long segmentId) {
        return verifyLegalState().thenCompose(v -> removeMarkerData(segmentId));
    }

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(Map<Long, Long> streamCut) {
        return getHistoryIndex()
                .thenCompose(historyIndex -> getHistoryTable()
                        .thenCompose(historyTable -> getSegmentIndex()
                                .thenCompose(segmentIndex -> getSegmentTable()
                                        .thenCompose(segmentTable -> getSealedSegmentsRecord()
                                                .thenApply(sealedData -> TableHelper.getSizeTillStreamCut(historyIndex.getData(),
                                                        historyTable.getData(), segmentIndex.getData(), segmentTable.getData(), streamCut,
                                                        SealedSegmentsRecord.parse(sealedData.getData())))))));
    }

    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(StreamCutRecord streamCut) {
        return getRetentionSet()
                .thenCompose(data -> {
                    RetentionRecord retention = RetentionRecord.parse(data.getData());
                    if (retention.getStreamCuts().contains(streamCut)) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        RetentionRecord update = RetentionRecord.addStreamCutIfLatest(retention, streamCut);
                        return updateRetentionSet(new Data(update.toByteArray(), data.getVersion()));
                    }
                });
    }

    @Override
    public CompletableFuture<List<StreamCutRecord>> getRetentionStreamCuts() {
        return getRetentionSet()
                .thenApply(data -> RetentionRecord.parse(data.getData()))
                .thenApply(RetentionRecord::getStreamCuts);
    }

    @Override
    public CompletableFuture<Void> deleteStreamCutBefore(StreamCutRecord streamCut) {
        return getRetentionSet()
                .thenCompose(data -> {
                    RetentionRecord retention = RetentionRecord.parse(data.getData());

                    if (!retention.getStreamCuts().contains(streamCut)) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        RetentionRecord update = RetentionRecord.removeStreamCutBefore(retention, streamCut);
                        return updateRetentionSet(
                                new Data(update.toByteArray(), data.getVersion()));
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startCommittingTransactions(final int epoch) {
        return getVersionedCommitTransactionsRecord()
            .thenCompose(versioned -> {
                if (versioned.getObject().equals(CommittingTransactionsRecord.EMPTY)) {
                    return getTxnCommitList(epoch)
                            .thenCompose(list -> {
                                if (list.isEmpty()) {
                                    return CompletableFuture.completedFuture(versioned);
                                } else {
                                    CommittingTransactionsRecord record = CommittingTransactionsRecord.builder()
                                            .epoch(epoch).transactionsToCommit(list).build();
                                    return updateCommittingTxnRecord(new Data(record.toByteArray(), versioned.getVersion()))
                                            .thenApply(version -> new VersionedMetadata<>(record, version));
                                }
                            });
                } else if (epoch != versioned.getObject().getEpoch() ) {
                    // check if the epoch in record matches current epoch. if not throw OperationNotAllowed
                    throw StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED,
                            "Transactions on different epoch are being committed");
                } else {
                    return CompletableFuture.completedFuture(versioned);
                }
            });
    }

    /**
     * Get transactions in epoch. If no transactions exist return null.
     */
    private CompletableFuture<List<UUID>> getTxnCommitList(int epoch) {
        return getTransactionsInEpoch(epoch)
                .thenApply(transactions -> transactions.entrySet().stream()
                        .filter(entry -> entry.getValue().getTxnStatus().equals(TxnStatus.COMMITTING))
                        .map(Map.Entry::getKey).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getVersionedCommitTransactionsRecord() {
        return getCommittingTxnRecord()
                .thenApply(record -> new VersionedMetadata<>(CommittingTransactionsRecord.parse(record.getData()),
                        record.getVersion()));
    }

    @Override
    public CompletableFuture<Void> completeCommittingTransactions(VersionedMetadata<CommittingTransactionsRecord> record) {
        // Chain all transaction commit futures one after the other. This will ensure that order of commit
        // if honoured and is based on the order in the list.
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (UUID txnId : record.getObject().getTransactionsToCommit()) {
            log.debug("Committing transaction {} on stream {}/{}", txnId, scope, name);
            // commit transaction in segment store
            future = future
                    // mark transaction as committed in metadata store.
                    .thenCompose(x -> commitTransaction(txnId)
                            .thenAccept(done -> {
                                log.debug("transaction {} on stream {}/{} committed successfully", txnId, scope, name);
                            }));
        }
        return future
                .thenCompose(x -> Futures.toVoid(updateCommittingTxnRecord(new Data(CommittingTransactionsRecord.EMPTY.toByteArray(),
                        record.getVersion()))));
    }

    private CompletableFuture<Map<UUID, ActiveTxnRecord>> getTransactionsInEpoch(final int epoch) {
        return getTxnInEpoch(epoch)
                .thenApply(x -> x.entrySet()
                        .stream()
                        .collect(toMap(k -> UUID.fromString(k.getKey()),
                                v -> ActiveTxnRecord.parse(v.getValue().getData()))));
    }

    @Override
    public CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName) {
        return createWaitingRequestNodeIfAbsent(processorName.getBytes());
    }

    @Override
    public CompletableFuture<String> getWaitingRequestProcessor() {
        return getWaitingRequestNode()
                .handle((data, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof DataNotFoundException) {
                            return null;
                        } else {
                            throw new CompletionException(e);
                        }
                    } else {
                        return new String(data.getData());
                    }
                });
    }

    @Override
    public CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName) {
        return getWaitingRequestProcessor()
            .thenCompose(waitingRequest -> {
                if (waitingRequest != null && waitingRequest.equals(processorName)) {
                    return deleteWaitingRequestNode();
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            });
    }

    @Override
    public CompletableFuture<Integer> getStartingSegmentNumber() {
        return getSegmentIndex().thenApply(segmentIndex -> TableHelper.getStartingSegmentNumber(segmentIndex.getData()));
    }

    private CompletableFuture<Void> verifyLegalState() {
        return getState(false).thenApply(state -> {
            if (state == null || state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    private CompletableFuture<Void> addSealedSegmentsToRecord(Map<Long, Long> sealedSegments) {
        return getSealedSegmentsRecord()
                .thenCompose(data -> {
                    SealedSegmentsRecord sealedSegmentsRecord = SealedSegmentsRecord.parse(data.getData());
                    Map<Long, Long> map = new HashMap<>();
                    map.putAll(sealedSegments);
                    map.putAll(sealedSegmentsRecord.getSealedSegmentsSizeMap());
                    return updateSealedSegmentsRecord(new Data(
                            new SealedSegmentsRecord(map).toByteArray(), data.getVersion()));
                });
    }

    private List<Long> getNewActiveSegments(final Set<Long> createdSegments, final Set<Long> sealedSegments,
                                            final HistoryRecord lastRecord) {
        final List<Long> segments = lastRecord.getSegments();
        segments.removeAll(sealedSegments);
        segments.addAll(createdSegments);
        return segments;
    }

    private CompletableFuture<Void> addHistoryIndexRecord(final int newEpoch, final int historyOffset) {
        return getHistoryIndex()
                .thenCompose(indexTable -> {
                    final Optional<HistoryIndexRecord> lastRecord = HistoryIndexRecord.readLatestRecord(indexTable.getData());
                    // check idempotent
                    if (lastRecord.isPresent() && lastRecord.get().getEpoch() == newEpoch) {
                        return CompletableFuture.completedFuture(null);
                    }

                    final byte[] updatedTable = TableHelper.updateHistoryIndex(indexTable.getData(), historyOffset);
                    final Data updated = new Data(updatedTable, indexTable.getVersion());
                    return updateHistoryIndex(updated);
                });
    }

    private CompletableFuture<Segment> getSegmentRow(final long number) {
        return getHistoryIndex()
                .thenCompose(historyIndex -> getHistoryTable()
                        .thenCompose(historyTable -> getSegmentIndex()
                                .thenCompose(segmentIndex -> getSegmentTable()
                                        .thenApply(segmentTable -> TableHelper.getSegment(number, segmentIndex.getData(),
                                                segmentTable.getData(), historyIndex.getData(), historyTable.getData())))));
    }

    protected int getTransactionEpoch(UUID txId) {
        // epoch == UUID.msb >> 32
        return TableHelper.getTransactionEpoch(txId);
    }

    abstract CompletableFuture<Void> deleteStream();

    abstract CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime, final int startingSegmentNumber);

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final byte[] configuration);

    abstract CompletableFuture<Void> setConfigurationData(final Data configuration);

    abstract CompletableFuture<Data> getConfigurationData(boolean ignoreCached);

    abstract CompletableFuture<Void> setTruncationData(final Data truncationRecord);

    abstract CompletableFuture<Void> createTruncationDataIfAbsent(final byte[] truncationRecord);

    abstract CompletableFuture<Data> getTruncationData(boolean ignoreCached);

    abstract CompletableFuture<Void> createStateIfAbsent(final byte[] state);

    abstract CompletableFuture<Version> setStateData(final Data state);

    abstract CompletableFuture<Data> getStateData(boolean ignoreCached);

    abstract CompletableFuture<Void> createSegmentIndexIfAbsent(final byte[] data);

    abstract CompletableFuture<Data> getSegmentIndex();

    abstract CompletableFuture<Data> getSegmentIndexFromStore();

    abstract CompletableFuture<Void> updateSegmentIndex(final Data data);

    abstract CompletableFuture<Void> createSegmentTableIfAbsent(final byte[] data);

    abstract CompletableFuture<Data> getSegmentTable();

    abstract CompletableFuture<Data> getSegmentTableFromStore();

    abstract CompletableFuture<Void> updateSegmentTable(final Data data);

    abstract CompletableFuture<Void> createHistoryIndexIfAbsent(final byte[] data);

    abstract CompletableFuture<Data> getHistoryIndex();

    abstract CompletableFuture<Data> getHistoryIndexFromStore();

    abstract CompletableFuture<Void> updateHistoryIndex(final Data updated);

    abstract CompletableFuture<Void> createHistoryTableIfAbsent(final byte[] data);

    abstract CompletableFuture<Void> updateHistoryTable(final Data updated);

    abstract CompletableFuture<Data> getHistoryTable();

    abstract CompletableFuture<Data> getHistoryTableFromStore();

    abstract CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final byte[] record);

    abstract CompletableFuture<Data> getActiveTx(final int epoch, final UUID txId);

    abstract CompletableFuture<Version> updateActiveTx(final int epoch,
                                                    final UUID txId,
                                                    final Data data);

    abstract CompletableFuture<Data> getCompletedTx(final UUID txId);

    abstract CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntryData(final UUID txId, final byte[] data);

    abstract CompletableFuture<Void> createMarkerData(long segmentId, long timestamp);

    abstract CompletableFuture<Void> updateMarkerData(long segmentId, Data data);

    abstract CompletableFuture<Void> removeMarkerData(long segmentId);

    abstract CompletableFuture<Data> getMarkerData(long segmentId);

    abstract CompletableFuture<Map<String, Data>> getCurrentTxns();

    abstract CompletableFuture<Map<String, Data>> getTxnInEpoch(int epoch);

    abstract CompletableFuture<Void> checkScopeExists() throws StoreException;

    abstract CompletableFuture<Void> createSealedSegmentsRecord(byte[] sealedSegmentsRecord);

    abstract CompletableFuture<Data> getSealedSegmentsRecord();

    abstract CompletableFuture<Void> updateSealedSegmentsRecord(Data update);

    abstract CompletableFuture<Void> createRetentionSet(byte[] retention);

    abstract CompletableFuture<Data> getRetentionSet();

    abstract CompletableFuture<Void> updateRetentionSet(Data retention);

    abstract CompletableFuture<Void> createEpochTransitionDataIfAbsent(byte[] epochTransition);

    abstract CompletableFuture<Version> updateEpochTransitionNode(Data epochTransition);

    abstract CompletableFuture<Data> getEpochTransitionNode();

    abstract CompletableFuture<Void> createCommittingTxnRecord(byte[] committingTxns);

    abstract CompletableFuture<Data> getCommittingTxnRecord();

    abstract CompletableFuture<Version> updateCommittingTxnRecord(Data committingTxnRecord);

    abstract CompletableFuture<Void> createWaitingRequestNodeIfAbsent(byte[] data);

    abstract CompletableFuture<Data> getWaitingRequestNode();

    abstract CompletableFuture<Void> deleteWaitingRequestNode();
}
