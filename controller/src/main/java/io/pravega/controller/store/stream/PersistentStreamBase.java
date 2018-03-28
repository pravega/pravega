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
import com.google.common.collect.Lists;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.store.stream.StoreException.DataNotFoundException;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CompletedTxnRecord;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.IndexRecord;
import io.pravega.controller.store.stream.tables.RetentionRecord;
import io.pravega.controller.store.stream.tables.SealedSegmentsRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

@Slf4j
public abstract class PersistentStreamBase<T> implements Stream {

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
                        .thenCompose((Void v) -> createConfigurationIfAbsent(StreamProperty.complete(createStreamResponse.getConfiguration())))
                        .thenCompose((Void v) -> createTruncationDataIfAbsent(StreamProperty.complete(StreamTruncationRecord.EMPTY)))
                        .thenCompose((Void v) -> createStateIfAbsent(State.CREATING))
                        .thenCompose((Void v) -> createNewSegmentTable(createStreamResponse.getConfiguration(), createStreamResponse.getTimestamp()))
                        .thenCompose((Void v) -> getState(true))
                        .thenCompose(state -> {
                            if (state.equals(State.CREATING)) {
                                return createNewEpoch(0);
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        })
                        .thenCompose((Void v) -> createIndexTableIfAbsent(new Data<>(
                                TableHelper.createIndexTable(createStreamResponse.getTimestamp()), null)))
                        .thenCompose((Void v) -> {
                            final int numSegments = createStreamResponse.getConfiguration().getScalingPolicy().getMinNumSegments();
                            final byte[] historyTable = TableHelper.createHistoryTable(createStreamResponse.getTimestamp(),
                                    IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

                            return createHistoryTableIfAbsent(new Data<>(historyTable, null));
                        })
                        .thenCompose((Void v) -> createSealedSegmentsRecord(
                                SerializationUtils.serialize(new SealedSegmentsRecord(Collections.emptyMap()))))
                        .thenCompose((Void v) -> createRetentionSet(SerializationUtils.serialize(new RetentionRecord(Collections.emptyList()))))
                        .thenApply((Void v) -> createStreamResponse));
    }

    private CompletableFuture<Void> createNewSegmentTable(final StreamConfiguration configuration, long timestamp) {
        final int numSegments = configuration.getScalingPolicy().getMinNumSegments();
        final double keyRangeChunk = 1.0 / numSegments;

        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        final byte[] segmentTable = TableHelper.createSegmentTable(newRanges, timestamp);

        return createSegmentTableIfAbsent(new Data<>(segmentTable, null));
    }

    @Override
    public CompletableFuture<Void> delete() {
        return deleteStream();
    }

    @Override
    public CompletableFuture<Void> startTruncation(final Map<Integer, Long> streamCut) {
        return Futures.allOfWithResults(streamCut.keySet().stream().map(x -> getSegment(x).thenApply(segment ->
                new SimpleEntry<>(segment.keyStart, segment.keyEnd)))
                .collect(Collectors.toList()))
                .thenAccept(TableHelper::validateStreamCut)
                .thenCompose(valid -> getTruncationData(true)
                .thenCompose(truncationData -> {
                            Preconditions.checkNotNull(truncationData);
                            StreamProperty<StreamTruncationRecord> previous = SerializationUtils.deserialize(truncationData.getData());
                            Exceptions.checkArgument(!previous.isUpdating(), "TruncationRecord", "Truncation record conflict");

                            return computeTruncationRecord(previous.getProperty(), streamCut)
                                    .thenApply(StreamProperty::update)
                                    .thenCompose(prop -> setTruncationData(
                                            new Data<>(SerializationUtils.serialize(prop), truncationData.getVersion())));
                        }));
    }

    private CompletableFuture<StreamTruncationRecord> computeTruncationRecord(StreamTruncationRecord truncationRecord,
                                                                              Map<Integer, Long> streamCut) {
        log.debug("computing truncation for stream {}/{}", scope, name);
        return getHistoryTableFromStore()
                .thenCompose(history -> getSegmentTableFromStore()
                        .thenCompose(segment -> getIndexTable()
                                .thenApply(index -> TableHelper.computeTruncationRecord(index.getData(), history.getData(),
                                        segment.getData(), streamCut, truncationRecord))));
    }

    @Override
    public CompletableFuture<Void> completeTruncation() {
        return getTruncationData(true)
                .thenCompose(truncationData -> {
                    Preconditions.checkNotNull(truncationData);
                    StreamProperty<StreamTruncationRecord> current = SerializationUtils.deserialize(truncationData.getData());
                    if (current.isUpdating()) {
                        StreamTruncationRecord truncationRecord = current.getProperty();
                        StreamProperty<StreamTruncationRecord> completedProp = StreamProperty.complete(truncationRecord.mergeDeleted());

                        return setTruncationData(new Data<>(SerializationUtils.serialize(completedProp), truncationData.getVersion()));
                    } else {
                        // idempotent
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<StreamTruncationRecord> getTruncationRecord() {
        return getTruncationProperty(false)
                .thenApply(prop -> prop == null ? StreamTruncationRecord.EMPTY : prop.getProperty());
    }

    @Override
    public CompletableFuture<StreamProperty<StreamTruncationRecord>> getTruncationProperty(boolean ignoreCached) {
        return getTruncationData(ignoreCached)
                .thenApply(data -> SerializationUtils.deserialize(data.getData()));
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
                    StreamProperty<StreamConfiguration> previous = SerializationUtils.deserialize(configData.getData());
                    Preconditions.checkNotNull(previous);
                    Preconditions.checkArgument(!previous.isUpdating());
                    StreamProperty<StreamConfiguration> update = StreamProperty.update(newConfiguration);
                    return setConfigurationData(new Data<>(SerializationUtils.serialize(update), configData.getVersion()));
                });
    }

    /**
     * Update configuration at configurationPath.
     *
     * @return future of operation
     */
    @Override
    public CompletableFuture<Void> completeUpdateConfiguration() {
        return getConfigurationData(true)
                .thenCompose(configData -> {
                    StreamProperty<StreamConfiguration> current = SerializationUtils.deserialize(configData.getData());
                    Preconditions.checkNotNull(current);
                    if (current.isUpdating()) {
                        StreamProperty<StreamConfiguration> newProperty = StreamProperty.complete(current.getProperty());
                        log.debug("Completing update configuration for stream {}/{}", scope, name);
                        return setConfigurationData(new Data<>(SerializationUtils.serialize(newProperty), configData.getVersion()));
                    } else {
                        // idempotent
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    /**
     * Fetch configuration at configurationPath.
     *
     * @return Future of stream configuration
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getConfigurationProperty(false).thenApply(StreamProperty::getProperty);
    }

    @Override
    public CompletableFuture<StreamProperty<StreamConfiguration>> getConfigurationProperty(boolean ignoreCached) {
        return getConfigurationData(ignoreCached)
                .thenApply(data -> SerializationUtils.deserialize(data.getData()));
    }

    @Override
    public CompletableFuture<Boolean> updateState(final State state) {
        return getStateData(true)
                .thenCompose(currState -> {
                    if (State.isTransitionAllowed(SerializationUtils.deserialize(currState.getData()), state)) {
                        return setStateData(new Data<>(SerializationUtils.serialize(state), currState.getVersion()))
                                .thenApply(x -> true);
                    } else {
                        return Futures.failedFuture(StoreException.create(
                                StoreException.Type.OPERATION_NOT_ALLOWED,
                                "Stream: " + getName() + " State: " + state.name()));
                    }
                });
    }

    @Override
    public CompletableFuture<State> getState(boolean ignoreCached) {
        return getStateData(ignoreCached)
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
                        Futures.allOfWithResults(listOfScaleRecords.stream().map(record -> {
                            long scaleTs = record.getLeft();
                            CompletableFuture<List<Segment>> list = Futures.allOfWithResults(
                                    record.getRight().stream().map(this::getSegment)
                                            .collect(Collectors.toList()));

                            return list.thenApply(segments -> new ImmutablePair<>(scaleTs, segments));
                        }).collect(Collectors.toList())))
                .thenApply(this::mapToScaleMetadata);
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
        return verifyLegalState().thenCompose(v -> Futures.allOfWithResults(candidates.stream()
                                                                                      .map(this::getSegment)
                                                                                      .collect(Collectors.toList())))
                                 .thenApply(successorCandidates -> successorCandidates.stream()
                                                                                      .filter(x -> x.overlaps(segment))
                                                                                      .collect(Collectors.toList()));
    }

    private CompletableFuture<List<Segment>> getSuccessorsForSegment(final int number) {
        return getIndexTable()
                .thenCompose(historyIndex -> getHistoryTable()
                        .thenCompose(historyTable -> getSegment(number)
                                .thenCompose(segment -> {
                                    List<Integer> candidates = TableHelper.findSegmentSuccessorCandidates(segment,
                                            historyIndex.getData(),
                                            historyTable.getData());
                                    return findOverlapping(segment, candidates);
                                })));
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
                                    return Futures.allOfWithResults(resultFutures);
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
        return getTruncationRecord()
                .thenCompose(truncationRecord ->
                        getHistoryTable().thenCompose(historyTable ->
                                getIndexTable().thenCompose(indexTable ->
                                        getSegmentTable().thenApply(segmentTable ->
                                                TableHelper.getActiveSegments(timestamp,
                                                        indexTable.getData(),
                                                        historyTable.getData(),
                                                        segmentTable.getData(),
                                                        truncationRecord
                                                )))));
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
    public CompletableFuture<StartScaleResponse> startScale(final List<Integer> segmentsToSeal,
                                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                            final long scaleTimestamp,
                                                            boolean runOnlyIfStarted) {
        return getHistoryTableFromStore()
                .thenCompose(historyTable -> getSegmentTableFromStore()
                        .thenCompose(segmentTable -> {
                            if (!TableHelper.isScaleInputValid(segmentsToSeal, newRanges, segmentTable.getData())) {
                                log.error("scale input invalid {} {}", segmentsToSeal, newRanges);
                                throw new ScaleOperationExceptions.ScaleInputInvalidException();
                            }

                            return startScale(segmentsToSeal, newRanges, scaleTimestamp, runOnlyIfStarted, historyTable,
                                    segmentTable);
                        })
                        .thenApply(epochTransition -> {
                            List<Segment> newSegments = new ArrayList<>();
                            epochTransition.getNewSegmentsWithRange().entrySet().forEach(x -> {
                                newSegments.add(new Segment(x.getKey(), epochTransition.getActiveEpoch(), scaleTimestamp,
                                        x.getValue().getKey(), x.getValue().getValue()));
                            });
                            return new StartScaleResponse(epochTransition.getActiveEpoch(), newSegments);
                        }));
    }

    private CompletableFuture<EpochTransitionRecord> startScale(List<Integer> segmentsToSeal,
                                                                List<SimpleEntry<Double, Double>> newRanges,
                                                                long scaleTimestamp, boolean runOnlyIfStarted,
                                                                Data<T> historyTable, Data<T> segmentTable) {
        return getEpochTransition()
                .thenCompose(record -> {
                    if (record != null) {
                        // verify that its the same as the supplied input (--> segments to be sealed
                        // and new ranges are identical). else throw scale conflict exception
                        if (!(newRanges.stream().allMatch(x ->
                                record.getNewSegmentsWithRange().values().stream()
                                        .anyMatch(y -> y.getKey().equals(x.getKey())
                                                && y.getValue().equals(x.getValue()))) &&
                                record.getSegmentsToSeal().stream().allMatch(segmentsToSeal::contains))) {
                            log.debug("scale conflict, another scale operation is ongoing");
                            throw new ScaleOperationExceptions.ScaleConflictException();
                        }
                        return CompletableFuture.completedFuture(record);
                    } else {
                        // if state is SCALING and epoch transition record does not exist, reset the state back to ACTIVE
                        return resetStateConditionally(State.SCALING)
                                .thenCompose(v -> {
                                    if (runOnlyIfStarted) {
                                        log.info("scale not started, retry later.");
                                        throw new TaskExceptions.StartException("Scale not started yet.");
                                    }

                                    // check input is valid and satisfies preconditions
                                    if (!TableHelper.canScaleFor(segmentsToSeal, historyTable.getData())) {
                                        // invalid input, log and ignore
                                        log.warn("scale precondition failed {}", segmentsToSeal);
                                        throw new ScaleOperationExceptions.ScalePreConditionFailureException();
                                    }

                                    EpochTransitionRecord epochTransition = TableHelper.computeEpochTransition(
                                            historyTable.getData(), segmentTable.getData(), segmentsToSeal, newRanges, scaleTimestamp);

                                    return createEpochTransitionNode(epochTransition.toByteArray())
                                            .handle((r, e) -> {
                                                if (Exceptions.unwrap(e) instanceof StoreException.DataExistsException) {
                                                    log.debug("scale conflict, another scale operation is ongoing");
                                                    throw new ScaleOperationExceptions.ScaleConflictException();
                                                }

                                                log.info("scale for stream {}/{} accepted. Segments to seal = {}", scope, name,
                                                        epochTransition.getSegmentsToSeal());
                                                return epochTransition;
                                            });
                                });
                    }
                });
    }

    @Override
    public CompletableFuture<Void> scaleCreateNewSegments() {
        // Called after start scale to indicate store to create new segments in the segment table. This method takes care of
        // checking for idempotent addition of segments to the table.
        return getState(true)
                .thenCompose(state -> {
                    checkState(state, State.SCALING);
                    return getHistoryTableFromStore()
                            .thenCompose(historyTable -> getSegmentTableFromStore()
                                    .thenCompose(segmentTable -> getEpochTransition().thenCompose(epochTransition -> {
                                        if (epochTransition == null) {
                                            return Futures.toVoid(updateState(State.ACTIVE));
                                        }
                                        // update segment table in idempotent fashion.
                                        int newEpoch = epochTransition.getNewEpoch();

                                        final int segmentCount = TableHelper.getSegmentCount(segmentTable.getData());
                                        final Segment latestSegment = TableHelper.getSegment(segmentCount - 1,
                                                segmentTable.getData());
                                        if (latestSegment.getEpoch() < newEpoch) {
                                            assert latestSegment.getEpoch() == epochTransition.getActiveEpoch();

                                            log.info("Scale {}/{} for segments started. Creating new segments. SegmentsToSeal {}",
                                                    scope, name, epochTransition.getSegmentsToSeal());

                                            return createNewSegments(
                                                    Lists.newArrayList(epochTransition.getNewSegmentsWithRange().values()),
                                                    historyTable.getData(), segmentTable,
                                                    segmentCount, newEpoch, epochTransition.getTime());
                                        } else {
                                            return isEpochTransitionConsistent(historyTable, segmentTable, epochTransition,
                                                    latestSegment);
                                        }
                                    })));
                });
    }

    private CompletableFuture<Void> isEpochTransitionConsistent(Data<T> historyTable, Data<T> segmentTable,
                                                              EpochTransitionRecord epochTransition, Segment latestSegment) {
        // verify that epoch transition is consistent with segments in the table.
        if (TableHelper.isEpochTransitionConsistent(epochTransition, historyTable.getData(), segmentTable.getData())) {
            log.debug("CreateNewSegments step for stream {}/{} is idempotent, " +
                    "segments are already present in segment table.", scope, name);
            return CompletableFuture.completedFuture(null);
        } else {
            return deleteEpochTransitionNode()
                    .thenCompose(v -> resetStateConditionally(State.SCALING))
                    .thenAccept(v -> {
                        log.warn("Scale epoch transition record is inconsistent with data in the table. ",
                                latestSegment.getEpoch(), epochTransition.getNewEpoch());
                        throw new IllegalArgumentException("Epoch transition record is inconsistent.");
                    });
        }

    }

    private CompletableFuture<Void> createNewSegments(final List<SimpleEntry<Double, Double>> newRanges,
                                                      final byte[] historyTable,
                                                      final Data<T> segmentTable,
                                                      final int nextSegmentNumber,
                                                      final int newEpoch,
                                                      final long scaleStartTime) {
        // Ensure that segment.creation time is monotonically increasing after each new scale.
        // because scale time could be supplied by a controller with a skewed clock, we should:
        // take max(scaleTime, lastScaleTime + 1, System.currentTimeMillis)
        long lastScaleTime = HistoryRecord.readLatestRecord(historyTable, true)
                .map(HistoryRecord::getScaleTime).orElse(0L);
        long scaleEventTime = Math.max(System.currentTimeMillis(), scaleStartTime);
        long segmentCreationTimestamp = Math.max(scaleEventTime, lastScaleTime + 1);

        final byte[] updated = TableHelper.updateSegmentTable(nextSegmentNumber, newEpoch,
                segmentTable.getData(), newRanges, segmentCreationTimestamp);

        final Data<T> updatedTable = new Data<>(updated, segmentTable.getVersion());

        return updateSegmentTable(updatedTable)
                .thenAccept(v -> log.info("scale for stream {}/{} epoch {}, new segments created successfully", scope, name,
                        newEpoch));
    }

    private CompletableFuture<EpochTransitionRecord> getEpochTransition() {
        return getEpochTransitionNode()
                .handle((r, e) -> {
                    if (e != null) {
                        Throwable ex = Exceptions.unwrap(e);
                        if (ex instanceof StoreException.DataNotFoundException) {
                            return null;
                        } else {
                            throw new CompletionException(ex);
                        }
                    } else {
                        return EpochTransitionRecord.parse(r.getData());
                    }
                });
    }

    /**
     * Segments created with pravega, update the history table with this fact so they are available as successors
     * 3. Add entry into the history table.
     *
     * @return Future which when complete will have the history record updated in store.
     */
    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated() {
        return getState(true)
                .thenCompose(state -> {
                    checkState(state, State.SCALING);
                    return getEpochTransition()
                            .thenCompose(epochTransition -> {
                                if (epochTransition == null) {
                                    log.debug("epochTransition for stream {}/{} is already removed, " +
                                            "which means ongoing scaling is finished.", scope, name);
                                    return Futures.toVoid(updateState(State.ACTIVE));
                                }

                                return addPartialHistoryRecordAndIndex(epochTransition.getSegmentsToSeal(),
                                        epochTransition.getNewSegmentsWithRange().keySet(), epochTransition.getActiveEpoch(),
                                        epochTransition.getNewEpoch());
                            });
                });
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
                .thenCompose(historyTable -> {
                    CompletableFuture<Boolean> result = new CompletableFuture<>();

                    if (TableHelper.isNewEpochCreated(historyTable.getData())) {
                        deleteEpochNode(epoch)
                                .whenComplete((r, e) -> {
                                    if (e != null) {
                                        Throwable ex = Exceptions.unwrap(e);
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

    private CompletableFuture<Void> clearMarkers(final Set<Integer> segments) {
        return Futures.toVoid(Futures.allOfWithResults(segments.stream().parallel()
                .map(this::removeColdMarker).collect(Collectors.toList())));
    }

    /**
     * Remainder of scale metadata update. Also set the state back to active.
     * 4. complete entry into the history table.
     * 5. Add entry into the index table.
     *
     * @param sealedSegmentSizes sealed segments with sizes
     * @return : list of newly created segments
     */
    @Override
    public CompletableFuture<Void> scaleOldSegmentsSealed(Map<Integer, Long> sealedSegmentSizes) {
        // If epochTransitionRecord does not exist, and state is scaling, set the state back to active
        // get sealed segments list
        // get scale timestamp, sealed segments, active epoch, new segments
        return getState(true)
                .thenCompose(state -> {
                    checkState(state, State.SCALING);
                    return getEpochTransition()
                            .thenCompose(epochTransition -> {
                                return Futures.toVoid(clearMarkers(epochTransition.getNewSegmentsWithRange().keySet())
                                        .thenCompose(x -> completeScale(sealedSegmentSizes, epochTransition.getActiveEpoch(),
                                                Lists.newArrayList(epochTransition.getNewSegmentsWithRange().keySet()),
                                                epochTransition.getTime())));

                            });
                });
    }

    /**
      * Reset state of stream to ACTIVE if it matches the supplied state.
      * @param state stream state to match
      * @return Future which when completes will have reset the state or failed with appropriate exception.
      */
    @Override
    public CompletableFuture<Void> resetStateConditionally(State state) {
        return Futures.toVoid(getState(true)
                        .thenCompose(currState -> {
                        if (currState.equals(state)) {
                                return updateState(State.ACTIVE);
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                    }));
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
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
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

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(Map<Integer, Long> streamCut) {
        return getSegmentTable()
                .thenCompose(segmentTable -> getHistoryTable()
                        .thenCompose(historyTable -> getIndexTable()
                                .thenCompose(indexTable -> getSealedSegmentsRecord()
                                        .thenApply(sealedData -> TableHelper.getSizeTillStreamCut(indexTable.getData(),
                                                historyTable.getData(), segmentTable.getData(), streamCut,
                                                SerializationUtils.deserialize(sealedData.getData()))))));
    }

    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(StreamCutRecord streamCut) {
        return getRetentionSet()
                .thenCompose(data -> {
                    RetentionRecord retention = SerializationUtils.deserialize(data.getData());
                    if (retention.getStreamCuts().contains(streamCut)) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        RetentionRecord update = RetentionRecord.addStreamCutIfLatest(retention, streamCut);
                        return updateRetentionSet(new Data<>(SerializationUtils.serialize(update), data.getVersion()));
                    }
                });
    }

    public CompletableFuture<List<StreamCutRecord>> getRetentionStreamCuts() {
        return getRetentionSet()
                .thenApply(data -> (RetentionRecord) SerializationUtils.deserialize(data.getData()))
                .thenApply(RetentionRecord::getStreamCuts);
    }

    @Override
    public CompletableFuture<Void> deleteStreamCutBefore(StreamCutRecord streamCut) {
        return getRetentionSet()
                .thenCompose(data -> {
                    RetentionRecord retention = SerializationUtils.deserialize(data.getData());

                    if (!retention.getStreamCuts().contains(streamCut)) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        RetentionRecord update = RetentionRecord.removeStreamCutBefore(retention, streamCut);
                        return updateRetentionSet(
                                new Data<>(SerializationUtils.serialize(update), data.getVersion()));
                    }
                });
    }

    private void checkState(State currState, State expectedState) {
        if (currState != expectedState) {
            throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                    "Stream: " + getName() + " Current State: " + currState.name() + " Expected State:"
                            + expectedState.name());
        }
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

    private CompletableFuture<List<Segment>> getSegments(final List<Integer> segments) {
        return Futures.allOfWithResults(segments.stream().map(this::getSegment)
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
    private CompletableFuture<Void> addPartialHistoryRecordAndIndex(final Set<Integer> segmentsToSeal,
                                                                    final Set<Integer> createdSegments,
                                                                    final int activeEpoch,
                                                                    final int newEpoch) {
        return getHistoryTableFromStore()
                        .thenCompose(historyTable -> {
                            final HistoryRecord lastRecord = HistoryRecord.readLatestRecord(historyTable.getData(),
                                    false).get();

                            // idempotent check
                            if (lastRecord.getEpoch() > activeEpoch) {
                                boolean idempotent = lastRecord.isPartial() &&
                                        lastRecord.getSegments().containsAll(createdSegments) && lastRecord.getEpoch() == newEpoch;
                                if (idempotent) {
                                    HistoryRecord previous = HistoryRecord.fetchPrevious(lastRecord, historyTable.getData()).get();

                                    idempotent = previous.getSegments().stream().noneMatch(createdSegments::contains);
                                }

                                if (idempotent) {
                                    log.debug("{}/{} scale op for epoch {} - history record already added", scope, name, activeEpoch);
                                    return CompletableFuture.completedFuture(null);
                                } else {
                                    log.warn("{}/{} scale op for epoch {}. Scale already completed.", scope, name, activeEpoch);
                                    throw new ScaleOperationExceptions.ScaleConditionInvalidException();
                                }
                            }

                            final List<Integer> newActiveSegments = getNewActiveSegments(createdSegments, segmentsToSeal, lastRecord);
                            final int offset = historyTable.getData().length;
                            // now we know the offset at which we want to add.
                            final byte[] updatedTable = TableHelper.addPartialRecordToHistoryTable(historyTable.getData(), newActiveSegments);
                            final Data<T> updated = new Data<>(updatedTable, historyTable.getVersion());

                            return createNewEpoch(newEpoch)
                                    .thenCompose(v -> getSegmentTable()
                                            .thenApply(segmentTable -> {
                                                final int segmentCount = TableHelper.getSegmentCount(segmentTable.getData());
                                                final Segment latestSegment = TableHelper.getSegment(segmentCount - 1,
                                                        segmentTable.getData());
                                                return latestSegment.getStart();
                                            }))
                                    .thenCompose(start -> addIndexRecord(newEpoch, start, offset))
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
                        });
    }

    private CompletableFuture<Void> completeScale(final Map<Integer, Long> sealedSegments, final int activeEpoch,
                                                  final List<Integer> newSegments, long scaleTimestamp) {
        return getHistoryTableFromStore()
                        .thenCompose(historyTable -> {
                            final Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable.getData(),
                                    false);

                            assert lastRecordOpt.isPresent();

                            final HistoryRecord lastRecord = lastRecordOpt.get();

                            // idempotent check
                            if (!lastRecord.isPartial()) {
                                if (lastRecord.getSegments().stream().noneMatch(sealedSegments::containsKey) &&
                                        newSegments.stream().allMatch(x -> lastRecord.getSegments().contains(x))) {
                                    log.debug("{}/{} scale already completed for epoch {}.", scope, name, activeEpoch);

                                    return CompletableFuture.completedFuture(null);
                                } else {
                                    log.debug("{}/{} scale complete attempt invalid for epoch {}.", scope, name, activeEpoch);

                                    throw new ScaleOperationExceptions.ScaleConditionInvalidException();
                                }
                            }

                            assert lastRecord.isPartial();

                            long scaleEventTime = Math.max(System.currentTimeMillis(), scaleTimestamp);
                            final Optional<HistoryRecord> previousOpt = HistoryRecord.fetchPrevious(lastRecord, historyTable.getData());
                            if (previousOpt.isPresent()) {
                                // To ensure that we always have ascending time in history records irrespective of controller
                                // clock mismatches.
                                scaleEventTime = Math.max(scaleEventTime, previousOpt.get().getScaleTime() + 1);

                                if (previousOpt.get().getEpoch() > activeEpoch) {
                                    throw new ScaleOperationExceptions.ScaleConditionInvalidException();
                                }
                            }

                            byte[] updatedTable = TableHelper.completePartialRecordInHistoryTable(historyTable.getData(),
                                    lastRecord, scaleEventTime);
                            final Data<T> updated = new Data<>(updatedTable, historyTable.getVersion());

                            return addSealedSegmentsToRecord(sealedSegments)
                                    .thenCompose(x -> updateHistoryTable(updated))
                                    .thenCompose(x -> deleteEpochTransitionNode())
                                    .thenCompose(x -> Futures.toVoid(updateState(State.ACTIVE)))
                                    .whenComplete((r, e) -> {
                                        if (e != null) {
                                            log.warn("{}/{} attempt to complete scale for epoch {}. {}", scope, name, activeEpoch,
                                                    e.getClass().getName());
                                        } else {
                                            log.debug("{}/{} scale complete, index and history tables updated for epoch {}.",
                                                    scope, name, activeEpoch);
                                        }
                                    });
                        });
    }

    private CompletableFuture<Void> addSealedSegmentsToRecord(Map<Integer, Long> sealedSegments) {
        return getSealedSegmentsRecord()
                .thenCompose(data -> {
                    SealedSegmentsRecord sealedSegmentsRecord = SerializationUtils.deserialize(data.getData());
                    Map<Integer, Long> map = new HashMap<>();
                    map.putAll(sealedSegments);
                    map.putAll(sealedSegmentsRecord.getSealedSegmentsSizeMap());
                    return updateSealedSegmentsRecord(new Data<>(
                            SerializationUtils.serialize(new SealedSegmentsRecord(map)), data.getVersion()));
                });
    }

    private List<Integer> getNewActiveSegments(final Set<Integer> createdSegments,
                                               final Set<Integer> sealedSegments,
                                               final HistoryRecord lastRecord) {
        final List<Integer> segments = lastRecord.getSegments();
        segments.removeAll(sealedSegments);
        segments.addAll(createdSegments);
        return segments;
    }

    private CompletableFuture<Void> addIndexRecord(int newEpoch, Long scaleTime, int offset) {
        return getIndexTable()
                .thenCompose(indexTable -> {
                    final Optional<IndexRecord> lastRecord = IndexRecord.readLatestRecord(indexTable.getData());
                    // check idempotent
                    if (lastRecord.isPresent() && lastRecord.get().getEpoch() == newEpoch) {
                        assert lastRecord.get().getHistoryOffset() == offset;
                        return CompletableFuture.completedFuture(null);
                    }

                    final byte[] updatedTable = TableHelper.updateIndexTable(indexTable.getData(), scaleTime, offset);
                    final Data<T> updated = new Data<>(updatedTable, indexTable.getVersion());
                    return updateIndexTable(updated);
                });
    }

    abstract CompletableFuture<Void> deleteStream();

    abstract CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration, final long creationTime);

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final StreamProperty<StreamConfiguration> configuration);

    abstract CompletableFuture<Void> setConfigurationData(final Data<T> configuration);

    abstract CompletableFuture<Data<T>> getConfigurationData(boolean ignoreCached);

    abstract CompletableFuture<Void> setTruncationData(final Data<T> truncationRecord);

    abstract CompletableFuture<Void> createTruncationDataIfAbsent(final StreamProperty<StreamTruncationRecord> truncationRecord);

    abstract CompletableFuture<Data<T>> getTruncationData(boolean ignoreCached);

    abstract CompletableFuture<Void> createStateIfAbsent(final State state);

    abstract CompletableFuture<Void> setStateData(final Data<T> state);

    abstract CompletableFuture<Data<T>> getStateData(boolean ignoreCached);

    abstract CompletableFuture<Void> createSegmentTableIfAbsent(final Data<T> data);

    abstract CompletableFuture<Segment> getSegmentRow(final int number);

    abstract CompletableFuture<Data<T>> getSegmentTable();

    abstract CompletableFuture<Data<T>> getSegmentTableFromStore();

    abstract CompletableFuture<Void> updateSegmentTable(final Data<T> data);

    abstract CompletableFuture<Void> createIndexTableIfAbsent(final Data<T> data);

    abstract CompletableFuture<Data<T>> getIndexTable();

    abstract CompletableFuture<Data<T>> getIndexTableFromStore();

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

    abstract CompletableFuture<Void> createSealedSegmentsRecord(byte[] sealedSegmentsRecord);

    abstract CompletableFuture<Data<T>> getSealedSegmentsRecord();

    abstract CompletableFuture<Void> updateSealedSegmentsRecord(Data<T> update);

    abstract CompletableFuture<Void> createRetentionSet(byte[] retention);

    abstract CompletableFuture<Data<T>> getRetentionSet();

    abstract CompletableFuture<Void> updateRetentionSet(Data<T> retention);

    abstract CompletableFuture<Void> createEpochTransitionNode(byte[] epochTransition);

    abstract CompletableFuture<Data<T>> getEpochTransitionNode();

    abstract CompletableFuture<Void> deleteEpochTransitionNode();
}
