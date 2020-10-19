/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CollectionHelpers;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.store.stream.StoreException.DataNotFoundException;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.shared.NameUtils;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.shared.NameUtils.getSegmentNumber;
import static java.util.stream.Collectors.groupingBy;

@Slf4j
public abstract class PersistentStreamBase implements Stream {
    private final String scope;
    private final String name;
    private final AtomicInteger historyChunkSize;
    private final AtomicInteger shardSize;

    PersistentStreamBase(final String scope, final String name, int historyChunkSize, int shardSize) {
        this.scope = scope;
        this.name = name;
        this.historyChunkSize = new AtomicInteger(historyChunkSize);
        this.shardSize = new AtomicInteger(shardSize);
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

    @Override
    public CompletableFuture<CreateStreamResponse> create(final StreamConfiguration configuration, long createTimestamp, int startingSegmentNumber) {
        return checkStreamExists(configuration, createTimestamp, startingSegmentNumber)
                .thenCompose(createStreamResponse -> createStreamMetadata()
                        .thenCompose((Void v) -> storeCreationTimeIfAbsent(createStreamResponse.getTimestamp()))
                        .thenCompose((Void v) -> createConfigurationIfAbsent(StreamConfigurationRecord.complete(
                                scope, name, createStreamResponse.getConfiguration())))
                        .thenCompose((Void v) -> createEpochTransitionIfAbsent(EpochTransitionRecord.EMPTY))
                        .thenCompose((Void v) -> createTruncationDataIfAbsent(StreamTruncationRecord.EMPTY))
                        .thenCompose((Void v) -> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord.EMPTY))
                        .thenCompose((Void v) -> createStateIfAbsent(StateRecord.builder().state(State.CREATING).build()))
                        .thenCompose((Void v) -> createHistoryRecords(startingSegmentNumber, createStreamResponse))
                        .thenCompose((Void v) -> createSubscribersRecordIfAbsent())
                        .thenApply((Void v) -> createStreamResponse));
    }

    private CompletionStage<Void> createHistoryRecords(int startingSegmentNumber, CreateStreamResponse createStreamResponse) {
        final int numSegments = createStreamResponse.getConfiguration().getScalingPolicy().getMinNumSegments();
        // create epoch 0 record
        final double keyRangeChunk = 1.0 / numSegments;

        long creationTime = createStreamResponse.getTimestamp();
        final ImmutableList.Builder<StreamSegmentRecord> builder = ImmutableList.builder();
        
        IntStream.range(0, numSegments).boxed()
                 .forEach(x -> builder.add(newSegmentRecord(0, startingSegmentNumber + x, creationTime,
                                                                    x * keyRangeChunk, (x + 1) * keyRangeChunk)));

        EpochRecord epoch0 = new EpochRecord(0, 0, builder.build(), creationTime);

        return createEpochRecord(epoch0)
                .thenCompose(r -> createHistoryChunk(epoch0))
                .thenCompose(r -> createSealedSegmentSizeMapShardIfAbsent(0))
                .thenCompose(r -> createRetentionSetDataIfAbsent(new RetentionSet(ImmutableList.of())))
                .thenCompose(r -> createCurrentEpochRecordDataIfAbsent(epoch0));
    }

    private CompletionStage<Void> createHistoryChunk(EpochRecord epoch0) {
        HistoryTimeSeriesRecord record = new HistoryTimeSeriesRecord(0, 0, 
                ImmutableList.of(), epoch0.getSegments(), epoch0.getCreationTime());
        return createHistoryTimeSeriesChunk(0, record);
    }

    private CompletableFuture<Void> createHistoryTimeSeriesChunk(int chunkNumber, HistoryTimeSeriesRecord epoch) {
        ImmutableList.Builder<HistoryTimeSeriesRecord> builder = ImmutableList.builder();
        HistoryTimeSeries timeSeries = new HistoryTimeSeries(builder.add(epoch).build());
        return createHistoryTimeSeriesChunkDataIfAbsent(chunkNumber, timeSeries);
    }

    @Override
    public CompletableFuture<Void> delete() {
        return deleteStream();
    }

    @Override
    public CompletableFuture<Void> startTruncation(final Map<Long, Long> streamCut) {
        return getTruncationRecord()
                .thenCompose(existing -> {
                    Preconditions.checkNotNull(existing);
                    Preconditions.checkArgument(!existing.getObject().isUpdating());
                    return isStreamCutValid(streamCut)
                        .thenCompose(isValid -> {
                            Exceptions.checkArgument(isValid, "streamCut", "invalid stream cut");
                            return computeStreamCutSpan(streamCut)
                                    .thenCompose(span -> {
                                        StreamTruncationRecord previous = existing.getObject();
                                        // check greater than
                                        Exceptions.checkArgument(greaterThan(streamCut, span, previous.getStreamCut(), previous.getSpan()),
                                                "StreamCut", "Supplied streamcut is behind previous truncation point");

                                        return computeTruncationRecord(previous, streamCut, span)
                                                .thenCompose(prop ->
                                                        Futures.toVoid(setTruncationData(new VersionedMetadata<>(prop, existing.getVersion()))));
                                    });
                        });
                });
    }

    private boolean greaterThan(Map<Long, Long> cut1, Map<StreamSegmentRecord, Integer> span1,
                                Map<Long, Long> cut2, Map<StreamSegmentRecord, Integer> span2) {
        // find overlapping segments in map2 for all segments in span1 compare epochs. 
        // span1 should have epochs gt or eq its overlapping segments in span2
        return span1.entrySet().stream().allMatch(e1 ->
                span2.entrySet().stream().noneMatch(e2 ->
                        (e2.getKey().segmentId() == e1.getKey().segmentId() &&
                                cut1.get(e1.getKey().segmentId()) < cut2.get(e2.getKey().segmentId()))
                                || (e2.getKey().overlaps(e1.getKey()) && e1.getValue() < e2.getValue())));
    }
    
    private CompletableFuture<StreamTruncationRecord> computeTruncationRecord(StreamTruncationRecord previous, 
                                                                              Map<Long, Long> streamCut,
                                                                              ImmutableMap<StreamSegmentRecord, Integer> span) {
        log.debug("computing truncation for stream {}/{}", scope, name);

        // find segments between "previous" stream cut and current stream cut. these are segments to delete.
        // Note: exclude segments in current streamcut
        CompletableFuture<Map<StreamSegmentRecord, Integer>> previousSpanFuture = previous.getSpan().isEmpty() ?
                getEpochRecord(0).thenApply(epoch -> convertToSpan(epoch))
                : CompletableFuture.completedFuture(previous.getSpan());

        return previousSpanFuture.thenCompose(spanFrom -> segmentsBetweenStreamCutSpans(spanFrom, span))
                                 .thenCompose(segmentsBetween -> sizeBetweenStreamCuts(previous.getStreamCut(), streamCut, segmentsBetween)
                                         .thenApply(sizeBetween -> {
                                             ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
                                             segmentsBetween.stream().map(StreamSegmentRecord::segmentId)
                                                            .filter(x -> !streamCut.containsKey(x))
                                                            .forEach(builder::add);

                                             return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), span, previous.getDeletedSegments(), 
                                                     builder.build(),
                                                     previous.getSizeTill() + sizeBetween, true);
                                         }));
    }
    
    @Override
    public CompletableFuture<Void> completeTruncation(VersionedMetadata<StreamTruncationRecord> record) {
        Preconditions.checkNotNull(record);
        Preconditions.checkArgument(record.getObject().isUpdating());
        StreamTruncationRecord current = record.getObject();
        if (current.isUpdating()) {
            StreamTruncationRecord completedProp = StreamTruncationRecord.complete(current);

            return Futures.toVoid(setTruncationData(new VersionedMetadata<>(completedProp, record.getVersion())));
        } else {
            // idempotent
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationRecord() {
        return getTruncationData(true)
                .thenApply(data -> {
                    StreamTruncationRecord truncationRecord = data.getObject();
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
        return getVersionedConfigurationRecord()
                .thenCompose(configRecord -> {
                    Preconditions.checkArgument(!configRecord.getObject().isUpdating());
                    StreamConfigurationRecord update = StreamConfigurationRecord.update(scope, name, newConfiguration);
                    return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(update, configRecord.getVersion())));
                });
    }

    /**
     * Update configuration at configurationPath.
     *
     * @return future of operation
     */
    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<StreamConfigurationRecord> existing) {
        StreamConfigurationRecord current = existing.getObject();
        Preconditions.checkNotNull(current);
        if (current.isUpdating()) {
            StreamConfigurationRecord newProperty = StreamConfigurationRecord.complete(scope, name, current.getStreamConfiguration());
            log.debug("Completing update configuration for stream {}/{}", scope, name);
            return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(newProperty, existing.getVersion())));
        } else {
            // idempotent
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Fetch configuration at configurationPath.
     *
     * @return Future of stream configuration
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getConfigurationData(false).thenApply(x -> x.getObject().getStreamConfiguration());
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getVersionedConfigurationRecord() {
        return getConfigurationData(true)
                .thenApply(data -> new VersionedMetadata<>(data.getObject(), data.getVersion()));
    }

    @Override
    public CompletableFuture<Void> updateState(final State state) {
        return getStateData(true)
                .thenCompose(currState -> {
                    VersionedMetadata<State> currentState = new VersionedMetadata<State>(currState.getObject().getState(), currState.getVersion());
                    return Futures.toVoid(updateVersionedState(currentState, state));
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<State>> getVersionedState() {
        return getStateData(true)
                .thenApply(x -> new VersionedMetadata<>(x.getObject().getState(), x.getVersion()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<State>> updateVersionedState(final VersionedMetadata<State> previous, final State newState) {
        if (State.isTransitionAllowed(previous.getObject(), newState)) {
            return setStateData(new VersionedMetadata<>(StateRecord.builder().state(newState).build(), previous.getVersion()))
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
                .thenApply(x -> x.getObject().getState());
    }

    /**
     * Fetches Segment metadata from the epoch in which segment was created.
     *
     * @param segmentId segment id.
     * @return : Future, which when complete contains segment object
     */
    @Override
    public CompletableFuture<StreamSegmentRecord> getSegment(final long segmentId) {
        // extract epoch from segment id.
        // fetch epoch record for the said epoch
        // extract segment record from it.
        int epoch = NameUtils.getEpoch(segmentId);
        return getEpochRecord(epoch)
                .thenApply(epochRecord -> {
                    Optional<StreamSegmentRecord> segmentRecord = epochRecord.getSegments().stream()
                                                                             .filter(x -> x.segmentId() == segmentId).findAny();
                    return segmentRecord
                            .orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                                    "segment not found in epoch"));
                });
    }

    @Override
    public CompletableFuture<List<ScaleMetadata>> getScaleMetadata(final long from, final long to) {
        // fetch history index and find epochs corresponding to "from" and "to"
        // fetch "from epoch" from epoch record
        // fetch epochs from history timeseries.
        CompletableFuture<Integer> fromEpoch = findEpochAtTime(from, false);
        CompletableFuture<Integer> toEpoch = findEpochAtTime(to, false);
        CompletableFuture<List<EpochRecord>> records =
                CompletableFuture.allOf(fromEpoch, toEpoch)
                                 .thenCompose(x -> {
                                     // fetch epochs will fetch it from history time series. 
                                     // this will be efficient if fromEpoch and toEpoch are near each other.
                                     return fetchEpochs(fromEpoch.join(), toEpoch.join(), false);
                                 });
        return records.thenApply(this::mapToScaleMetadata);
    }

    private List<ScaleMetadata> mapToScaleMetadata(List<EpochRecord> epochRecords) {
        final AtomicReference<List<StreamSegmentRecord>> previous = new AtomicReference<>();
        return epochRecords.stream()
                           .map(record -> {
                               long splits = 0;
                               long merges = 0;
                               List<StreamSegmentRecord> segments = record.getSegments();
                               if (previous.get() != null) {
                                   splits = findSegmentSplitsMerges(previous.get(), segments);
                                   merges = findSegmentSplitsMerges(segments, previous.get());
                               }
                               previous.set(segments);
                               return new ScaleMetadata(record.getCreationTime(), transform(segments), splits, merges);
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
    private long findSegmentSplitsMerges(List<StreamSegmentRecord> referenceSegmentsList, List<StreamSegmentRecord> targetSegmentsList) {
        return referenceSegmentsList.stream().filter(
                segment -> targetSegmentsList.stream().filter(target -> target.overlaps(segment)).count() > 1 ).count();
    }

    @Override
    public CompletableFuture<Integer> getSegmentSealedEpoch(long segmentId) {
        return getSegmentSealedRecordData(segmentId).handle((x, e) -> {
            if (e != null) {
                if (Exceptions.unwrap(e) instanceof DataNotFoundException) {
                    return -1;
                } else {
                    throw new CompletionException(e);
                }
            }
            return x.getObject();
        });
    }

    @Override
    public CompletableFuture<Set<Long>> getAllSegmentIds() {
        CompletableFuture<Map<StreamSegmentRecord, Integer>> fromSpanFuture = getTruncationRecord()
                .thenCompose(truncationRecord -> {
                    if (truncationRecord.getObject().equals(StreamTruncationRecord.EMPTY)) {
                        return getEpochRecord(0)
                                .thenApply(this::convertToSpan);
                    } else {
                        return CompletableFuture.completedFuture(truncationRecord.getObject().getSpan());
                    }
                });
        CompletableFuture<Map<StreamSegmentRecord, Integer>> toSpanFuture = getActiveEpoch(true)
                .thenApply(this::convertToSpan);

        return CompletableFuture.allOf(fromSpanFuture, toSpanFuture)
                                .thenCompose(v -> {
                                    Map<StreamSegmentRecord, Integer> fromSpan = fromSpanFuture.join();
                                    Map<StreamSegmentRecord, Integer> toSpan = toSpanFuture.join();
                                    return segmentsBetweenStreamCutSpans(fromSpan, toSpan)
                                            .thenApply(x -> x.stream().map(StreamSegmentRecord::segmentId).collect(Collectors.toSet()));
                                });
    }

    @Override
    public CompletableFuture<Map<StreamSegmentRecord, List<Long>>> getSuccessorsWithPredecessors(final long segmentId) {
        // fetch segment sealed epoch record.
        return getSegmentSealedEpoch(segmentId)
                .thenCompose(sealedEpoch -> {
                    if (sealedEpoch < 0) {
                        return getActiveEpoch(true).thenApply(activeSegments -> Collections.emptyMap());
                    }

                    // if sealed record exists. fetch its sealing epoch.
                    // Note: sealed record is created even before the segment is sealed. So if client is requesting for successor,
                    // we should find it.
                    CompletableFuture<EpochRecord> sealedEpochFuture = getEpochRecord(sealedEpoch);

                    // fetch previous epoch as well.
                    CompletableFuture<EpochRecord> previousEpochFuture = getEpochRecord(sealedEpoch - 1);

                    return CompletableFuture.allOf(sealedEpochFuture, previousEpochFuture)
                                            .thenApply(x -> {
                                                EpochRecord sealedEpochRecord = sealedEpochFuture.join();
                                                EpochRecord previousEpochRecord = previousEpochFuture.join();
                                                Optional<StreamSegmentRecord> segmentOpt = previousEpochRecord.getSegments().stream()
                                                                                                              .filter(r -> r.segmentId() == segmentId).findAny();
                                                assert segmentOpt.isPresent();
                                                StreamSegmentRecord segment = segmentOpt.get();

                                                List<StreamSegmentRecord> successors = sealedEpochRecord.getSegments().stream()
                                                                                                        .filter(r -> r.overlaps(segment)).collect(Collectors.toList());

                                                return successors
                                                        .stream().collect(Collectors.toMap(record -> record,
                                                                z -> previousEpochRecord
                                                                        .getSegments()
                                                                        .stream().filter(predecessor -> predecessor.overlaps(z))
                                                                        .map(StreamSegmentRecord::segmentId).collect(Collectors.toList())));
                                            });
                });
    }

    private CompletableFuture<EpochRecord> getActiveEpochRecord(boolean ignoreCached) {
        return getCurrentEpochRecordData(ignoreCached).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getActiveSegments() {
        // read current epoch record
        return verifyLegalState()
                .thenCompose(v -> getActiveEpochRecord(true).thenApply(epochRecord -> epochRecord.getSegments()));
    }

    @Override
    public CompletableFuture<Map<StreamSegmentRecord, Long>> getSegmentsAtHead() {
        // read current epoch record
        return getTruncationRecord()
                .thenCompose(truncationRecord -> {
                    if (truncationRecord.getObject().equals(StreamTruncationRecord.EMPTY)) {
                        return getSegmentsInEpoch(0)
                                .thenApply(segments -> segments.stream().collect(Collectors.toMap(x -> x, x ->  0L)));
                    } else {
                        return CompletableFuture.completedFuture(truncationRecord.getObject().getStreamCut().entrySet()
                                                                                 .stream().collect(Collectors.toMap(x ->
                                                truncationRecord.getObject().getSpan().keySet().stream()
                                                                          .filter(y -> y.segmentId() == x.getKey()).findFirst().get(),
                                        Map.Entry::getValue)));
                    }
                });
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(final int epoch) {
        return getEpochRecord(epoch)
                .thenApply(epochRecord -> epochRecord.getSegments());
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsBetweenStreamCuts(Map<Long, Long> from, Map<Long, Long> to) {
        return segmentsBetweenStreamCuts(from, to).thenApply(ArrayList::new);
    }

    private CompletableFuture<ImmutableSet<StreamSegmentRecord>> segmentsBetweenStreamCuts(Map<Long, Long> from, Map<Long, Long> to) {
        // compute stream cut span for `from` till `to`
        // if from is empty we need to start from epoch 0.
        // if to is empty we need to go on till current epoch.
        CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> spanFromFuture = from.isEmpty() ?
                getEpochRecord(0).thenApply(this::convertToSpan)
                : computeStreamCutSpan(from);
        CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> spanToFuture = to.isEmpty() ?
                getActiveEpochRecord(true).thenApply(this::convertToSpan)
                : computeStreamCutSpan(to);

        return CompletableFuture.allOf(spanFromFuture, spanToFuture)
                                .thenCompose(x -> {
                                    if (!from.isEmpty() && !to.isEmpty()) {
                                        Preconditions.checkArgument(RecordHelper.streamCutComparator(to, spanToFuture.join(),
                                                from, spanFromFuture.join()));
                                    }
                                    return segmentsBetweenStreamCutSpans(spanFromFuture.join(), spanToFuture.join());
                                });
    }

    @VisibleForTesting
    CompletableFuture<ImmutableSet<StreamSegmentRecord>> segmentsBetweenStreamCutSpans(Map<StreamSegmentRecord, Integer> spanFrom,
                                                                                      Map<StreamSegmentRecord, Integer> spanTo) {
        int toLow = Collections.min(spanTo.values());
        int toHigh = Collections.max(spanTo.values());
        int fromLow = Collections.min(spanFrom.values());
        int fromHigh = Collections.max(spanFrom.values());
        Set<StreamSegmentRecord> segments = new HashSet<>();

        return fetchEpochs(fromLow, toHigh, true)
                .thenAccept(epochs -> {
                    epochs.forEach(epoch -> {
                        // for epochs that cleanly lie between from.high and to.low epochs we can include all segments present in them
                        // because they are guaranteed to be greater than `from` and less than `to` stream cuts.
                        if (epoch.getEpoch() >= fromHigh && epoch.getEpoch() <= toLow) {
                            segments.addAll(epoch.getSegments());
                        } else {
                            // for each segment in epoch.segments, find overlaps in from and to
                            epoch.getSegments().stream().filter(x -> !segments.contains(x)).forEach(segment -> {
                                // if segment.number >= from.segmentNumber && segment.number <= to.segmentNumber include segment.number
                                boolean greaterThanFrom = spanFrom.keySet().stream().filter(x -> x.overlaps(segment))
                                                                  .allMatch(x -> x.segmentId() <= segment.segmentId());
                                boolean lessThanTo = spanTo.keySet().stream().filter(x -> x.overlaps(segment))
                                                           .allMatch(x -> segment.segmentId() <= x.segmentId());
                                if (greaterThanFrom && lessThanTo) {
                                    segments.add(segment);
                                }
                            });
                        }
                    });
                }).thenApply(x -> ImmutableSet.copyOf(segments));
    }

    @VisibleForTesting
    CompletableFuture<Long> sizeBetweenStreamCuts(Map<Long, Long> streamCutFrom, Map<Long, Long> streamCutTo,
                                                          Set<StreamSegmentRecord> segmentsInBetween) {
        Map<Integer, List<StreamSegmentRecord>> shards =
                segmentsInBetween.stream().collect(Collectors.groupingBy(x -> getShardNumber(x.segmentId())));
        return Futures.allOfWithResults(
                shards.entrySet().stream()
                      .map(entry -> getSealedSegmentSizeMapShard(entry.getKey())
                              .thenApply(shardMap -> {
                                  return entry.getValue().stream()
                                              .collect(Collectors.toMap(x -> x, x -> {
                                                  if (shardMap.getSize(x.segmentId()) == null) {
                                                      return Long.MIN_VALUE;
                                                  } else {
                                                      return shardMap.getSize(x.segmentId());
                                                  }
                                              } ));
                              }))
                      .collect(Collectors.toList()))
                      .thenApply(listOfMap -> {
                          return listOfMap.stream().flatMap(s -> s.entrySet().stream())
                                          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                      })
                      .thenApply(sizes -> {
                          AtomicLong sizeTill = new AtomicLong(0L);
                          sizes.forEach((segment, value) -> {
                              // segments in both.. to.offset - from.offset
                              if (streamCutTo.containsKey(segment.segmentId()) && streamCutFrom.containsKey(segment.segmentId())) {
                                  sizeTill.addAndGet(streamCutTo.get(segment.segmentId()) - streamCutFrom.get(segment.segmentId()));
                              } else if (streamCutTo.containsKey(segment.segmentId())) {
                                  // segments only in streamcutTo: take their offsets in streamcut
                                  sizeTill.addAndGet(streamCutTo.get(segment.segmentId()));
                              } else if (streamCutFrom.containsKey(segment.segmentId())) {
                                  // segments only in from: take their total size - offset in from
                                  assert value >= 0;
                                  sizeTill.addAndGet(value - streamCutFrom.get(segment.segmentId()));
                              } else {
                                  assert value >= 0;
                                  sizeTill.addAndGet(value);
                              }
                          });
                          return sizeTill.get();
                      });
    }

    @VisibleForTesting
    CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> computeStreamCutSpan(Map<Long, Long> streamCut) {
        long mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        long oldest = streamCut.keySet().stream().min(Comparator.naturalOrder()).get();
        int epochLow = NameUtils.getEpoch(oldest);
        int epochHigh = NameUtils.getEpoch(mostRecent);

        return fetchEpochs(epochLow, epochHigh, true).thenApply(epochs ->  {
            List<Long> toFind = new ArrayList<>(streamCut.keySet());
            ImmutableMap.Builder<StreamSegmentRecord, Integer> resultSet = ImmutableMap.builder();
            for (int i = epochHigh - epochLow; i >= 0; i--) {
                if (toFind.isEmpty()) {
                    break;
                }
                EpochRecord epochRecord = epochs.get(i);
                Set<Long> epochSegments = epochRecord.getSegmentIds();
                List<Long> found = toFind.stream().filter(epochSegments::contains).collect(Collectors.toList());
                resultSet.putAll(found.stream().collect(
                        Collectors.toMap(x -> epochRecord.getSegments().stream().filter(z -> z.segmentId() == x).findFirst().get(),
                                x -> epochRecord.getEpoch())));

                toFind.removeAll(epochSegments);
            }
            return resultSet.build();
        });
    }

    @Override
    public CompletableFuture<Boolean> isStreamCutValid(Map<Long, Long> streamCut) {
        Map<Integer, List<Long>> groupByEpoch = streamCut.keySet().stream().collect(groupingBy(NameUtils::getEpoch));

        CompletableFuture<List<List<Map.Entry<Double, Double>>>> segmentRangesByEpoch = Futures.allOfWithResults(groupByEpoch.entrySet().stream().map(epochGroup -> {
            return getEpochRecord(epochGroup.getKey())
                    .thenApply(epochRecord -> {
                        return epochGroup.getValue().stream().map(segmentId -> {
                            StreamSegmentRecord segment = epochRecord.getSegment(segmentId);
                            return (Map.Entry<Double, Double>) new SimpleEntry<>(segment.getKeyStart(), segment.getKeyEnd());
                        }).collect(Collectors.toList());
                    });
        }).collect(Collectors.toList()));

        CompletableFuture<List<Map.Entry<Double, Double>>> segmentRangesFlattened = segmentRangesByEpoch
                .thenApply(listOfList -> listOfList.stream().flatMap(Collection::stream).collect(Collectors.toList()));
        
        return segmentRangesFlattened
                      .thenAccept(x -> RecordHelper.validateStreamCut(new ArrayList<>(x)))
                      .handle((r, e) -> {
                          if (e != null) {
                              if (Exceptions.unwrap(e) instanceof IllegalArgumentException) {
                                  return false;
                              } else {
                                  log.warn("Exception while trying to validate a stream cut for stream {}/{}", scope, name);
                                  throw Exceptions.sneakyThrow(e);
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
     * @return : list of newly created segments with current epoch
     */
    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> submitScale(final List<Long> segmentsToSeal,
                                                                                   final List<Map.Entry<Double, Double>> newRanges,
                                                                                   final long scaleTimestamp,
                                                                                   final VersionedMetadata<EpochTransitionRecord> existing) {
        return verifyNotSealed().thenCompose(v -> {
            if (existing == null) {
                return getEpochTransition();
            } else {
                return CompletableFuture.completedFuture(existing);
            }
        }).thenCompose(record -> getActiveEpochRecord(true).thenCompose(currentEpoch -> 
                getConfiguration().thenCompose(config -> {
            if (!record.getObject().equals(EpochTransitionRecord.EMPTY)) {
                // verify that it's the same as the supplied input (--> segments to be sealed
                // and new ranges are identical). else throw scale conflict exception
                if (!RecordHelper.verifyRecordMatchesInput(segmentsToSeal, newRanges, false, record.getObject())) {
                    log.debug("scale conflict, another scale operation is ongoing");
                    throw new EpochTransitionOperationExceptions.ConflictException();
                }
                return CompletableFuture.completedFuture(record);
            } else {
                // check input is valid and satisfies preconditions
                if (!RecordHelper.canScaleFor(segmentsToSeal, currentEpoch)) {
                    return updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, record.getVersion()))
                            .thenApply(x -> {
                                log.warn("scale precondition failed {}", segmentsToSeal);
                                throw new EpochTransitionOperationExceptions.PreConditionFailureException();
                            });
                }
                if (!RecordHelper.validateInputRange(segmentsToSeal, newRanges, currentEpoch)) {
                    log.error("scale input invalid {} {}", segmentsToSeal, newRanges);
                    throw new EpochTransitionOperationExceptions.InputInvalidException();
                }

                int numberOfSegmentsPostScale = currentEpoch.getSegments().size() - segmentsToSeal.size() + newRanges.size();
                if (numberOfSegmentsPostScale < config.getScalingPolicy().getMinNumSegments()) {
                    log.warn("Scale cannot be performed as Min Segment Count will not hold {} {}", segmentsToSeal, newRanges);
                    throw new EpochTransitionOperationExceptions.PreConditionFailureException();
                }
                
                EpochTransitionRecord epochTransition = RecordHelper.computeEpochTransition(
                        currentEpoch, segmentsToSeal, newRanges, scaleTimestamp);

                return updateEpochTransitionNode(new VersionedMetadata<>(epochTransition, record.getVersion()))
                        .thenApply(version -> {
                            log.info("scale for stream {}/{} accepted. Segments to seal = {}", scope, name,
                                    epochTransition.getSegmentsToSeal());
                            return new VersionedMetadata<>(epochTransition, version);
                        });
            }
        })));
    }

    private CompletableFuture<Void> verifyNotSealed() {
        return getState(false).thenAccept(state -> {
            if (state.equals(State.SEALING) || state.equals(State.SEALED)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
        });
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> startScale(boolean isManualScale,
                                                                                  VersionedMetadata<EpochTransitionRecord> record,
                                                                                  VersionedMetadata<State> state) {
        Preconditions.checkArgument(state.getObject().equals(State.SCALING));
        return getCurrentEpochRecordData(true)
                .thenCompose(currentEpoch -> {
                    EpochRecord currentEpochRecord = currentEpoch.getObject();
                    if (isManualScale) {
                        return migrateManualScaleToNewEpoch(record, state, currentEpochRecord);
                    } else {
                        // if rolling transactions happened before the scale could be picked up, we will discard
                        // epoch transition record and reset the state to active.
                        return discardInconsistentEpochTransition(record, state, currentEpochRecord);
                    }
                });
    }


    private CompletableFuture<VersionedMetadata<EpochTransitionRecord>> discardInconsistentEpochTransition(
            VersionedMetadata<EpochTransitionRecord> epochTransition, VersionedMetadata<State> state,
            EpochRecord currentEpoch) {
        if (epochTransition.getObject().getNewEpoch() > currentEpoch.getEpoch()) {
            return CompletableFuture.completedFuture(epochTransition);
        } else {
            return updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, epochTransition.getVersion()))
                    .thenCompose(v -> updateVersionedState(state, State.ACTIVE))
                    .thenApply(v -> {
                        log.warn("Scale epoch transition record is inconsistent with VersionedMetadata in the table. {}",
                                epochTransition.getObject().getNewEpoch());
                        throw new IllegalStateException("Epoch transition record is inconsistent.");
                    });
        }
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> scaleCreateNewEpoch(
            VersionedMetadata<EpochTransitionRecord> versionedMetadata) {
        return getActiveEpochRecord(true)
                .thenCompose(currentEpoch -> {
                    // only perform idempotent update. If update is already completed, do nothing. 
                    if (currentEpoch.getEpoch() < versionedMetadata.getObject().getNewEpoch()) {
                        EpochTransitionRecord epochTransition = versionedMetadata.getObject();
                        // time
                        long time = Math.max(epochTransition.getTime(), currentEpoch.getCreationTime() + 1);
                        // new segments
                        ImmutableList.Builder<StreamSegmentRecord> newSegmentsBuilder = ImmutableList.builder();

                        epochTransition.getNewSegmentsWithRange().forEach((key, value) -> newSegmentsBuilder.add(
                                newSegmentRecord(key, time, value.getKey(), value.getValue())));
                        
                        // sealed segments
                        ImmutableList.Builder<StreamSegmentRecord> sealedSegmentsBuilder = ImmutableList.builder();
                        epochTransition.getSegmentsToSeal().forEach(x -> sealedSegmentsBuilder.add(currentEpoch.getSegment(x)));
                        
                        // overall segments in epoch
                        ImmutableList.Builder<StreamSegmentRecord> builder = ImmutableList.builder();
                        currentEpoch.getSegments()
                                    .forEach(x -> {
                                        if (!epochTransition.getSegmentsToSeal().contains(x.segmentId())) {
                                            builder.add(x);
                                        }
                                    } );
                        ImmutableList<StreamSegmentRecord> newSegments = newSegmentsBuilder.build();
                        builder.addAll(newSegments);
                        // epoch record
                        EpochRecord epochRecord = new EpochRecord(epochTransition.getNewEpoch(), epochTransition.getNewEpoch(), 
                                builder.build(), time);

                        HistoryTimeSeriesRecord timeSeriesRecord = 
                                new HistoryTimeSeriesRecord(epochTransition.getNewEpoch(), epochTransition.getNewEpoch(), 
                                        sealedSegmentsBuilder.build(), newSegments, epochRecord.getCreationTime());
                        return createEpochRecord(epochRecord)
                                .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecord))
                                .thenCompose(x -> createSegmentSealedEpochRecords(epochTransition.getSegmentsToSeal(), epochTransition.getNewEpoch()))
                                .thenApply(x -> versionedMetadata);
                    } else {
                        return CompletableFuture.completedFuture(versionedMetadata);
                    }
                });
    }
 
    private CompletableFuture<Void> updateHistoryTimeSeries(HistoryTimeSeriesRecord record) {
        int historyChunk = record.getEpoch() / historyChunkSize.get();
        boolean isFirst = record.getEpoch() % historyChunkSize.get() == 0;

        if (isFirst) {
            return createHistoryTimeSeriesChunk(historyChunk, record);
        } else {
            return getHistoryTimeSeriesChunkData(historyChunk, true)
                    .thenCompose(x -> {
                        HistoryTimeSeries historyChunkTimeSeries = x.getObject();
                        if (historyChunkTimeSeries.getLatestRecord().getEpoch() < record.getEpoch()) {
                            HistoryTimeSeries update = HistoryTimeSeries.addHistoryRecord(historyChunkTimeSeries, record);
                            return Futures.toVoid(updateHistoryTimeSeriesChunkData(historyChunk, new VersionedMetadata<>(update, x.getVersion())));
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        }
    }

    private CompletableFuture<VersionedMetadata<EpochTransitionRecord>> migrateManualScaleToNewEpoch(
            VersionedMetadata<EpochTransitionRecord> versionedMetadata, VersionedMetadata<State> versionedState,
            EpochRecord currentEpoch) {
        EpochTransitionRecord epochTransition = versionedMetadata.getObject();
        return getEpochRecord(epochTransition.getActiveEpoch())
                .thenCompose(epochRecordActiveEpoch -> {
                    if (epochTransition.getActiveEpoch() == currentEpoch.getEpoch()) {
                        // no migration needed
                        return CompletableFuture.completedFuture(versionedMetadata);
                    } else if (currentEpoch.getEpoch() > epochTransition.getActiveEpoch() &&
                            currentEpoch.getReferenceEpoch() == epochRecordActiveEpoch.getReferenceEpoch()) {

                        List<Long> duplicateSegmentsToSeal = epochTransition.getSegmentsToSeal().stream()
                                                                            .map(seg -> computeSegmentId(getSegmentNumber(seg),
                                                                                    currentEpoch.getEpoch()))
                                                                            .collect(Collectors.toList());

                        EpochTransitionRecord updatedRecord = RecordHelper.computeEpochTransition(
                                currentEpoch, duplicateSegmentsToSeal, Lists.newArrayList(epochTransition.getNewSegmentsWithRange().values()),
                                epochTransition.getTime());
                        return updateEpochTransitionNode(new VersionedMetadata<>(updatedRecord, versionedMetadata.getVersion()))
                                .thenApply(v -> new VersionedMetadata<>(updatedRecord, v));
                    } else {
                        return updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, versionedMetadata.getVersion()))
                                .thenCompose(v -> updateVersionedState(versionedState, State.ACTIVE))
                                .thenApply(v -> {
                                    log.warn("Scale epoch transition record is inconsistent with VersionedMetadata in the table. {}",
                                            epochTransition.getNewEpoch());
                                    throw new IllegalStateException("Epoch transition record is inconsistent.");
                                });
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransition() {
        return getEpochTransitionNode()
                .thenApply(x -> new VersionedMetadata<>(x.getObject(), x.getVersion()));
    }

    private CompletableFuture<Void> clearMarkers(final Set<Long> segments) {
        return Futures.toVoid(Futures.allOfWithResults(segments.stream().parallel()
                                                               .map(this::removeColdMarker).collect(Collectors.toList())));
    }
    
    @Override
    public CompletableFuture<Void> scaleOldSegmentsSealed(Map<Long, Long> sealedSegmentSizes,
                                                          VersionedMetadata<EpochTransitionRecord> record) {
        EpochTransitionRecord epochTransition = record.getObject();
        return Futures.toVoid(clearMarkers(epochTransition.getSegmentsToSeal())
                .thenCompose(x -> updateSealedSegmentSizes(sealedSegmentSizes))
                .thenCompose(x -> updateCurrentEpochRecord(epochTransition.getNewEpoch())));
    }

    @Override
    public CompletableFuture<Void> completeScale(VersionedMetadata<EpochTransitionRecord> record) {
        Preconditions.checkNotNull(record);
        Preconditions.checkArgument(!record.getObject().equals(EpochTransitionRecord.EMPTY));
        return Futures.toVoid(updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, record.getVersion())));
    }
    
    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startRollingTxn(int activeEpoch,
                                                                                              VersionedMetadata<CommittingTransactionsRecord> existing) {
        CommittingTransactionsRecord record = existing.getObject();
        if (record.isRollingTxnRecord()) {
            return CompletableFuture.completedFuture(existing);
        } else {
            CommittingTransactionsRecord update = record.createRollingTxnRecord(activeEpoch);
            return updateCommittingTxnRecord(new VersionedMetadata<>(update, existing.getVersion()))
                    .thenApply(version -> new VersionedMetadata<>(update, version));
        }
    }

    @Override
    public CompletableFuture<Void> rollingTxnCreateDuplicateEpochs(
            Map<Long, Long> sealedTxnEpochSegments, long time, VersionedMetadata<CommittingTransactionsRecord> record) {
        Preconditions.checkArgument(record.getObject().isRollingTxnRecord());
        CommittingTransactionsRecord committingTxnRecord = record.getObject();
        return getActiveEpoch(true)
                .thenCompose(activeEpochRecord -> getEpochRecord(committingTxnRecord.getEpoch())
                        .thenCompose(transactionEpochRecord -> {
                            if (activeEpochRecord.getEpoch() > committingTxnRecord.getCurrentEpoch()) {
                                log.debug("Duplicate Epochs {} already created. Ignore.", committingTxnRecord.getNewActiveEpoch());
                                return CompletableFuture.completedFuture(null);
                            }
                            long timeStamp = Math.max(activeEpochRecord.getCreationTime() + 1, time);
                            ImmutableList.Builder<StreamSegmentRecord> duplicateTxnSegmentsBuilder = ImmutableList.builder();
                            transactionEpochRecord
                                    .getSegments().stream()
                                    .forEach(x ->
                                            duplicateTxnSegmentsBuilder.add(newSegmentRecord(computeSegmentId(getSegmentNumber(x.segmentId()),
                                                    committingTxnRecord.getNewTxnEpoch()),
                                                    timeStamp, x.getKeyStart(), x.getKeyEnd())));
                            ImmutableList.Builder<StreamSegmentRecord> duplicateActiveSegmentsBuilder = ImmutableList.builder();

                            activeEpochRecord
                                    .getSegments().stream()
                                    .forEach(x -> duplicateActiveSegmentsBuilder.add(
                                            newSegmentRecord(computeSegmentId(getSegmentNumber(x.segmentId()),
                                            committingTxnRecord.getNewActiveEpoch()),
                                            timeStamp + 1, x.getKeyStart(), x.getKeyEnd())));

                            EpochRecord duplicateTxnEpoch = new EpochRecord(committingTxnRecord.getNewTxnEpoch(), 
                                    transactionEpochRecord.getReferenceEpoch(), duplicateTxnSegmentsBuilder.build(), timeStamp);

                            EpochRecord duplicateActiveEpoch = new EpochRecord(committingTxnRecord.getNewActiveEpoch(),
                                    activeEpochRecord.getReferenceEpoch(), duplicateActiveSegmentsBuilder.build(), timeStamp + 1);

                            HistoryTimeSeriesRecord timeSeriesRecordTxnEpoch =
                                    new HistoryTimeSeriesRecord(duplicateTxnEpoch.getEpoch(), duplicateTxnEpoch.getReferenceEpoch(), 
                                            ImmutableList.of(), ImmutableList.of(), timeStamp);

                            HistoryTimeSeriesRecord timeSeriesRecordActiveEpoch =
                                    new HistoryTimeSeriesRecord(duplicateActiveEpoch.getEpoch(), duplicateActiveEpoch.getReferenceEpoch(),
                                            ImmutableList.of(), ImmutableList.of(), timeStamp + 1);
                            return createEpochRecord(duplicateTxnEpoch)
                                    .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordTxnEpoch))
                                    .thenCompose(x -> createEpochRecord(duplicateActiveEpoch))
                                    .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordActiveEpoch))
                                    .thenCompose(x -> createSegmentSealedEpochRecords(
                                            activeEpochRecord.getSegments().stream().map(StreamSegmentRecord::segmentId)
                                                             .collect(Collectors.toList()), duplicateTxnEpoch.getEpoch()))
                                    .thenCompose(x -> createSegmentSealedEpochRecords(
                                            duplicateTxnEpoch.getSegments().stream().map(StreamSegmentRecord::segmentId)
                                                             .collect(Collectors.toList()), duplicateActiveEpoch.getEpoch()));
                        })
                        .thenCompose(r -> updateSealedSegmentSizes(sealedTxnEpochSegments)));
    }

    @Override
    public CompletableFuture<Void> completeRollingTxn(Map<Long, Long> sealedActiveEpochSegments,
                                                      VersionedMetadata<CommittingTransactionsRecord> versionedMetadata) {
        return getActiveEpoch(true)
                .thenCompose(activeEpochRecord -> {
                    CommittingTransactionsRecord committingTxnRecord = versionedMetadata.getObject();
                    int activeEpoch = committingTxnRecord.getCurrentEpoch();
                    if (activeEpochRecord.getEpoch() == activeEpoch) {
                        return updateSealedSegmentSizes(sealedActiveEpochSegments)
                                .thenCompose(x -> clearMarkers(sealedActiveEpochSegments.keySet()))
                                .thenCompose(x -> updateCurrentEpochRecord(committingTxnRecord.getNewActiveEpoch()));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<UUID> generateNewTxnId(int msb32Bit, long lsb64Bit) {
        return getActiveEpochRecord(true)
                .thenApply(epochRecord -> {
                    // always set transaction epoch as refrence epoch so that all transactions on duplicate epochs
                    // are collected.
                    // epochs that are not duplicates will refer to themselves.
                    return RecordHelper.generateTxnId(epochRecord.getReferenceEpoch(), msb32Bit, lsb64Bit);
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
        final int epoch = RecordHelper.getTransactionEpoch(txnId);
        ActiveTxnRecord record = ActiveTxnRecord.builder().txnStatus(TxnStatus.OPEN).leaseExpiryTime(leaseTimestamp)
                                                .txCreationTimestamp(current).maxExecutionExpiryTime(maxExecTimestamp)
                                                .writerId(Optional.empty())
                                                .commitTime(Optional.empty())
                                                .commitOrder(Optional.empty())
                                                .build();
        return verifyNotSealed().thenCompose(v -> createNewTransaction(epoch, txnId, record)
                .thenApply(version -> new VersionedTransactionData(epoch, txnId, version,
                        TxnStatus.OPEN, current, maxExecTimestamp, "", Long.MIN_VALUE, Long.MIN_VALUE, ImmutableMap.of())));
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
        final String writerId = txnData.getWriterId();
        final long commitTime = txnData.getCommitTime();
        final long position = txnData.getPosition();
        final ImmutableMap<Long, Long> commitOffsets = txnData.getCommitOffsets();
        final ActiveTxnRecord newData = new ActiveTxnRecord(creationTime, System.currentTimeMillis() + lease,
                maxExecutionExpiryTime, status, writerId, commitTime, position, commitOffsets);
        final VersionedMetadata<ActiveTxnRecord> data = new VersionedMetadata<>(newData, version);

        return updateActiveTx(epoch, txnId, data)
                .thenApply(updatedVersion -> new VersionedTransactionData(epoch, txnId, updatedVersion, status, creationTime, 
                        maxExecutionExpiryTime, writerId, commitTime, position, commitOffsets));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId) {
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return getActiveTx(epoch, txId)
                .thenApply(data -> {
                    ActiveTxnRecord activeTxnRecord = data.getObject();
                    return new VersionedTransactionData(epoch, txId, data.getVersion(),
                            activeTxnRecord.getTxnStatus(), activeTxnRecord.getTxCreationTimestamp(),
                            activeTxnRecord.getMaxExecutionExpiryTime(), activeTxnRecord.getWriterId(), 
                            activeTxnRecord.getCommitTime(), activeTxnRecord.getCommitOrder(), activeTxnRecord.getCommitOffsets());
                });
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId) {
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return getActiveTx(epoch, txId).handle((ok, ex) -> {
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return ok.getObject().getTxnStatus();
        }).thenCompose(x -> {
            if (x.equals(TxnStatus.UNKNOWN)) {
                return getCompletedTxnStatus(txId);
            } else {
                return CompletableFuture.completedFuture(x);
            }
        });
    }

    private CompletableFuture<TxnStatus> getCompletedTxnStatus(UUID txId) {
        return getCompletedTx(txId).handle((ok, ex) -> {
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return ok.getObject().getCompletionStatus();
        });
    }

    @Override
    public CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final UUID txId, final boolean commit,
                                                                              final Optional<Version> version,                                                                               
                                                                              final String writerId, final long timestamp) {
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return sealActiveTxn(epoch, txId, commit, version, writerId, timestamp)
                .exceptionally(ex -> new SimpleEntry<>(handleDataNotFoundException(ex), null))
                .thenCompose(pair -> {
                    if (pair.getKey() == TxnStatus.UNKNOWN) {
                        return validateCompletedTxn(txId, commit).thenApply(status -> new SimpleEntry<>(status, null));
                    } else {
                        return CompletableFuture.completedFuture(pair);
                    }
                });
    }

    /**
     * Seal a transaction in OPEN/COMMITTING_TXN/ABORTING state. This method does CAS on the transaction VersionedMetadata node if
     * the transaction is in OPEN state, optionally checking version of transaction VersionedMetadata node, if required.
     *
     * @param epoch   transaction epoch.
     * @param txId    transaction identifier.
     * @param commit  boolean indicating whether to commit or abort the transaction.
     * @param version optional expected version of transaction node to validate before updating it.
     * @param writerId writer Id
     * @param timestamp commit timestamp supplied by writer
     * @return        a pair containing transaction status and its epoch.
     */
    private CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealActiveTxn(final int epoch,
                                                                             final UUID txId,
                                                                             final boolean commit,
                                                                             final Optional<Version> version, 
                                                                             final String writerId, final long timestamp) {
        return getActiveTx(epoch, txId).thenCompose(data -> {
            ActiveTxnRecord txnRecord = data.getObject();
            Version dataVersion = version.orElseGet(data::getVersion);
            TxnStatus status = txnRecord.getTxnStatus();
            switch (status) {
                case OPEN:
                    return sealActiveTx(epoch, txId, commit, txnRecord, dataVersion, writerId, timestamp).thenApply(y ->
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
                                                    final Version version, final String writerId, final long timestamp) {
        CompletableFuture<ActiveTxnRecord> future;
        if (commit) {
            // if request is for commit --> add a new entry to orderer. 
            // update record
            if (!previous.getTxnStatus().equals(TxnStatus.COMMITTING)) {
                future = addTxnToCommitOrder(txId)
                        .thenApply(position -> {
                            return new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                                    previous.getLeaseExpiryTime(),
                                    previous.getMaxExecutionExpiryTime(),
                                    TxnStatus.COMMITTING,
                                    writerId,
                                    timestamp,
                                    position);
                        });
            } else {
                future = CompletableFuture.completedFuture(previous);
            }
        } else {
            future = CompletableFuture.completedFuture(new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                    previous.getLeaseExpiryTime(),
                    previous.getMaxExecutionExpiryTime(),
                    TxnStatus.ABORTING));
        }
        return future.thenCompose(updated -> {
            final VersionedMetadata<ActiveTxnRecord> data = new VersionedMetadata<>(updated, version);
            return updateActiveTx(epoch, txId, data);  
        });
    }

    @Override
    public CompletableFuture<Void> recordCommitOffsets(final UUID txnId, final Map<Long, Long> commitOffsets) {
        // The transaction may already have been committed and its record removed and this could be an idempotent run. 
        // So this method will ignore any data not found exceptions that are thrown while trying to retrieve active 
        // transaction record. 
        // This method updates the commit offsets if they are empty. Otherwise it ignores them. 
        int epoch = RecordHelper.getTransactionEpoch(txnId);
        return Futures.exceptionallyExpecting(getActiveTx(epoch, txnId)
                .thenCompose(txnRecord -> {
                    ActiveTxnRecord activeTxnRecord = txnRecord.getObject();
                    Preconditions.checkArgument(activeTxnRecord.getTxnStatus().equals(TxnStatus.COMMITTING));
                    if (activeTxnRecord.getCommitOffsets().isEmpty()) {
                        ActiveTxnRecord updated = new ActiveTxnRecord(activeTxnRecord.getTxCreationTimestamp(),
                                activeTxnRecord.getLeaseExpiryTime(),
                                activeTxnRecord.getMaxExecutionExpiryTime(),
                                TxnStatus.COMMITTING,
                                activeTxnRecord.getWriterId(),
                                activeTxnRecord.getCommitTime(),
                                activeTxnRecord.getCommitOrder(),
                                ImmutableMap.copyOf(commitOffsets));
                        return Futures.toVoid(updateActiveTx(epoch, txnId, new VersionedMetadata<>(updated, txnRecord.getVersion())));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }), DATA_NOT_FOUND_PREDICATE, null);
    }

    /**
     * This method takes the list of transactions in the committing transactions record, and tries to report marks
     * for writers for these transactions, if the information about writer is present in the record. The information 
     * about writer and commit time is optionally provided by the client. A client not interested in watermarking may not 
     * report writer id and time during commit request. Similarly older clients will not report writer id and time either.  
     * WriterId, commit time and commit offsets are recorded in ActiveTxnRecord for each transaction. 
     * For transactions where above fields are present, a mark is recorded for them. 
     * This method will ignore any INVALID_TIME or INVALID_POSITION related failures in noting marks for writers.
     * This is because those will typically arise from idempotent commit case where this and a transaction with higher 
     * position and time may already have been committed and the overall mark for the writer may already have progressed.
     * 
     * @param committingTransactionsRecord Committing transaction record
     * @return A completableFuture, which when completed will have marks reported for all transactions in the committing 
     * transaction record for which a writer with time and position information is available. 
     */
    CompletableFuture<Void> generateMarksForTransactions(CommittingTransactionsRecord committingTransactionsRecord) {
        val getTransactionsFuture = Futures.allOfWithResults(committingTransactionsRecord.getTransactionsToCommit().stream().map(txId -> {
            int epoch = RecordHelper.getTransactionEpoch(txId);
            // Ignore data not found exceptions. DataNotFound Exceptions can be thrown because transaction record no longer 
            // exists and this is an idempotent case. DataNotFound can also be thrown because writer's mark was deleted 
            // as we attempted to update an existing record. Note: Delete can be triggered by writer explicitly calling
            // removeWriter api. 
            return Futures.exceptionallyExpecting(getActiveTx(epoch, txId), DATA_NOT_FOUND_PREDICATE, null);
        }).collect(Collectors.toList()));
        
        return getTransactionsFuture
                .thenCompose(txnRecords -> {
                    // Filter transactions for which either writer id is not present of time/position is not reported
                    // Then group transactions by writer ids
                    val groupedByWriters = txnRecords.stream().filter(x ->
                            x != null && !Strings.isNullOrEmpty(x.getObject().getWriterId()) &&
                                    x.getObject().getCommitTime() >= 0L && !x.getObject().getCommitOffsets().isEmpty())
                                                     .collect(Collectors.groupingBy(x -> x.getObject().getWriterId()));

                    // For each writerId we will take the transaction with the time and position pair (which is to take
                    // max of all transactions for the said writer). 
                    // Note: if multiple transactions from same writer have same time, we will take any one arbitrarily and
                    // use its position for watermarks. Other positions and times would be ignored. 
                    val noteTimeFutures = groupedByWriters.entrySet().stream().map(groupEntry -> {
                        ActiveTxnRecord latest = groupEntry.getValue().stream().max(Comparator.comparingLong(x -> x.getObject().getCommitTime()))
                                                           .get().getObject();
                        return Futures.exceptionallyExpecting(
                                noteWriterMark(latest.getWriterId(), latest.getCommitTime(), latest.getCommitOffsets()),
                                DATA_NOT_FOUND_PREDICATE, null);
                    }).collect(Collectors.toList());
                    return Futures.allOf(noteTimeFutures);
                });
    }

    @VisibleForTesting
    public CompletableFuture<TxnStatus> commitTransaction(final UUID txId) {
        int epoch = RecordHelper.getTransactionEpoch(txId);

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
                        return createCompletedTxEntry(txId, new CompletedTxnRecord(System.currentTimeMillis(), TxnStatus.COMMITTED));
                    } else {
                        return CompletableFuture.completedFuture(null); // already committed, do nothing
                    }
                }).thenCompose(x -> removeActiveTxEntry(epoch, txId)).thenApply(x -> TxnStatus.COMMITTED);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final UUID txId) {
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return checkTransactionStatus(txId).thenApply(x -> {
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
                return createCompletedTxEntry(txId, new CompletedTxnRecord(System.currentTimeMillis(), TxnStatus.ABORTED));
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

    private CompletableFuture<TxnStatus> validateCompletedTxn(UUID txId, boolean commit) {
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
    public CompletableFuture<EpochRecord> getActiveEpoch(boolean ignoreCached) {
        return getCurrentEpochRecordData(ignoreCached).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<EpochRecord> getEpochRecord(int epoch) {
        return getEpochRecordData(epoch).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> setColdMarker(long segmentId, long timestamp) {
        return getMarkerData(segmentId).thenCompose(x -> {
            if (x != null) {
                final VersionedMetadata<Long> data = new VersionedMetadata<>(timestamp, x.getVersion());
                return Futures.toVoid(updateMarkerData(segmentId, data));
            } else {
                return createMarkerData(segmentId, timestamp);
            }
        });
    }

    @Override
    public CompletableFuture<Long> getColdMarker(long segmentId) {
        return getMarkerData(segmentId)
                .thenApply(x -> (x != null) ? x.getObject() : 0L);
    }

    @Override
    public CompletableFuture<Void> removeColdMarker(long segmentId) {
        return removeMarkerData(segmentId);
    }

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(Map<Long, Long> streamCut, Optional<StreamCutRecord> reference) {
        Map<Long, Long> referenceStreamCut = reference.map(StreamCutRecord::getStreamCut).orElse(Collections.emptyMap());
        return segmentsBetweenStreamCuts(referenceStreamCut, streamCut)
                .thenCompose(segmentsInBetween -> sizeBetweenStreamCuts(referenceStreamCut, streamCut, segmentsInBetween)
                        .thenApply(sizeBetween -> sizeBetween + reference.map(StreamCutRecord::getRecordingSize).orElse(0L)));
    }

    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(StreamCutRecord record) {
        return getRetentionSetData()
                .thenCompose(data -> {
                    RetentionSet retention = data.getObject();

                    RetentionSet update = RetentionSet.addReferenceToStreamCutIfLatest(retention, record);
                    return createStreamCutRecordData(record.getRecordingTime(), record)
                            .thenCompose(v -> Futures.toVoid(updateRetentionSetData(new VersionedMetadata<>(update, data.getVersion()))));
                });
    }

    @Override
    public CompletableFuture<RetentionSet> getRetentionSet() {
        return getRetentionSetData()
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<StreamCutRecord> getStreamCutRecord(StreamCutReferenceRecord record) {
        return getStreamCutRecordData(record.getRecordingTime()).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> deleteStreamCutBefore(StreamCutReferenceRecord record) {
        return getRetentionSetData()
                .thenCompose(data -> {
                    RetentionSet retention = data.getObject();
                    RetentionSet update = RetentionSet.removeStreamCutBefore(retention, record);
                    List<StreamCutReferenceRecord> toRemove = retention.retentionRecordsBefore(record);
                    return Futures.allOf(toRemove.stream().map(x -> deleteStreamCutRecordData(x.getRecordingTime())).collect(Collectors.toList()))
                                  .thenCompose(x -> Futures.toVoid(updateRetentionSetData(new VersionedMetadata<>(update, data.getVersion()))));
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startCommittingTransactions(int limit) {
        return getVersionedCommitTransactionsRecord()
                .thenCompose(versioned -> {
                    if (versioned.getObject().equals(CommittingTransactionsRecord.EMPTY)) {
                        return getOrderedCommittingTxnInLowestEpoch(limit)
                                .thenCompose(list -> {
                                    if (list.isEmpty()) {
                                        return CompletableFuture.completedFuture(versioned);
                                    } else {
                                        Map.Entry<UUID, ActiveTxnRecord> firstEntry = list.get(0);
                                        ImmutableList.Builder<UUID> txIdList = ImmutableList.builder(); 
                                        list.forEach(x -> txIdList.add(x.getKey()));
                                        List<Long> positions = list.stream().map(x -> x.getValue().getCommitOrder()).collect(Collectors.toList());
                                        int epoch = RecordHelper.getTransactionEpoch(firstEntry.getKey());
                                        CommittingTransactionsRecord record =
                                                new CommittingTransactionsRecord(epoch, txIdList.build());
                                        return updateCommittingTxnRecord(new VersionedMetadata<>(record, versioned.getVersion()))
                                                // now that we have included transactions from positions for commit, we
                                                // can safely remove the position references in orderer. 
                                                .thenCompose(version -> removeTxnsFromCommitOrder(positions)
                                                        .thenApply(v -> new VersionedMetadata<>(record, version)));
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(versioned);
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getVersionedCommitTransactionsRecord() {
        return getCommitTxnRecord()
                .thenApply(r -> new VersionedMetadata<>(r.getObject(), r.getVersion()));
    }

    @Override
    public CompletableFuture<Void> completeCommittingTransactions(VersionedMetadata<CommittingTransactionsRecord> record) {
        // Chain all transaction commit futures one after the other. This will ensure that order of commit
        // if honoured and is based on the order in the list.
        CompletableFuture<Void> future = generateMarksForTransactions(record.getObject());
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
                .thenCompose(x -> getNumberOfOngoingTransactions().thenAccept(count ->
                                TransactionMetrics.reportOpenTransactions(getScope(), getName(), count)))
                .thenCompose(x -> Futures.toVoid(updateCommittingTxnRecord(new VersionedMetadata<>(CommittingTransactionsRecord.EMPTY,
                        record.getVersion()))));
    }

    @Override
    public CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName) {
        return createWaitingRequestNodeIfAbsent(processorName);
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
                        return data;
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
    public CompletableFuture<WriterTimestampResponse> noteWriterMark(String writer, long timestamp, Map<Long, Long> position) {
        // Remember: a writer position is not a stream cut.  
        // For sanity check we will check that the request advances the time and position.
        // if time is not advanced --> respond with invalid time.
        // if position is not advanced --> respond with invalid position
        ImmutableMap<Long, Long> newPosition = ImmutableMap.copyOf(position);
        return Futures.exceptionallyExpecting(getWriterMarkRecord(writer), DATA_NOT_FOUND_PREDICATE, null) 
                .thenCompose(record -> {
                    if (record == null) {
                        // Attempt to create a new record. It is possible that while we are attempting to create the record, 
                        // a concurrent request could have created it. 
                        // For example: a writer sends a request to one controller instance to note a writer mark and the 
                        // connection drops before it receives a confirmation. It could then send the request to noteTime to a different controller 
                        // instance. So two controller instances could attempt concurrent creation of mark for the said writer.
                        // In this case, we may have found mark to be non existent when we did a get but it may have been created 
                        // when we attempt to create it. So immediately after attempting a create, if we get DataExists, 
                        // we will translate it to writeConflict and let the caller deal with it.
                        return createWriterMarkRecord(writer, timestamp, newPosition)
                                .exceptionally(e -> {
                                    if (Exceptions.unwrap(e) instanceof StoreException.DataExistsException) {
                                        throw StoreException.create(StoreException.Type.WRITE_CONFLICT, "writer mark exists");
                                    }
                                    throw new CompletionException(e);
                                })
                                .thenApply(v -> WriterTimestampResponse.SUCCESS);
                    } else {
                        // sanity check and update
                        if (record.getObject().getTimestamp() > timestamp) {
                            // existing time is already ahead of new time
                            return CompletableFuture.completedFuture(WriterTimestampResponse.INVALID_TIME);
                        }
                        if (!compareWriterPositions(record.getObject().getPosition(), newPosition)) {
                            // existing position is already ahead of new position
                            return CompletableFuture.completedFuture(WriterTimestampResponse.INVALID_POSITION);
                        }
                        
                        // its a valid mark, update it
                        return updateWriterMarkRecord(writer, timestamp, newPosition, true, record.getVersion())
                                .thenApply(v -> WriterTimestampResponse.SUCCESS);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> shutdownWriter(String writer) {
        return getWriterMarkRecord(writer)
               .thenCompose(record -> updateWriterMarkRecord(writer, record.getObject().getTimestamp(), record.getObject().getPosition(), 
                       false, record.getVersion()));
    }

    @Override
    public CompletableFuture<Void> removeWriter(String writer, WriterMark writerMark) {
        return Futures.exceptionallyExpecting(getWriterMarkRecord(writer), DATA_NOT_FOUND_PREDICATE, null)
                      .thenCompose(record -> {
                          if (record == null) {
                              // do nothing
                              return CompletableFuture.completedFuture(null);
                          } else {
                              if (writerMark.equals(record.getObject())) {
                                  return removeWriterRecord(writer, record.getVersion());
                              } else {
                                  throw StoreException.create(StoreException.Type.WRITE_CONFLICT, 
                                          "Writer mark supplied for removal doesn't match stored writer mark");
                              }
                          }
                      });
    }

    /**
     * Compares two given positions and returns true if position 2 is ahead of position 1. 
     * 
     * Note: This is not absolutely correct implementation because that would be a very costly computation. 
     * This method simply checks if position 2 has a higher max segment number than position 1. 
     * If those are equal, then it simply checks the offsets in each segment in position 2 to be ahead of same segment in 
     * position 1. 
     * It doesn't deal with complex cases where in a position 1 there could be both predecessor and successor while position 2 
     * only contains successor. There can be many such complex checks which will require looking up ranges of segments involved
     * which we will avoid in this method. 
     * 
     * @param position1 position 1 
     * @param position2 position 2
     * @return return true if position 2 is ahead of/or equal to position 1. false otherwise. 
     */
    @VisibleForTesting
    boolean compareWriterPositions(Map<Long, Long> position1, Map<Long, Long> position2) {
        long maxInPos2 = position2.keySet().stream().filter(position1::containsKey).max(Long::compare).orElse(Long.MIN_VALUE);
        long maxInPos1 = position1.keySet().stream().filter(position2::containsKey).max(Long::compare).orElse(Long.MIN_VALUE);
        
        boolean compareMaxes = maxInPos2 >= maxInPos1;
        
        if (compareMaxes) {
            return position2.entrySet().stream()
                            .filter(x -> position1.containsKey(x.getKey()))
                            .allMatch(x -> {
                                // for all segments that are present in both, position 2 should have 
                                // greater than eq offsets
                                return x.getValue() >= position1.get(x.getKey());
                            });
        } 
        
        return false;
    }

    @Override
    public CompletableFuture<WriterMark> getWriterMark(String writer) {
        return getWriterMarkRecord(writer).thenApply(VersionedMetadata::getObject);
    }
    
    protected CompletableFuture<List<Map.Entry<UUID, ActiveTxnRecord>>> getOrderedCommittingTxnInLowestEpochHelper(
            ZkOrderedStore txnCommitOrderer, int limit, Executor executor) {
        // get all transactions that have been added to commit orderer.
        return Futures.exceptionallyExpecting(txnCommitOrderer.getEntitiesWithPosition(getScope(), getName()),
                AbstractStreamMetadataStore.DATA_NOT_FOUND_PREDICATE, Collections.emptyMap())
                      .thenCompose(allTxns -> {
                          // group transactions by epoch and then iterate over it from smallest epoch to largest
                          val groupByEpoch = allTxns.entrySet().stream().collect(
                                  Collectors.groupingBy(x -> RecordHelper.getTransactionEpoch(UUID.fromString(x.getValue()))));

                          // sort transaction groups by epoch
                          val iterator = groupByEpoch
                                  .entrySet().stream()
                                  .sorted(Comparator.comparingInt(Map.Entry::getKey))
                                  .iterator();

                          // We will opportunistically identify ordered positions that are stale (either transaction is no longer active)
                          // or its a duplicate entry or transaction is aborting. 
                          ConcurrentSkipListSet<Long> toPurge = new ConcurrentSkipListSet<>();
                          ConcurrentHashMap<UUID, ActiveTxnRecord> transactionsMap = new ConcurrentHashMap<>();

                          // Collect transactions that are in committing state from smallest available epoch 
                          // smallest epoch has transactions in committing state, we should break, else continue.
                          // also remove any transaction order references which are invalid.
                          return Futures.loop(() -> iterator.hasNext() && transactionsMap.isEmpty(), () -> {
                              return processTransactionsInEpoch(iterator.next(), toPurge, transactionsMap, limit, executor);
                          }, executor).thenCompose(v -> txnCommitOrderer.removeEntities(getScope(), getName(), toPurge))
                                        .thenApply(v ->
                                                transactionsMap.entrySet().stream().sorted(
                                                        Comparator.comparing(x -> x.getValue().getCommitOrder()))
                                                               .collect(Collectors.toList()));
                      });
    }

    protected CompletableFuture<Map<Long, UUID>> getAllOrderedCommittingTxnsHelper(ZkOrderedStore txnCommitOrderer) {
        return Futures.exceptionallyExpecting(txnCommitOrderer.getEntitiesWithPosition(getScope(), getName()),
                ZKStreamMetadataStore.DATA_NOT_FOUND_PREDICATE, Collections.emptyMap())
                      .thenApply(map -> map.entrySet().stream()
                                           .collect(Collectors.toMap(Map.Entry::getKey, x -> UUID.fromString(x.getValue()))));
    }

    private CompletableFuture<Void> processTransactionsInEpoch(Map.Entry<Integer, List<Map.Entry<Long, String>>> nextEpoch,
                                                             ConcurrentSkipListSet<Long> toPurge,
                                                             ConcurrentHashMap<UUID, ActiveTxnRecord> transactionsMap,
                                                             int limit,
                                                             Executor executor) {
        int epoch = nextEpoch.getKey();
        List<Long> orders = new ArrayList<>();
        List<String> txnIds = new ArrayList<>();
        nextEpoch.getValue().forEach(x -> {
            orders.add(x.getKey());
            txnIds.add(x.getValue());
        });

        AtomicInteger from = new AtomicInteger(0);
        AtomicInteger till = new AtomicInteger(Math.min(limit, txnIds.size()));
        return Futures.loop(() -> from.get() < txnIds.size() && transactionsMap.size() < limit, 
                () -> getTransactionRecords(epoch, txnIds.subList(from.get(), till.get())).thenAccept(txns -> {
            for (int i = 0; i < txns.size(); i++) {
                ActiveTxnRecord txnRecord = txns.get(i);
                UUID txnId = UUID.fromString(txnIds.get(i));
                long order = orders.get(i);
                switch (txnRecord.getTxnStatus()) {
                    case COMMITTING:
                        if (txnRecord.getCommitOrder() == order) {
                            // if entry matches record's position then include it
                            transactionsMap.put(txnId, txnRecord);
                            if (transactionsMap.size() >= limit) {
                                break;
                            }
                        } else {
                            log.debug("duplicate txn {} at position {}. removing {}", txnId, txnRecord.getCommitOrder(), order);
                            toPurge.add(order);
                        }
                        break;
                    case OPEN:  // do nothing
                        // since we first add reference to transaction order followed by updating transaction
                        // metadata record, which may or may not have happened. So we will ignore all open 
                        // transactions for which references are found. 
                        break;
                    case COMMITTED:
                    case ABORTING:
                    case ABORTED:
                    case UNKNOWN:
                        // Aborting, aborted, unknown and committed 
                        log.debug("stale txn {} with status. removing {}", txnId, txnRecord.getTxnStatus(), order);
                        toPurge.add(order);
                        break;
                }
            }
            from.set(till.get());
            till.set(Math.min(from.get() + limit, txnIds.size()));
        }), executor);
    }

    CompletableFuture<List<ActiveTxnRecord>> getTransactionRecords(int epoch, List<String> txnIds) {
        return Futures.allOfWithResults(txnIds.stream().map(txnIdStr -> {
            UUID txnId = UUID.fromString(txnIdStr);
            return Futures.exceptionallyExpecting(getActiveTx(epoch, txnId).thenApply(VersionedMetadata::getObject),
                    ZKStreamMetadataStore.DATA_NOT_FOUND_PREDICATE, ActiveTxnRecord.EMPTY);
        }).collect(Collectors.toList()));
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

    private CompletableFuture<Void> createEpochRecord(EpochRecord epoch) {
        return createEpochRecordDataIfAbsent(epoch.getEpoch(), epoch);
    }

    private CompletableFuture<Void> updateCurrentEpochRecord(int newActiveEpoch) {
        return getEpochRecord(newActiveEpoch)
                .thenCompose(epochRecord -> getCurrentEpochRecordData(true)
                        .thenCompose(currentEpochRecordData -> {
                            EpochRecord existing = currentEpochRecordData.getObject();
                            if (existing.getEpoch() < newActiveEpoch) {
                                return Futures.toVoid(updateCurrentEpochRecordData(
                                        new VersionedMetadata<>(epochRecord, currentEpochRecordData.getVersion())));
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }));
    }

    private CompletableFuture<Void> createSealedSegmentSizeMapShardIfAbsent(int shardNumber) {
        SealedSegmentsMapShard shard = SealedSegmentsMapShard.builder().shardNumber(shardNumber).sealedSegmentsSizeMap(Collections.emptyMap()).build();
        return createSealedSegmentSizesMapShardDataIfAbsent(shardNumber, shard);
    }

    @Override
    public CompletableFuture<SealedSegmentsMapShard> getSealedSegmentSizeMapShard(int shard) {
        return getSealedSegmentSizesMapShardData(shard)
                .handle((r, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof DataNotFoundException) {
                            return SealedSegmentsMapShard.builder().shardNumber(shard).sealedSegmentsSizeMap(Collections.emptyMap()).build();
                        } 
                        throw new CompletionException(e);
                    } else {
                        return r.getObject();
                    }
                });
    }

    private CompletableFuture<Void> updateSealedSegmentSizes(Map<Long, Long> sealedSegmentSizes) {
        Map<Integer, List<Long>> shards = sealedSegmentSizes.keySet().stream()
                                                            .collect(Collectors.groupingBy(this::getShardNumber));
        return Futures.allOf(shards.entrySet().stream().map(x -> {
            int shard = x.getKey();
            List<Long> segments = x.getValue();

            return Futures.exceptionallyComposeExpecting(getSealedSegmentSizesMapShardData(shard), 
                    DATA_NOT_FOUND_PREDICATE, () -> createSealedSegmentSizeMapShardIfAbsent(shard)
                            .thenCompose(v -> getSealedSegmentSizesMapShardData(shard))) 
                    .thenCompose(mapShardData -> {
                        SealedSegmentsMapShard mapShard = mapShardData.getObject();
                        segments.forEach(z -> mapShard.addSealedSegmentSize(z, sealedSegmentSizes.get(z)));
                        return updateSealedSegmentSizesMapShardData(shard, new VersionedMetadata<>(mapShard, mapShardData.getVersion()));
                    });
        }).collect(Collectors.toList()));
    }

    private int getShardNumber(long segmentId) {
        return NameUtils.getEpoch(segmentId) / shardSize.get();
    }

    private ImmutableMap<StreamSegmentRecord, Integer> convertToSpan(EpochRecord epochRecord) {
        ImmutableMap.Builder<StreamSegmentRecord, Integer> builder = ImmutableMap.builder();
        epochRecord.getSegments().forEach(x -> builder.put(x, epochRecord.getEpoch()));
        return builder.build();
    }

    private Segment transform(StreamSegmentRecord segmentRecord) {
        return new Segment(segmentRecord.segmentId(), segmentRecord.getCreationTime(),
                segmentRecord.getKeyStart(), segmentRecord.getKeyEnd());
    }

    private List<Segment> transform(List<StreamSegmentRecord> segmentRecords) {
        return segmentRecords.stream().map(this::transform).collect(Collectors.toList());
    }
    
    @VisibleForTesting
    CompletableFuture<List<EpochRecord>> fetchEpochs(int fromEpoch, int toEpoch, boolean ignoreCache) {
        // fetch history time series chunk corresponding to from.
        // read entries till either last entry or till to
        // if to is not in this chunk fetch the next chunk and read till to
        // keep doing this until all records till to have been read.
        // keep computing history record from history time series by applying delta on previous.
        return getActiveEpochRecord(ignoreCache)
                .thenApply(currentEpoch -> currentEpoch.getEpoch() / historyChunkSize.get())
                .thenCompose(latestChunkNumber -> Futures.allOfWithResults(
                        IntStream.range(fromEpoch / historyChunkSize.get(), toEpoch / historyChunkSize.get() + 1)
                                 .mapToObj(i -> {
                                     int firstEpoch = i * historyChunkSize.get() > fromEpoch ? i * historyChunkSize.get() : fromEpoch;

                                     boolean ignoreCached = i >= latestChunkNumber;
                                     return getEpochsFromHistoryChunk(i, firstEpoch, toEpoch, ignoreCached);
                                 }).collect(Collectors.toList())))
                .thenApply(c -> c.stream().flatMap(Collection::stream).collect(Collectors.toList()));
    }

    private CompletableFuture<List<EpochRecord>> getEpochsFromHistoryChunk(int chunk, int firstEpoch, int toEpoch, boolean ignoreCached) {
        return getEpochRecord(firstEpoch)
                .thenCompose(first -> getHistoryTimeSeriesChunk(chunk, ignoreCached)
                        .thenCompose(x -> {
                            List<CompletableFuture<EpochRecord>> identity = new ArrayList<>();
                            identity.add(CompletableFuture.completedFuture(first));
                            return Futures.allOfWithResults(x.getHistoryRecords().stream()
                                                             .filter(r -> r.getEpoch() > firstEpoch && r.getEpoch() <= toEpoch)
                                                             .reduce(identity, (r, s) -> {
                                                                 CompletableFuture<EpochRecord> next = newEpochRecord(r.get(r.size() - 1),
                                                                         s.getEpoch(), s.getReferenceEpoch(), s.getSegmentsCreated(),
                                                                         s.getSegmentsSealed().stream().map(StreamSegmentRecord::segmentId)
                                                                          .collect(Collectors.toList()), s.getScaleTime());
                                                                 ArrayList<CompletableFuture<EpochRecord>> list = new ArrayList<>(r);
                                                                 list.add(next);
                                                                 return list;
                                                             }, (r, s) -> {
                                                                 ArrayList<CompletableFuture<EpochRecord>> list = new ArrayList<>(r);
                                                                 list.addAll(s);
                                                                 return list;
                                                             }));
                        }));
    }

    private CompletableFuture<EpochRecord> newEpochRecord(final CompletableFuture<EpochRecord> lastRecordFuture,
                                                          final int epoch, final int referenceEpoch,
                                                          final Collection<StreamSegmentRecord> createdSegments,
                                                          final Collection<Long> sealedSegments, final long time) {
        if (epoch == referenceEpoch) {
            return lastRecordFuture.thenApply(lastRecord -> {
                assert lastRecord.getEpoch() == epoch - 1;
                ImmutableList.Builder<StreamSegmentRecord> segmentsBuilder = ImmutableList.builder();
                lastRecord.getSegments().forEach(segment -> {
                   if (!sealedSegments.contains(segment.segmentId())) {
                       segmentsBuilder.add(segment);
                   }
                });
                segmentsBuilder.addAll(createdSegments);
                return new EpochRecord(epoch, referenceEpoch, segmentsBuilder.build(), time);
            });
        } else {
            return getEpochRecord(epoch);
        }
    }

    private StreamSegmentRecord newSegmentRecord(long segmentId, long time, Double low, Double high) {
        return newSegmentRecord(NameUtils.getEpoch(segmentId), NameUtils.getSegmentNumber(segmentId),
                time, low, high);
    }

    private StreamSegmentRecord newSegmentRecord(int epoch, int segmentNumber, long time, Double low, Double high) {
        return StreamSegmentRecord.builder().creationEpoch(epoch).segmentNumber(segmentNumber).creationTime(time)
                                  .keyStart(low).keyEnd(high).build();
    }

    @VisibleForTesting
    CompletableFuture<Integer> findEpochAtTime(long timestamp, boolean ignoreCached) {
        return getActiveEpoch(ignoreCached)
                .thenCompose(activeEpoch -> searchEpochAtTime(0, activeEpoch.getEpoch() / historyChunkSize.get(),
                        x -> x == activeEpoch.getEpoch() / historyChunkSize.get(), timestamp)
                        .thenApply(epoch -> {
                            if (epoch == -1) {
                                if (timestamp > activeEpoch.getCreationTime()) {
                                    return activeEpoch.getEpoch();
                                } else {
                                    return 0;
                                }

                            } else {
                                return epoch;
                            }
                        }));
    }

    private CompletableFuture<Integer> searchEpochAtTime(int lowest, int highest, Predicate<Integer> ignoreCached, long timestamp) {
        final int middle = (lowest + highest) / 2;

        if (lowest > highest) {
            // either return epoch 0 or latest epoch
            return CompletableFuture.completedFuture(-1);
        }

        return getHistoryTimeSeriesChunk(middle, ignoreCached.test(middle))
                .thenCompose(chunk -> {
                    List<HistoryTimeSeriesRecord> historyRecords = chunk.getHistoryRecords();
                    long rangeLow = historyRecords.get(0).getScaleTime();
                    long rangeHigh = historyRecords.get(historyRecords.size() - 1).getScaleTime();
                    if (timestamp >= rangeLow && timestamp <= rangeHigh) {
                        // found
                        int index = CollectionHelpers.findGreatestLowerBound(historyRecords, x -> Long.compare(timestamp, x.getScaleTime()));
                        assert index >= 0;
                        return CompletableFuture.completedFuture(historyRecords.get(index).getEpoch());
                    } else if (timestamp < rangeLow) {
                        return searchEpochAtTime(lowest, middle - 1, ignoreCached, timestamp);
                    } else {
                        return searchEpochAtTime(middle + 1, highest, ignoreCached, timestamp);
                    }
                });
    }

    @Override
    public CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber) {
        return getHistoryTimeSeriesChunk(chunkNumber, true);
    }
    
    private CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber, boolean ignoreCached) {
        return getHistoryTimeSeriesChunkData(chunkNumber, ignoreCached)
                .thenCompose(x -> {
                    HistoryTimeSeries timeSeries = x.getObject();
                    // we should only retrieve the chunk from cache once the chunk is full to capacity and hence immutable. 
                    if (!ignoreCached && timeSeries.getHistoryRecords().size() < historyChunkSize.get()) {
                        return getHistoryTimeSeriesChunk(chunkNumber, true);
                    }
                    return CompletableFuture.completedFuture(timeSeries);
                });
    }

    // region abstract methods
    //region create delete
    abstract CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration,
                                                                       final long creationTime, final int startingSegmentNumber);

    abstract CompletableFuture<Void> createStreamMetadata();
    
    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<Void> deleteStream();
    // endregion

    // region configuration
    abstract CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord data);

    abstract CompletableFuture<Version> setConfigurationData(final VersionedMetadata<StreamConfigurationRecord> configuration);

    abstract CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached);
    // endregion

    // region truncation
    abstract CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncation);

    abstract CompletableFuture<Version> setTruncationData(final VersionedMetadata<StreamTruncationRecord> truncationRecord);

    abstract CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached);
    // endregion

    // region state
    abstract CompletableFuture<Void> createStateIfAbsent(final StateRecord state);

    // invoke this when creating a new Stream or when moving a Stream to CBR
    abstract CompletableFuture<Void> createSubscribersRecordIfAbsent();

    abstract CompletableFuture<Version> setStateData(final VersionedMetadata<StateRecord> state);

    abstract CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached);
    // endregion

    // region retention
    abstract CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet data);

    abstract CompletableFuture<Void> createStreamCutRecordData(long key, StreamCutRecord record);

    abstract CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime);

    abstract CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime);

    abstract CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> tData);

    abstract CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData();
    // endregion

    // region history
    abstract CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data);

    abstract CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber, boolean ignoreCached);

    abstract CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int historyChunk, VersionedMetadata<HistoryTimeSeries> tData);

    abstract CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data);

    abstract CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> data);

    abstract CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached);

    abstract CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data);

    abstract CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch);

    abstract CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shardNumber, SealedSegmentsMapShard data);

    abstract CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(int shard);

    abstract CompletableFuture<Version> updateSealedSegmentSizesMapShardData(int shard, VersionedMetadata<SealedSegmentsMapShard> data);

    abstract CompletableFuture<Void> createSegmentSealedEpochRecords(Collection<Long> segmentToSeal, int epoch);

    abstract CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId);
    // endregion

    // region transactions
    abstract CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final ActiveTxnRecord record);

    abstract CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(final int epoch, final UUID txId);

    abstract CompletableFuture<Version> updateActiveTx(final int epoch,
                                                       final UUID txId,
                                                       final VersionedMetadata<ActiveTxnRecord> data);

    /**
     * This method is called for adding a transaction to committing order. 
     * It is important to note that this method is called before marking transaction as committing in active txn record. 
     * So retrieving a transaction from this ordered list is no guarantee that the transaction has been set to committing. 
     * Similarly, there could be duplicate entries for the same transaction if retried. 
     * So the ordered list merely captures the order in which a request may have been received and is only opportunistic 
     * and makes no strong claim of its consistency with active txn record. 
     * To summarize some key points:
     *      1. we could have duplicate entries for same transaction in commit order.
     *      2. we could have transactions that are marked for aborting added to commit order
     *      3. we could have transactions that are no longer in activeTxn list yet present in commit order. 
     * @return CompletableFuture which when completed will have transactions added to commit order
    */
    abstract CompletableFuture<Long> addTxnToCommitOrder(final UUID txId);

    abstract CompletableFuture<Void> removeTxnsFromCommitOrder(final List<Long> positions);

    abstract CompletableFuture<VersionedMetadata<CompletedTxnRecord>> getCompletedTx(final UUID txId);

    abstract CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, CompletedTxnRecord data);

    abstract CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnInEpoch(int epoch);

    /**
     * This method finds transactions to commit in lowest epoch and returns a sorted list of transaction ids, 
     * sorted by their order of commits. 
     * @param limit number of txns to fetch.
     * @return CompletableFuture which when completed will return ordered list of transaction ids and records.
     */
    abstract CompletableFuture<List<Map.Entry<UUID, ActiveTxnRecord>>> getOrderedCommittingTxnInLowestEpoch(int limit);
    
    @VisibleForTesting
    abstract CompletableFuture<Map<Long, UUID>> getAllOrderedCommittingTxns();
    // endregion

    // region marker
    abstract CompletableFuture<Void> createMarkerData(long segmentId, long timestamp);

    abstract CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data);

    abstract CompletableFuture<Void> removeMarkerData(long segmentId);

    abstract CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId);
    // endregion

    // region scale
    abstract CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransition);

    abstract CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> epochTransition);

    abstract CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode();
    // endregion

    // region txn commit
    abstract CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns);

    abstract CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord();

    abstract CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> data);
    // endregion

    // region processor
    abstract CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String data);

    abstract CompletableFuture<String> getWaitingRequestNode();

    abstract CompletableFuture<Void> deleteWaitingRequestNode();
    // endregion

    // region watermarking
    /**
     * Method to create new writer mark record. 
     * @param writer writer id
     * @param timestamp timestamp
     * @param position position
     * @return CompletableFuture which when completed will have writer mark created.  
     * Implementation should throw DataExistsException if data exists.
     */
    abstract CompletableFuture<Void> createWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position);

    /**
     * Method to get writer mark record. 
     * @param writer writer id
     * @return CompletableFuture which when completed will contain writer's last reported mark.  
     */
    abstract CompletableFuture<VersionedMetadata<WriterMark>> getWriterMarkRecord(String writer);

    /**
     * Method to update existing writer mark record. 
     * 
     * @param writer writer id
     * @param timestamp timestamp
     * @param position position
     * @param isAlive whether writer is shutdown or not
     * @param version version of last record
     * @return CompletableFuture which when completed will have writer mark updated. 
     * @throws DataNotFoundException if while attempting to update a writer mark, its previous mark was removed by 
     * watermarking workflow.
     */
    abstract CompletableFuture<Void> updateWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position, boolean isAlive, Version version);

    /**
     * Method to delete existing writer mark record conditionally. 
     * 
     * @param writer writer id
     * @param version version of last record
     * @return CompletableFuture which when completed will have writer mark deleted.
     * Can throw Write Conflict if delete version mismatches.
     */
    abstract CompletableFuture<Void> removeWriterRecord(String writer, Version version);
    // endregion
    // endregion
}
