/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CollectionHelpers;
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
import io.pravega.controller.store.stream.records.StreamSubscriber;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.shared.NameUtils;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import lombok.val;
import org.slf4j.LoggerFactory;

import static io.pravega.controller.store.stream.AbstractStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.shared.NameUtils.getSegmentNumber;

public abstract class PersistentStreamBase implements Stream {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PersistentStreamBase.class));

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
    public CompletableFuture<CreateStreamResponse> create(final StreamConfiguration configuration, long createTimestamp,
                                                          int startingSegmentNumber, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        return checkStreamExists(configuration, createTimestamp, startingSegmentNumber, context)
                .thenCompose(createStreamResponse -> createStreamMetadata(context)
                        .thenCompose((Void v) -> storeCreationTimeIfAbsent(createStreamResponse.getTimestamp(), context))
                        .thenCompose((Void v) -> createConfigurationIfAbsent(StreamConfigurationRecord.complete(
                                scope, name, createStreamResponse.getConfiguration()), context))
                        .thenCompose((Void v) -> createEpochTransitionIfAbsent(EpochTransitionRecord.EMPTY, context))
                        .thenCompose((Void v) -> createTruncationDataIfAbsent(StreamTruncationRecord.EMPTY, context))
                        .thenCompose((Void v) -> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord.EMPTY, context))
                        .thenCompose((Void v) -> createStateIfAbsent(StateRecord.builder().state(State.CREATING).build(), context))
                        .thenCompose((Void v) -> createHistoryRecords(startingSegmentNumber, createStreamResponse, context))
                        .thenCompose((Void v) -> createSubscribersRecordIfAbsent(context))
                        .thenApply((Void v) -> createStreamResponse));
    }

    private CompletionStage<Void> createHistoryRecords(int startingSegmentNumber, CreateStreamResponse createStreamResponse,
                                                       OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        final int numSegments = createStreamResponse.getConfiguration().getScalingPolicy().getMinNumSegments();
        // create epoch 0 record
        final double keyRangeChunk = 1.0 / numSegments;

        long creationTime = createStreamResponse.getTimestamp();
        final ImmutableList.Builder<StreamSegmentRecord> builder = ImmutableList.builder();
        
        IntStream.range(0, numSegments).boxed()
                 .forEach(x -> builder.add(newSegmentRecord(0, startingSegmentNumber + x, creationTime,
                                                                    x * keyRangeChunk, (x + 1) * keyRangeChunk)));

        EpochRecord epoch0 = new EpochRecord(0, 0, builder.build(), creationTime, 0L, 0L);

        return createEpochRecord(epoch0, context)
                .thenCompose(r -> createHistoryChunk(epoch0, context))
                .thenCompose(r -> createSealedSegmentSizeMapShardIfAbsent(0, context))
                .thenCompose(r -> createRetentionSetDataIfAbsent(new RetentionSet(ImmutableList.of()), context))
                .thenCompose(r -> createCurrentEpochRecordDataIfAbsent(epoch0, context));
    }

    private CompletionStage<Void> createHistoryChunk(EpochRecord epoch0, OperationContext context) {
        HistoryTimeSeriesRecord record = new HistoryTimeSeriesRecord(0, 0, 
                ImmutableList.of(), epoch0.getSegments(), epoch0.getCreationTime());
        return createHistoryTimeSeriesChunk(0, record, context);
    }

    private CompletableFuture<Void> createHistoryTimeSeriesChunk(int chunkNumber, HistoryTimeSeriesRecord epoch,
                                                                 OperationContext context) {
        ImmutableList.Builder<HistoryTimeSeriesRecord> builder = ImmutableList.builder();
        HistoryTimeSeries timeSeries = new HistoryTimeSeries(builder.add(epoch).build());
        return createHistoryTimeSeriesChunkDataIfAbsent(chunkNumber, timeSeries, context);
    }

    @Override
    public CompletableFuture<Void> delete(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        return deleteStream(context);
    }

    @Override
    public CompletableFuture<Void> startTruncation(final Map<Long, Long> streamCut, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        return getTruncationRecord(context)
                .thenCompose(existing -> {
                    Preconditions.checkNotNull(existing);
                    Preconditions.checkArgument(!existing.getObject().isUpdating());
                    long mostRecent = getMostRecent(streamCut);
                    long oldest = getOldest(streamCut);
                    int epochLow = NameUtils.getEpoch(oldest);
                    int epochHigh = NameUtils.getEpoch(mostRecent);

                    return fetchEpochs(epochLow, epochHigh, true, context)
                        .thenCompose(epochs -> {
                            boolean isValid = isStreamCutValidInternal(streamCut, epochLow, epochs);
                            Exceptions.checkArgument(isValid, "streamCut", "invalid stream cut");
                            ImmutableMap<StreamSegmentRecord, Integer> span = computeStreamCutSpanInternal(streamCut,
                                    epochLow, epochHigh, epochs);
                            StreamTruncationRecord previous = existing.getObject();
                            // check greater than
                            Exceptions.checkArgument(streamCutEqualOrAfter(streamCut, span,
                                    previous.getStreamCut(), previous.getSpan()),
                                    "StreamCut", "Supplied streamcut is behind previous truncation point");

                            return computeTruncationRecord(previous, streamCut, span, context)
                                    .thenCompose(prop ->
                                            Futures.toVoid(setTruncationData(new VersionedMetadata<>(prop, existing.getVersion()),
                                                    context)));
                        });
                });
    }

    private boolean streamCutEqualOrAfter(Map<Long, Long> cut1, Map<StreamSegmentRecord, Integer> span1,
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
                                                                              ImmutableMap<StreamSegmentRecord, Integer> span,
                                                                              OperationContext context) {
        log.debug(context.getRequestId(), "computing truncation for stream {}/{}", scope, name);

        // find segments between "previous" stream cut and current stream cut. these are segments to delete.
        // Note: exclude segments in current streamcut
        CompletableFuture<Map<StreamSegmentRecord, Integer>> previousSpanFuture = previous.getSpan().isEmpty() ?
                getEpochRecord(0, context).thenApply(epoch -> convertToSpan(epoch))
                : CompletableFuture.completedFuture(previous.getSpan());

        return previousSpanFuture.thenCompose(spanFrom -> segmentsBetweenStreamCutSpans(spanFrom, span, context))
                                 .thenCompose(segmentsBetween -> sizeBetweenStreamCuts(previous.getStreamCut(),
                                         streamCut, segmentsBetween, context)
                                         .thenApply(sizeBetween -> {
                                             ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
                                             segmentsBetween.stream().map(StreamSegmentRecord::segmentId)
                                                            .filter(x -> !streamCut.containsKey(x))
                                                            .forEach(builder::add);

                                             return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), span,
                                                     previous.getDeletedSegments(),
                                                     builder.build(),
                                                     previous.getSizeTill() + sizeBetween, true);
                                         }));
    }
    
    @Override
    public CompletableFuture<Void> completeTruncation(VersionedMetadata<StreamTruncationRecord> record, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        Preconditions.checkNotNull(record);
        Preconditions.checkArgument(record.getObject().isUpdating());
        StreamTruncationRecord current = record.getObject();
        if (current.isUpdating()) {
            StreamTruncationRecord completedProp = StreamTruncationRecord.complete(current);

            return Futures.toVoid(setTruncationData(new VersionedMetadata<>(completedProp, record.getVersion()), context));
        } else {
            // idempotent
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationRecord(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");
        return getTruncationData(true, context)
                .thenApply(data -> {
                    StreamTruncationRecord truncationRecord = data.getObject();
                    return new VersionedMetadata<>(truncationRecord, data.getVersion());
                });
    }

    @Override
    public CompletableFuture<StreamCutComparison> compareStreamCuts(Map<Long, Long> streamcut1, Map<Long, Long> streamcut2,
                                                                    OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        LongSummaryStatistics stats1 = streamcut1.keySet().stream().collect(Collectors.summarizingLong(Long::longValue));
        LongSummaryStatistics stats2 = streamcut1.keySet().stream().collect(Collectors.summarizingLong(Long::longValue));

        // if all segments in one streamcut are older than all segments another streamcut then we dont need to compare 
        // streamcuts at all and can simply return the response. 
        if (stats1.getMax() < stats2.getMin()) {
            // stats1 less than stats2
            return CompletableFuture.completedFuture(StreamCutComparison.Before);
        } else  if (stats2.getMax() < stats1.getMin()) {
            // stats2 less than min
            return CompletableFuture.completedFuture(StreamCutComparison.EqualOrAfter);
        } 

        CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> span1Future = computeStreamCutSpan(streamcut1, context);
        CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> span2Future = computeStreamCutSpan(streamcut2, context);
        return CompletableFuture.allOf(span1Future, span2Future)
                    .thenApply(v -> {
                        ImmutableMap<StreamSegmentRecord, Integer> span1 = span1Future.join();
                        ImmutableMap<StreamSegmentRecord, Integer> span2 = span2Future.join();
                        // loop over all segments in streamcut1 and compare them with segments in streamcut2. 
                        // if we find all segments in streamcut1 greater than or equal to all segments in streamcut2
                        boolean foundGt = false;
                        boolean foundLt = false;
                        for (Map.Entry<StreamSegmentRecord, Integer> e1 : span1.entrySet()) {
                            for (Map.Entry<StreamSegmentRecord, Integer> e2 : span2.entrySet()) {
                                int comparison;
                                if (e2.getKey().segmentId() == e1.getKey().segmentId()) {
                                    // same segment. compare offsets
                                    comparison = Long.compare(streamcut1.get(e1.getKey().segmentId()), streamcut2.get(e2.getKey().segmentId()));
                                } else if (e2.getKey().overlaps(e1.getKey())) {
                                    // overlapping segment. compare segment id.
                                    comparison = Long.compare(e1.getKey().segmentId(), e2.getKey().segmentId());
                                } else {
                                    continue;
                                }
                                foundGt = !foundGt ? comparison > 0 : foundGt;
                                foundLt = !foundLt ? comparison < 0 : foundLt;
                            }
                        }

                        if (foundGt) {
                            if (foundLt) { // some segments are greater and some less. return overlapping
                                return StreamCutComparison.Overlaps;
                            } else { // segments are only greater or equal. 
                                return StreamCutComparison.EqualOrAfter;
                            }
                        } else { 
                            if (foundLt) { // no segment greater than but some segment less than. 
                                return StreamCutComparison.Before;
                            } else { 
                                // no segment greater than no segment less than. this means all segments are equal.
                                return StreamCutComparison.EqualOrAfter;
                            }
                        }
                    });
    }

    @Override
    public CompletableFuture<StreamCutReferenceRecord> findStreamCutReferenceRecordBefore(Map<Long, Long> streamCut,
                                                                                          RetentionSet retentionSet,
                                                                                          OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        Map<Set<Long>, ImmutableMap<StreamSegmentRecord, Integer>> fetched = new HashMap<>();
        int size = retentionSet.getRetentionRecords().size();

        if (retentionSet.getRetentionRecords().isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return computeStreamCutSpan(streamCut, context)
                .thenCompose(span1 -> {
                    fetched.put(streamCut.keySet(), span1);
                    Function<StreamCutReferenceRecord, CompletableFuture<Integer>> fn
                            = refRecord -> getStreamCutRecord(refRecord, context)
                            .thenCompose(record -> {
                                ImmutableMap<StreamSegmentRecord, Integer> sc = fetched.get(record.getStreamCut().keySet());
                                CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> future;
                                if (sc != null) {
                                    future = CompletableFuture.completedFuture(sc);
                                } else {
                                    future = computeStreamCutSpan(record.getStreamCut(), context)
                                            .thenApply(span2 -> {
                                                fetched.put(record.getStreamCut().keySet(), span2);
                                                return span2;
                                            });
                                }
                                return future.thenApply(span2 -> {
                                    boolean compare = streamCutEqualOrAfter(streamCut, span1, record.getStreamCut(), span2);
                                    if (compare) {
                                        return 1;
                                    } else {
                                        return -1;
                                    }
                                });
                            });

                    // binary search retention set. 
                    return binarySearch(0, size - 1, index -> {
                        StreamCutReferenceRecord refRecord = retentionSet.getRetentionRecords().get(index);
                        return fn.apply(refRecord)
                                 .thenCompose(compared -> {
                                     if (compared == 1) {
                                       if (index < size - 1) {
                                         StreamCutReferenceRecord next = retentionSet.getRetentionRecords().get(index + 1);
                                         // if its greater than current but not strictly greater than next then return found.
                                         return fn.apply(next).thenApply(r -> r == 1 ? 1 : 0);
                                       } else {
                                         // if its greater than the latest element, then return matched
                                         return CompletableFuture.completedFuture(0);
                                       }
                                     } else {
                                         return CompletableFuture.completedFuture(compared);
                                     }
                                 });
                    });
                }).thenApply(refIndex -> {
                    if (refIndex == null) {
                        return null;
                    } else {
                        return retentionSet.getRetentionRecords().get(refIndex);
                    }
                });
    }

    private CompletableFuture<Integer> binarySearch(int start, int end, Function<Integer, CompletableFuture<Integer>> find) {
        if (end >= start) {
            int mid = (end + start) / 2;
            return find.apply(mid)
                    .thenCompose(found -> {
                        // if its greater than 
                        if (found < 0) {
                            return binarySearch(start, mid -1, find);
                        } else if (found == 0) {
                            return CompletableFuture.completedFuture(mid);
                        } else {
                            return binarySearch(mid + 1, end, find);
                        }
                    });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
    
    /**
     * Update configuration at configurationPath.
     *
     * @param newConfiguration new stream configuration.
     * @return future of operation.
     */
    @Override
    public CompletableFuture<Void> startUpdateConfiguration(final StreamConfiguration newConfiguration, OperationContext context) {
        return getVersionedConfigurationRecord(context)
                .thenCompose(configRecord -> {
                    Preconditions.checkArgument(!configRecord.getObject().isUpdating());
                    StreamConfiguration oldCfg = configRecord.getObject().getStreamConfiguration();
                    // Check for tags that needs to be removed.
                    Set<String> currentTags = new HashSet<>(oldCfg.getTags());
                    currentTags.removeAll(newConfiguration.getTags());
                    // Create a StreamConfigurationRecord with the newer Configuration.
                    StreamConfigurationRecord update;
                    if (StreamConfiguration.isTagOnlyChange(oldCfg, newConfiguration)) {
                        update = StreamConfigurationRecord.updateTag(scope, name, newConfiguration, currentTags);
                    } else {
                        update = StreamConfigurationRecord.update(scope, name, newConfiguration, currentTags);
                    }
                    // Update this in the metadata Stoe.
                    return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(update, configRecord.getVersion()), context));
                });
    }

    /**
     * Update configuration at configurationPath.
     *
     * @return future of operation
     */
    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<StreamConfigurationRecord> existing,
                                                               OperationContext context) {
        StreamConfigurationRecord current = existing.getObject();
        Preconditions.checkNotNull(current);
        if (current.isUpdating()) {
            StreamConfigurationRecord newProperty = StreamConfigurationRecord.complete(scope, name, current.getStreamConfiguration());
            log.debug(context.getRequestId(), "Completing update configuration for stream {}/{}", scope, name);
            return Futures.toVoid(setConfigurationData(new VersionedMetadata<>(newProperty, existing.getVersion()), context));
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
    public CompletableFuture<StreamConfiguration> getConfiguration(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getConfigurationData(false, context).thenApply(x -> x.getObject().getStreamConfiguration());
    }

    @Override
    public CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getVersionedConfigurationRecord(
            OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getConfigurationData(true, context)
                .thenApply(data -> new VersionedMetadata<>(data.getObject(), data.getVersion()));
    }

    @Override
    public CompletableFuture<Void> updateState(final State state, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStateData(true, context)
                .thenCompose(currState -> {
                    VersionedMetadata<State> currentState = new VersionedMetadata<State>(currState.getObject().getState(),
                            currState.getVersion());
                    return Futures.toVoid(updateVersionedState(currentState, state, context));
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<State>> getVersionedState(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStateData(true, context)
                .thenApply(x -> new VersionedMetadata<>(x.getObject().getState(), x.getVersion()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<State>> updateVersionedState(final VersionedMetadata<State> previous,
                                                                            final State newState,
                                                                            OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        if (State.isTransitionAllowed(previous.getObject(), newState)) {
            return setStateData(new VersionedMetadata<>(StateRecord.builder().state(newState).build(),
                    previous.getVersion()), context)
                    .thenApply(updatedVersion -> new VersionedMetadata<>(newState, updatedVersion));
        } else {
            return Futures.failedFuture(StoreException.create(
                    StoreException.Type.OPERATION_NOT_ALLOWED,
                    "Stream: " + getName() + " State: " + newState.name() + " current state = " +
                            previous.getObject()));
        }
    }

    @Override
    public CompletableFuture<Void> updateSubscriberStreamCut(final VersionedMetadata<StreamSubscriber> previous,
                                                             final String subscriber, long generation,
                                                             ImmutableMap<Long, Long> streamCut,
                                                             OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return isStreamCutValidForTruncation(previous.getObject().getTruncationStreamCut(), streamCut, context)
                .thenCompose(isValid -> {
                    if (isValid) {
                       return Futures.toVoid(setSubscriberData(new VersionedMetadata<>(new StreamSubscriber(subscriber, generation,
                                                                                            streamCut, System.currentTimeMillis()),
                                                                                            previous.getVersion()), context));
                    } else {
                        return Futures.failedFuture(StoreException.create(StoreException.Type.OPERATION_NOT_ALLOWED,
                                "New StreamCut is lower than the previous value."));
                    }
               });
    }

    @Override
    public CompletableFuture<State> getState(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStateData(ignoreCached, context)
                .thenApply(x -> x.getObject().getState());
    }

    /**
     * Fetches Segment metadata from the epoch in which segment was created.
     *
     * @param segmentId segment id.
     * @return : Future, which when complete contains segment object
     */
    @Override
    public CompletableFuture<StreamSegmentRecord> getSegment(final long segmentId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // extract epoch from segment id.
        // fetch epoch record for the said epoch
        // extract segment record from it.
        int epoch = NameUtils.getEpoch(segmentId);
        return getEpochRecord(epoch, context)
                .thenApply(epochRecord -> {
                    Optional<StreamSegmentRecord> segmentRecord = epochRecord.getSegments().stream()
                                                                             .filter(x -> x.segmentId() == segmentId).findAny();
                    return segmentRecord
                            .orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                                    "segment not found in epoch"));
                });
    }

    @Override
    public CompletableFuture<List<ScaleMetadata>> getScaleMetadata(final long from, final long to, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // fetch history index and find epochs corresponding to "from" and "to"
        // fetch "from epoch" from epoch record
        // fetch epochs from history timeseries.
        CompletableFuture<Integer> fromEpoch = findEpochAtTime(from, false, context);
        CompletableFuture<Integer> toEpoch = findEpochAtTime(to, false, context);
        CompletableFuture<List<EpochRecord>> records =
                CompletableFuture.allOf(fromEpoch, toEpoch)
                                 .thenCompose(x -> {
                                     // fetch epochs will fetch it from history time series. 
                                     // this will be efficient if fromEpoch and toEpoch are near each other.
                                     return fetchEpochs(fromEpoch.join(), toEpoch.join(), false, context);
                                 });
        return records.thenApply(this::mapToScaleMetadata);
    }

    private List<ScaleMetadata> mapToScaleMetadata(List<EpochRecord> epochRecords) {
        final AtomicReference<ImmutableList<StreamSegmentRecord>> previous = new AtomicReference<>();
        return epochRecords.stream()
                           .map(record -> {
                               long splits = 0;
                               long merges = 0;
                               ImmutableList<StreamSegmentRecord> segments = record.getSegments();
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
    private long findSegmentSplitsMerges(List<StreamSegmentRecord> referenceSegmentsList,
                                         List<StreamSegmentRecord> targetSegmentsList) {
        return referenceSegmentsList.stream().filter(
                segment -> targetSegmentsList.stream().filter(target -> target.overlaps(segment)).count() > 1 ).count();
    }

    @Override
    public CompletableFuture<Integer> getSegmentSealedEpoch(long segmentId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getSegmentSealedRecordData(segmentId, context).handle((x, e) -> {
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
    public CompletableFuture<Set<Long>> getAllSegmentIds(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        CompletableFuture<Map<StreamSegmentRecord, Integer>> fromSpanFuture = getTruncationRecord(context)
                .thenCompose(truncationRecord -> {
                    if (truncationRecord.getObject().equals(StreamTruncationRecord.EMPTY)) {
                        return getEpochRecord(0, context)
                                .thenApply(this::convertToSpan);
                    } else {
                        return CompletableFuture.completedFuture(truncationRecord.getObject().getSpan());
                    }
                });
        CompletableFuture<Map<StreamSegmentRecord, Integer>> toSpanFuture = getActiveEpoch(true, context)
                .thenApply(this::convertToSpan);

        return CompletableFuture.allOf(fromSpanFuture, toSpanFuture)
                                .thenCompose(v -> {
                                    Map<StreamSegmentRecord, Integer> fromSpan = fromSpanFuture.join();
                                    Map<StreamSegmentRecord, Integer> toSpan = toSpanFuture.join();
                                    return segmentsBetweenStreamCutSpans(fromSpan, toSpan, context)
                                            .thenApply(x -> x.stream().map(StreamSegmentRecord::segmentId)
                                                             .collect(Collectors.toSet()));
                                });
    }

    @Override
    public CompletableFuture<Map<StreamSegmentRecord, List<Long>>> getSuccessorsWithPredecessors(final long segmentId,
                                                                                                 OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // fetch segment sealed epoch record.
        return getSegmentSealedEpoch(segmentId, context)
                .thenCompose(sealedEpoch -> {
                    if (sealedEpoch < 0) {
                        return getActiveEpoch(true, context).thenApply(activeSegments -> Collections.emptyMap());
                    }

                    // if sealed record exists. fetch its sealing epoch.
                    // Note: sealed record is created even before the segment is sealed. So if client is requesting for successor,
                    // we should find it.
                    CompletableFuture<EpochRecord> sealedEpochFuture = getEpochRecord(sealedEpoch, context);

                    // fetch previous epoch as well.
                    CompletableFuture<EpochRecord> previousEpochFuture = getEpochRecord(sealedEpoch - 1, context);

                    return CompletableFuture.allOf(sealedEpochFuture, previousEpochFuture)
                            .thenApply(x -> {
                                EpochRecord sealedEpochRecord = sealedEpochFuture.join();
                                EpochRecord previousEpochRecord = previousEpochFuture.join();
                                Optional<StreamSegmentRecord> segmentOpt = previousEpochRecord
                                        .getSegments().stream().filter(r -> r.segmentId() == segmentId).findAny();
                                assert segmentOpt.isPresent();
                                StreamSegmentRecord segment = segmentOpt.get();

                                List<StreamSegmentRecord> successors = sealedEpochRecord
                                        .getSegments().stream().filter(r -> r.overlaps(segment)).collect(Collectors.toList());

                                return successors
                                        .stream().collect(Collectors.toMap(record -> record,
                                                z -> previousEpochRecord
                                                        .getSegments()
                                                        .stream().filter(predecessor -> predecessor.overlaps(z))
                                                        .map(StreamSegmentRecord::segmentId).collect(Collectors.toList())));
                            });
                });
    }

    private CompletableFuture<EpochRecord> getActiveEpochRecord(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getCurrentEpochRecordData(ignoreCached, context).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getActiveSegments(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // read current epoch record
        return verifyLegalState(context)
                .thenCompose(v -> getActiveEpochRecord(true, context).thenApply(EpochRecord::getSegments));
    }

    @Override
    public CompletableFuture<Map<StreamSegmentRecord, Long>> getSegmentsAtHead(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // read current epoch record
        return getTruncationRecord(context)
                .thenCompose(truncationRecord -> {
                    if (truncationRecord.getObject().equals(StreamTruncationRecord.EMPTY)) {
                        return getSegmentsInEpoch(0, context)
                                .thenApply(segments -> segments.stream().collect(Collectors.toMap(x -> x, x ->  0L)));
                    } else {
                        return CompletableFuture.completedFuture(truncationRecord.getObject().getStreamCut().entrySet()
                                                                                 .stream().collect(Collectors.toMap(x ->
                                                truncationRecord.getObject().getSpan().keySet().stream()
                                                                          .filter(y -> y.segmentId() == x.getKey())
                                                                .findFirst().get(),
                                        Map.Entry::getValue)));
                    }
                });
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(final int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getEpochRecord(epoch, context)
                .thenApply(EpochRecord::getSegments);
    }

    @Override
    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsBetweenStreamCuts(Map<Long, Long> from, Map<Long, Long> to,
                                                                                     OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return segmentsBetweenStreamCuts(from, to, context).thenApply(ArrayList::new);
    }

    private CompletableFuture<ImmutableSet<StreamSegmentRecord>> segmentsBetweenStreamCuts(Map<Long, Long> from,
                                                                                           Map<Long, Long> to,
                                                                                           OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // compute stream cut span for `from` till `to`
        // if from is empty we need to start from epoch 0.
        // if to is empty we need to go on till current epoch.
        CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> spanFromFuture = from.isEmpty() ?
                getEpochRecord(0, context).thenApply(this::convertToSpan)
                : computeStreamCutSpan(from, context);
        CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> spanToFuture = to.isEmpty() ?
                getActiveEpochRecord(true, context).thenApply(this::convertToSpan)
                : computeStreamCutSpan(to, context);

        return CompletableFuture.allOf(spanFromFuture, spanToFuture)
                                .thenCompose(x -> {
                                    if (!from.isEmpty() && !to.isEmpty()) {
                                        Preconditions.checkArgument(RecordHelper.streamCutComparator(to, spanToFuture.join(),
                                                from, spanFromFuture.join()));
                                    }
                                    return segmentsBetweenStreamCutSpans(spanFromFuture.join(), spanToFuture.join(), context);
                                });
    }

    @VisibleForTesting
    CompletableFuture<ImmutableSet<StreamSegmentRecord>> segmentsBetweenStreamCutSpans(Map<StreamSegmentRecord, Integer> spanFrom,
                                                                                      Map<StreamSegmentRecord, Integer> spanTo,
                                                                                       OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        int toLow = Collections.min(spanTo.values());
        int toHigh = Collections.max(spanTo.values());
        int fromLow = Collections.min(spanFrom.values());
        int fromHigh = Collections.max(spanFrom.values());
        Set<StreamSegmentRecord> segments = new HashSet<>();

        return fetchEpochs(fromLow, toHigh, true, context)
                .thenAccept(epochs -> {
                    epochs.forEach(epoch -> {
                        // for epochs that cleanly lie between from.high and to.low epochs we can include all
                        // segments present in them
                        // because they are guaranteed to be greater than `from` and less than `to` stream cuts.
                        if (epoch.getEpoch() >= fromHigh && epoch.getEpoch() <= toLow) {
                            segments.addAll(epoch.getSegments());
                        } else {
                            // for each segment in epoch.segments, find overlaps in from and to
                            epoch.getSegments().stream().filter(x -> !segments.contains(x)).forEach(segment -> {
                                // if segment.number >= from.segmentNumber && segment.number <= to.segmentNumber
                                // include segment.number
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
                                                          Set<StreamSegmentRecord> segmentsInBetween, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        Map<Integer, List<StreamSegmentRecord>> shards =
                segmentsInBetween.stream().collect(Collectors.groupingBy(x -> getShardNumber(x.segmentId())));
        return Futures.allOfWithResults(
                shards.entrySet().stream()
                      .map(entry -> getSealedSegmentSizeMapShard(entry.getKey(), context)
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
                                  long sizeFrom = Math.max(streamCutTo.get(segment.segmentId()), 0);
                                  long sizeTo = Math.max(streamCutFrom.get(segment.segmentId()), 0);
                                  sizeTill.addAndGet(sizeFrom - sizeTo);
                              } else if (streamCutTo.containsKey(segment.segmentId())) {
                                  long sizeFrom = Math.max(streamCutTo.get(segment.segmentId()), 0);

                                  // segments only in streamcutTo: take their offsets in streamcut
                                  sizeTill.addAndGet(sizeFrom);
                              } else if (streamCutFrom.containsKey(segment.segmentId())) {
                                  // segments only in from: take their total size - offset in from
                                  long sizeTo = Math.max(streamCutFrom.get(segment.segmentId()), 0);
                                
                                  sizeTill.addAndGet(Math.max(value, 0) - sizeTo);
                              } else {
                                  sizeTill.addAndGet(Math.max(value, 0));
                              }
                          });
                          return sizeTill.get();
                      });
    }

    @VisibleForTesting
    CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> computeStreamCutSpan(Map<Long, Long> streamCut,
                                                                                       OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        long mostRecent = getMostRecent(streamCut);
        long oldest = getOldest(streamCut);
        int epochLow = NameUtils.getEpoch(oldest);
        int epochHigh = NameUtils.getEpoch(mostRecent);

        return fetchEpochs(epochLow, epochHigh, true, context).thenApply(epochs ->  {
            return computeStreamCutSpanInternal(streamCut, epochLow, epochHigh, epochs);
        });
    }

    private Long getMostRecent(Map<Long, Long> streamCut) {
        return streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
    }

    private Long getOldest(Map<Long, Long> streamCut) {
        return streamCut.keySet().stream().min(Comparator.naturalOrder()).get();
    }

    private ImmutableMap<StreamSegmentRecord, Integer> computeStreamCutSpanInternal(Map<Long, Long> streamCut, int epochLow, 
                                                                                    int epochHigh, List<EpochRecord> epochs) {
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
    }

    @Override
    public CompletableFuture<Boolean> isStreamCutValid(Map<Long, Long> streamCut, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        long mostRecent = getMostRecent(streamCut);
        long oldest = getOldest(streamCut);
        int epochLow = NameUtils.getEpoch(oldest);
        int epochHigh = NameUtils.getEpoch(mostRecent);

        return fetchEpochs(epochLow, epochHigh, true, context).thenApply(epochs -> isStreamCutValidInternal(
                streamCut, epochLow, epochs));
    }

    private boolean isStreamCutValidInternal(Map<Long, Long> streamCut, int epochLow, List<EpochRecord> epochs) {
        Set<StreamSegmentRecord> segmentsInStreamCut = new HashSet<>();
        Set<StreamSegmentRecord> futureSegment = new HashSet<>();

        boolean isValid = true;
        // for each segment get its epoch and the segment record
        streamCut.forEach((key, value) -> {
            int epoch = NameUtils.getEpoch(key);
            int index = epoch - epochLow;
            EpochRecord epochRecord = epochs.get(index);
            StreamSegmentRecord segmentRecord = epochRecord.getSegment(key);
            if (value < 0) {
                futureSegment.add(segmentRecord);
            } else {
                segmentsInStreamCut.add(segmentRecord);
            }
        });

        isValid = futureSegment.stream().allMatch(x -> 
                segmentsInStreamCut.stream().filter(y -> y.overlaps(x)).allMatch(y -> y.segmentId() < x.segmentId()));

        if (isValid) {
            List<StreamSegmentRecord> sorted = segmentsInStreamCut
                    .stream().sorted(Comparator.comparingDouble(StreamSegmentRecord::getKeyStart)).collect(Collectors.toList());

            // all future segments should have a predecessor and all missing ranges should be covered by a future segment. 
            Map<Double, Double> missingRanges = new HashMap<>();

            StreamSegmentRecord previous = sorted.get(0);
            BiFunction<Double, Double, Boolean> validate = (start, end) -> futureSegment
                    .stream().anyMatch(x -> x.overlaps(start, end));

            if (previous.getKeyStart() > 0.0) {
                double start = 0.0;
                double end = previous.getKeyStart();
                missingRanges.put(start, end);
                // missing range should be covered by a future segment
                isValid = validate.apply(start, end);
            }

            for (int i = 1; i < sorted.size(); i++) {
                StreamSegmentRecord next = sorted.get(i);
                if (previous.getKeyEnd() < next.getKeyStart()) {
                    double start = previous.getKeyEnd();
                    double end = next.getKeyStart();
                    missingRanges.put(start, end);
                    //  missing range should be covered by a future segment
                    isValid = validate.apply(start, end);
                    if (!isValid) {
                        break;
                    }
                } else if (previous.getKeyEnd() > next.getKeyStart()) {
                    isValid = false;
                    break;
                }
                previous = next;
            }

            if (previous.getKeyEnd() < 1.0) {
                double start = previous.getKeyEnd();
                double end = 1.0;
                missingRanges.put(start, end);
                isValid = validate.apply(start, end);
            }

            if (isValid) {
                List<StreamSegmentRecord> toCheck = new ArrayList<>();
                Set<StreamSegmentRecord> fullyReadFrom = new HashSet<>();

                // now traverse the stream for missing ranges and verify that we can reach those future segments 
                // in logically consistent fashion for the missing ranges. 
                missingRanges.entrySet().forEach(x -> toCheck.addAll(findSegmentsForMissingRange(epochs.get(0), x)));
                while (!toCheck.isEmpty()) {
                    StreamSegmentRecord segmentRecord = toCheck.get(0);
                    if (!(fullyReadFrom.contains(segmentRecord) || segmentsInStreamCut.contains(segmentRecord) ||
                            futureSegment.contains(segmentRecord))) {
                        for (StreamSegmentRecord s : segmentsInStreamCut) {
                            if (s.overlaps(segmentRecord)) {
                                if (s.segmentId() < segmentRecord.segmentId()) {
                                    // if segment record has a predecessor, then it should have been in future segment.  
                                    if (!futureSegment.contains(segmentRecord)) {
                                        return false;
                                    } else {
                                        // segment record is a predecessor of a previous segment. 
                                        fullyReadFrom.add(segmentRecord);
                                    }
                                } else {
                                    // if segment is predecessor of another segment in the stream cut then it has to be 
                                    // fully read 
                                    fullyReadFrom.add(segmentRecord);
                                    // find successors of segmentRecord and add it to tocheck list
                                    int segmentEpoch = NameUtils.getEpoch(segmentRecord.segmentId());
                                    int index = segmentEpoch - epochLow;
                                    for (int i = index; i < epochs.size(); i++) {
                                        if (!epochs.get(i).containsSegment(segmentRecord.segmentId())) {
                                            epochs.get(i).getSegments().forEach(x -> {
                                                if (x.overlaps(segmentRecord)) {
                                                    toCheck.add(x);
                                                }
                                            });
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    toCheck.remove(segmentRecord);
                }
            }
        }
        return isValid;
    }
    
    private List<StreamSegmentRecord> findSegmentsForMissingRange(EpochRecord epochRecord, Map.Entry<Double, Double> missingRange) {
        return epochRecord.getSegments().stream().filter(x -> x.overlaps(missingRange.getKey(), missingRange.getValue()))
                          .collect(Collectors.toList());
    }

    private CompletableFuture<Boolean> isStreamCutValidForTruncation(Map<Long, Long> previousStreamCut,
                                                                     final Map<Long, Long> streamCut,
                                                                     OperationContext context) {
        if (previousStreamCut.isEmpty()) {
            return isStreamCutValid(streamCut, context);
        } else {
            return isStreamCutValid(streamCut, context)
                    .thenCompose(isValidStreamCut -> {
                        if (isValidStreamCut) {
                            CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> span1 =
                                    computeStreamCutSpan(streamCut, context);
                            CompletableFuture<ImmutableMap<StreamSegmentRecord, Integer>> span2 = computeStreamCutSpan(
                                    previousStreamCut, context);
                            return CompletableFuture.allOf(span1, span2)
                                                    .thenApply(v -> {
                                                        ImmutableMap<StreamSegmentRecord, Integer> span = span1.join();
                                                        ImmutableMap<StreamSegmentRecord, Integer> previousSpan = span2.join();

                                                        return streamCutEqualOrAfter(streamCut, span, previousStreamCut,
                                                                previousSpan);
                                                    });
                        } else {
                            return CompletableFuture.completedFuture(false);
                        }
                    });
        }
    }
    
    /**
     * This method attempts to start a new scale workflow. For this it first computes epoch transition and stores it
     * in the metadastore.
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
                                                                                   final VersionedMetadata<EpochTransitionRecord> existing,
                                                                                   OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return verifyNotSealed(context).thenCompose(v -> {
            if (existing == null) {
                return getEpochTransition(context);
            } else {
                return CompletableFuture.completedFuture(existing);
            }
        }).thenCompose(record -> getActiveEpochRecord(true, context).thenCompose(currentEpoch ->
                getConfiguration(context).thenCompose(config -> {
            if (!record.getObject().equals(EpochTransitionRecord.EMPTY)) {
                // verify that it's the same as the supplied input (--> segments to be sealed
                // and new ranges are identical). else throw scale conflict exception
                if (!RecordHelper.verifyRecordMatchesInput(segmentsToSeal, newRanges, false, record.getObject())) {
                    log.debug(context.getRequestId(), "scale conflict, another scale operation is ongoing");
                    throw new EpochTransitionOperationExceptions.ConflictException();
                }
                return CompletableFuture.completedFuture(record);
            } else {
                // check input is valid and satisfies preconditions
                if (!RecordHelper.canScaleFor(segmentsToSeal, currentEpoch)) {
                    return updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, record.getVersion()),
                            context)
                            .thenApply(x -> {
                                log.warn(context.getRequestId(), "scale precondition failed {}", segmentsToSeal);
                                throw new EpochTransitionOperationExceptions.PreConditionFailureException();
                            });
                }
                if (!RecordHelper.validateInputRange(segmentsToSeal, newRanges, currentEpoch)) {
                    log.error(context.getRequestId(), "scale input invalid {} {}", segmentsToSeal, newRanges);
                    throw new EpochTransitionOperationExceptions.InputInvalidException();
                }

                int numberOfSegmentsPostScale = currentEpoch.getSegments().size() - segmentsToSeal.size() + newRanges.size();
                if (numberOfSegmentsPostScale < config.getScalingPolicy().getMinNumSegments()) {
                    log.warn(context.getRequestId(),
                            "Scale cannot be performed as Min Segment Count will not hold {} {}",
                            segmentsToSeal, newRanges);
                    throw new EpochTransitionOperationExceptions.PreConditionFailureException();
                }
                
                EpochTransitionRecord epochTransition = RecordHelper.computeEpochTransition(
                        currentEpoch, segmentsToSeal, newRanges, scaleTimestamp);

                return updateEpochTransitionNode(new VersionedMetadata<>(epochTransition, record.getVersion()), context)
                        .thenApply(version -> {
                            log.info(context.getRequestId(), "scale for stream {}/{} accepted. Segments to seal = {}",
                                    scope, name, epochTransition.getSegmentsToSeal());
                            return new VersionedMetadata<>(epochTransition, version);
                        });
            }
        })));
    }

    private CompletableFuture<Void> verifyNotSealed(OperationContext context) {
        return getState(false, context).thenAccept(state -> {
            if (state.equals(State.SEALING) || state.equals(State.SEALED)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
        });
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> startScale(boolean isManualScale,
                                                                                  VersionedMetadata<EpochTransitionRecord> record,
                                                                                  VersionedMetadata<State> state,
                                                                                  OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        Preconditions.checkArgument(state.getObject().equals(State.SCALING));
        return getCurrentEpochRecordData(true, context)
                .thenCompose(currentEpoch -> {
                    EpochRecord currentEpochRecord = currentEpoch.getObject();
                    if (isManualScale) {
                        return migrateManualScaleToNewEpoch(record, state, currentEpochRecord, context);
                    } else {
                        // if rolling transactions happened before the scale could be picked up, we will discard
                        // epoch transition record and reset the state to active.
                        return discardInconsistentEpochTransition(record, state, currentEpochRecord, context);
                    }
                });
    }


    private CompletableFuture<VersionedMetadata<EpochTransitionRecord>> discardInconsistentEpochTransition(
            VersionedMetadata<EpochTransitionRecord> epochTransition, VersionedMetadata<State> state,
            EpochRecord currentEpoch, OperationContext context) {
        if (epochTransition.getObject().getNewEpoch() > currentEpoch.getEpoch()) {
            return CompletableFuture.completedFuture(epochTransition);
        } else {
            return updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, epochTransition.getVersion()),
                    context)
                    .thenCompose(v -> updateVersionedState(state, State.ACTIVE, context))
                    .thenApply(v -> {
                        log.warn(context.getRequestId(),
                                "Scale epoch transition record is inconsistent with VersionedMetadata in the table. {}",
                                epochTransition.getObject().getNewEpoch());
                        throw new IllegalStateException("Epoch transition record is inconsistent.");
                    });
        }
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> scaleCreateNewEpoch(
            VersionedMetadata<EpochTransitionRecord> versionedMetadata, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getActiveEpochRecord(true, context)
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
                        ImmutableList<StreamSegmentRecord> newEpochSegments = builder.build();
                        // epoch record
                        return getSplitMergeCountsTillEpoch(currentEpoch, context).thenCompose(cumulativeSplitMergeCount -> {
                            EpochRecord epochRecord = new EpochRecord(epochTransition.getNewEpoch(),
                                    epochTransition.getNewEpoch(), newEpochSegments, time,
                                    getNewEpochSplitCount(cumulativeSplitMergeCount.getKey(),
                                            currentEpoch.getSegments(), newEpochSegments),
                                    getNewEpochMergeCount(cumulativeSplitMergeCount.getValue(),
                                            currentEpoch.getSegments(), newEpochSegments));
                            HistoryTimeSeriesRecord timeSeriesRecord =
                                    new HistoryTimeSeriesRecord(epochTransition.getNewEpoch(), epochTransition.getNewEpoch(),
                                            sealedSegmentsBuilder.build(), newSegments, epochRecord.getCreationTime());
                            return createEpochRecord(epochRecord, context)
                                    .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecord, context))
                                    .thenCompose(x -> createSegmentSealedEpochRecords(epochTransition.getSegmentsToSeal(),
                                            epochTransition.getNewEpoch(), context))
                                    .thenApply(x -> versionedMetadata);
                        });
                    } else {
                        return CompletableFuture.completedFuture(versionedMetadata);
                    }
                });
    }

    @Override
    public CompletableFuture<SimpleEntry<Long, Long>> getSplitMergeCountsTillEpoch(EpochRecord epochRecord,
                                                                                   OperationContext context) {
        if (epochRecord.hasSplitMergeCounts()) {
            return CompletableFuture.completedFuture(new SimpleEntry<>(epochRecord.getSplits(), epochRecord.getMerges()));
        } else {
            //migration case: build the cumulative count from all previous epochs
            return fetchEpochs(0, epochRecord.getEpoch(), true, context)
                    .thenApply(this::mapToScaleMetadata)
                    .thenApply(scaleMetadataList -> {
                AtomicLong totalNumSplits = new AtomicLong(0L);
                AtomicLong totalNumMerges = new AtomicLong(0L);
                scaleMetadataList.forEach(x -> {
                    totalNumMerges.addAndGet(x.getMerges());
                    totalNumSplits.addAndGet(x.getSplits());
                });
                return new SimpleEntry<>(totalNumSplits.get(), totalNumMerges.get());
            });
        }
    }

    private long getNewEpochSplitCount(final long splitCountPrevEpoch, ImmutableList<StreamSegmentRecord> lastEpochSegments, ImmutableList<StreamSegmentRecord> newEpochSegments) {
        if (splitCountPrevEpoch == EpochRecord.DEFAULT_COUNT_VALUE) {
            return EpochRecord.DEFAULT_COUNT_VALUE;
        }
        return splitCountPrevEpoch + findSegmentSplitsMerges(lastEpochSegments, newEpochSegments);
    }

    private long getNewEpochMergeCount(final long mergeCountPrevEpoch, ImmutableList<StreamSegmentRecord> lastEpochSegments, ImmutableList<StreamSegmentRecord> newEpochSegments) {
        if (mergeCountPrevEpoch == EpochRecord.DEFAULT_COUNT_VALUE) {
            return EpochRecord.DEFAULT_COUNT_VALUE;
        }
        return mergeCountPrevEpoch + findSegmentSplitsMerges(newEpochSegments, lastEpochSegments);
    }
 
    private CompletableFuture<Void> updateHistoryTimeSeries(HistoryTimeSeriesRecord record, OperationContext context) {
        int historyChunk = record.getEpoch() / historyChunkSize.get();
        boolean isFirst = record.getEpoch() % historyChunkSize.get() == 0;

        if (isFirst) {
            return createHistoryTimeSeriesChunk(historyChunk, record, context);
        } else {
            return getHistoryTimeSeriesChunkData(historyChunk, true, context)
                    .thenCompose(x -> {
                        HistoryTimeSeries historyChunkTimeSeries = x.getObject();
                        if (historyChunkTimeSeries.getLatestRecord().getEpoch() < record.getEpoch()) {
                            HistoryTimeSeries update = HistoryTimeSeries.addHistoryRecord(historyChunkTimeSeries, record);
                            return Futures.toVoid(updateHistoryTimeSeriesChunkData(historyChunk,
                                    new VersionedMetadata<>(update, x.getVersion()), context));
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        }
    }

    private CompletableFuture<VersionedMetadata<EpochTransitionRecord>> migrateManualScaleToNewEpoch(
            VersionedMetadata<EpochTransitionRecord> versionedMetadata, VersionedMetadata<State> versionedState,
            EpochRecord currentEpoch, OperationContext context) {
        EpochTransitionRecord epochTransition = versionedMetadata.getObject();
        return getEpochRecord(epochTransition.getActiveEpoch(), context)
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
                                currentEpoch, duplicateSegmentsToSeal, Lists.newArrayList(
                                        epochTransition.getNewSegmentsWithRange().values()),
                                epochTransition.getTime());
                        return updateEpochTransitionNode(new VersionedMetadata<>(updatedRecord, versionedMetadata.getVersion()),
                                context)
                                .thenApply(v -> new VersionedMetadata<>(updatedRecord, v));
                    } else {
                        return updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY,
                                        versionedMetadata.getVersion()), context)
                                .thenCompose(v -> updateVersionedState(versionedState, State.ACTIVE, context))
                                .thenApply(v -> {
                                    log.warn(context.getRequestId(),
                                            "Scale epoch transition record is inconsistent with VersionedMetadata in the table. {}",
                                            epochTransition.getNewEpoch());
                                    throw new IllegalStateException("Epoch transition record is inconsistent.");
                                });
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransition(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getEpochTransitionNode(context)
                .thenApply(x -> new VersionedMetadata<>(x.getObject(), x.getVersion()));
    }

    @Override
    public CompletableFuture<VersionedMetadata<EpochTransitionRecord>> resetEpochTransition(
            VersionedMetadata<EpochTransitionRecord> record, OperationContext context) {
        Preconditions.checkNotNull(record);
        return updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY, record.getVersion()), context)
                .thenApply(v -> new VersionedMetadata<>(EpochTransitionRecord.EMPTY, v));
    }

    private CompletableFuture<Void> clearMarkers(final Set<Long> segments, OperationContext context) {
        return Futures.toVoid(Futures.allOfWithResults(segments.stream().parallel()
                                                               .map(x -> removeColdMarker(x, context))
                                                               .collect(Collectors.toList())));
    }
    
    @Override
    public CompletableFuture<Void> scaleOldSegmentsSealed(Map<Long, Long> sealedSegmentSizes,
                                                          VersionedMetadata<EpochTransitionRecord> record,
                                                          OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        EpochTransitionRecord epochTransition = record.getObject();
        return Futures.toVoid(clearMarkers(epochTransition.getSegmentsToSeal(), context)
                .thenCompose(x -> updateSealedSegmentSizes(sealedSegmentSizes, context))
                .thenCompose(x -> updateCurrentEpochRecord(epochTransition.getNewEpoch(), context)));
    }

    @Override
    public CompletableFuture<Void> completeScale(VersionedMetadata<EpochTransitionRecord> record, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        Preconditions.checkNotNull(record);
        Preconditions.checkArgument(!record.getObject().equals(EpochTransitionRecord.EMPTY));
        return Futures.toVoid(updateEpochTransitionNode(new VersionedMetadata<>(EpochTransitionRecord.EMPTY,
                record.getVersion()), context));
    }
    
    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startRollingTxn(
            int activeEpoch, VersionedMetadata<CommittingTransactionsRecord> existing, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        CommittingTransactionsRecord record = existing.getObject();
        if (record.isRollingTxnRecord()) {
            return CompletableFuture.completedFuture(existing);
        } else {
            CommittingTransactionsRecord update = record.createRollingTxnRecord(activeEpoch);
            return updateCommittingTxnRecord(new VersionedMetadata<>(update, existing.getVersion()), context)
                    .thenApply(version -> new VersionedMetadata<>(update, version));
        }
    }

    @Override
    public CompletableFuture<Void> rollingTxnCreateDuplicateEpochs(
            Map<Long, Long> sealedTxnEpochSegments, long time, VersionedMetadata<CommittingTransactionsRecord> record,
            OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        Preconditions.checkArgument(record.getObject().isRollingTxnRecord());
        CommittingTransactionsRecord committingTxnRecord = record.getObject();
        return getActiveEpoch(true, context)
                .thenCompose(activeEpochRecord -> getEpochRecord(committingTxnRecord.getEpoch(), context)
                        .thenCompose(transactionEpochRecord -> {
                            if (activeEpochRecord.getEpoch() > committingTxnRecord.getCurrentEpoch()) {
                                log.debug(context.getRequestId(), "Duplicate Epochs {} already created. Ignore.",
                                        committingTxnRecord.getNewActiveEpoch());
                                return CompletableFuture.completedFuture(null);
                            }
                            long timeStamp = Math.max(activeEpochRecord.getCreationTime() + 1, time);
                            ImmutableList.Builder<StreamSegmentRecord> duplicateTxnSegmentsBuilder = ImmutableList.builder();
                            transactionEpochRecord
                                    .getSegments().stream()
                                    .forEach(x ->
                                            duplicateTxnSegmentsBuilder.add(newSegmentRecord(
                                                    computeSegmentId(getSegmentNumber(x.segmentId()),
                                                    committingTxnRecord.getNewTxnEpoch()),
                                                    timeStamp, x.getKeyStart(), x.getKeyEnd())));
                            ImmutableList.Builder<StreamSegmentRecord> duplicateActiveSegmentsBuilder = ImmutableList.builder();

                            activeEpochRecord
                                    .getSegments().stream()
                                    .forEach(x -> duplicateActiveSegmentsBuilder.add(
                                            newSegmentRecord(computeSegmentId(getSegmentNumber(x.segmentId()),
                                            committingTxnRecord.getNewActiveEpoch()),
                                            timeStamp + 1, x.getKeyStart(), x.getKeyEnd())));

                            CompletableFuture<EpochRecord> txnEpochFuture = getSplitMergeCountsTillEpoch(
                                    activeEpochRecord, context).thenCompose(txnSplitMergeCount -> {
                                ImmutableList<StreamSegmentRecord> duplicateTxnEpochSegments =
                                        duplicateTxnSegmentsBuilder.build();
                                EpochRecord duplicateTxnEpoch = new EpochRecord(committingTxnRecord.getNewTxnEpoch(),
                                        transactionEpochRecord.getReferenceEpoch(), duplicateTxnEpochSegments, timeStamp,
                                        getNewEpochSplitCount(txnSplitMergeCount.getKey(),
                                                activeEpochRecord.getSegments(), duplicateTxnEpochSegments),
                                        getNewEpochMergeCount(txnSplitMergeCount.getValue(),
                                                activeEpochRecord.getSegments(), duplicateTxnEpochSegments));
                                return CompletableFuture.completedFuture(duplicateTxnEpoch);
                            });
                            CompletableFuture<EpochRecord> activeEpochFuture = txnEpochFuture
                                    .thenCompose(previousEpoch -> getSplitMergeCountsTillEpoch(previousEpoch, context)
                                            .thenCompose(prevSplitMergeCounts -> {
                                ImmutableList<StreamSegmentRecord> activeEpochSegments = duplicateActiveSegmentsBuilder.build();
                                EpochRecord duplicateActiveEpoch = new EpochRecord(committingTxnRecord.getNewActiveEpoch(),
                                        activeEpochRecord.getReferenceEpoch(), activeEpochSegments, timeStamp + 1,
                                        getNewEpochSplitCount(prevSplitMergeCounts.getKey(),
                                                previousEpoch.getSegments(), activeEpochSegments),
                                        getNewEpochMergeCount(prevSplitMergeCounts.getValue(),
                                                previousEpoch.getSegments(), activeEpochSegments));
                                return CompletableFuture.completedFuture(duplicateActiveEpoch);
                            }));
                            return CompletableFuture.allOf(txnEpochFuture, activeEpochFuture).thenCompose(v -> {
                                EpochRecord duplicateTxnEpoch = txnEpochFuture.join();
                                EpochRecord duplicateActiveEpoch = activeEpochFuture.join();
                                HistoryTimeSeriesRecord timeSeriesRecordTxnEpoch =
                                        new HistoryTimeSeriesRecord(duplicateTxnEpoch.getEpoch(),
                                                duplicateTxnEpoch.getReferenceEpoch(),
                                                ImmutableList.of(), ImmutableList.of(), timeStamp);

                                HistoryTimeSeriesRecord timeSeriesRecordActiveEpoch =
                                        new HistoryTimeSeriesRecord(duplicateActiveEpoch.getEpoch(),
                                                duplicateActiveEpoch.getReferenceEpoch(),
                                                ImmutableList.of(), ImmutableList.of(), timeStamp + 1);
                                return createEpochRecord(duplicateTxnEpoch, context)
                                        .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordTxnEpoch, context))
                                        .thenCompose(x -> createEpochRecord(duplicateActiveEpoch, context))
                                        .thenCompose(x -> updateHistoryTimeSeries(timeSeriesRecordActiveEpoch, context))
                                        .thenCompose(x -> createSegmentSealedEpochRecords(
                                                activeEpochRecord.getSegments().stream().map(StreamSegmentRecord::segmentId)
                                                        .collect(Collectors.toList()), duplicateTxnEpoch.getEpoch(), context))
                                        .thenCompose(x -> createSegmentSealedEpochRecords(
                                                duplicateTxnEpoch.getSegments().stream().map(StreamSegmentRecord::segmentId)
                                                        .collect(Collectors.toList()), duplicateActiveEpoch.getEpoch(), context));
                            }).thenCompose(r -> updateSealedSegmentSizes(sealedTxnEpochSegments, context));
                        }));
    }

    @Override
    public CompletableFuture<Void> completeRollingTxn(Map<Long, Long> sealedActiveEpochSegments,
                                                      VersionedMetadata<CommittingTransactionsRecord> versionedMetadata,
                                                      OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getActiveEpoch(true, context)
                .thenCompose(activeEpochRecord -> {
                    CommittingTransactionsRecord committingTxnRecord = versionedMetadata.getObject();
                    int activeEpoch = committingTxnRecord.getCurrentEpoch();
                    if (activeEpochRecord.getEpoch() == activeEpoch) {
                        return updateSealedSegmentSizes(sealedActiveEpochSegments, context)
                                .thenCompose(x -> clearMarkers(sealedActiveEpochSegments.keySet(), context))
                                .thenCompose(x -> updateCurrentEpochRecord(committingTxnRecord.getNewActiveEpoch(), context));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<UUID> generateNewTxnId(int msb32Bit, long lsb64Bit, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getActiveEpochRecord(true, context)
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
                                                                         final long maxExecutionTime,
                                                                         OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
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
        return verifyNotSealed(context).thenCompose(v -> createNewTransaction(epoch, txnId, record, context)
                .thenApply(version -> new VersionedTransactionData(epoch, txnId, version,
                        TxnStatus.OPEN, current, maxExecTimestamp, "", Long.MIN_VALUE, Long.MIN_VALUE, ImmutableMap.of())));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final VersionedTransactionData txnData,
                                                                       final long lease,
                                                                       OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        // Update txn record with new lease value and return versioned tx data.
        final int epoch = txnData.getEpoch();
        final UUID txnId = txnData.getId();
        final Version version = txnData.getVersion();
        final long creationTime = txnData.getCreationTime();
        final long maxExecutionExpiryTime = txnData.getMaxExecutionExpiryTime();
        final TxnStatus status = txnData.getStatus();
        final String writerId = txnData.getWriterId();
        final long commitTime = txnData.getCommitTime();
        final long position = txnData.getCommitOrder();
        final ImmutableMap<Long, Long> commitOffsets = txnData.getCommitOffsets();
        final ActiveTxnRecord newData = new ActiveTxnRecord(creationTime, System.currentTimeMillis() + lease,
                maxExecutionExpiryTime, status, writerId, commitTime, position, commitOffsets);
        final VersionedMetadata<ActiveTxnRecord> data = new VersionedMetadata<>(newData, version);

        return updateActiveTx(epoch, txnId, data, context)
                .thenApply(updatedVersion -> new VersionedTransactionData(epoch, txnId, updatedVersion, status, creationTime, 
                        maxExecutionExpiryTime, writerId, commitTime, position, commitOffsets));
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return getActiveTx(epoch, txId, context)
                .thenApply(data -> {
                    ActiveTxnRecord activeTxnRecord = data.getObject();
                    return new VersionedTransactionData(epoch, txId, data.getVersion(),
                            activeTxnRecord.getTxnStatus(), activeTxnRecord.getTxCreationTimestamp(),
                            activeTxnRecord.getMaxExecutionExpiryTime(), activeTxnRecord.getWriterId(), 
                            activeTxnRecord.getCommitTime(), activeTxnRecord.getCommitOrder(), activeTxnRecord.getCommitOffsets());
                });
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return getActiveTx(epoch, txId, context).handle((ok, ex) -> {
            if (ex != null && Exceptions.unwrap(ex) instanceof DataNotFoundException) {
                return TxnStatus.UNKNOWN;
            } else if (ex != null) {
                throw new CompletionException(ex);
            }
            return ok.getObject().getTxnStatus();
        }).thenCompose(x -> {
            if (x.equals(TxnStatus.UNKNOWN)) {
                return getCompletedTxnStatus(txId, context);
            } else {
                return CompletableFuture.completedFuture(x);
            }
        });
    }

    private CompletableFuture<TxnStatus> getCompletedTxnStatus(UUID txId, OperationContext context) {
        return getCompletedTx(txId, context).handle((ok, ex) -> {
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
                                                                              final String writerId, final long timestamp,
                                                                              OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return sealActiveTxn(epoch, txId, commit, version, writerId, timestamp, context)
                .exceptionally(ex -> new SimpleEntry<>(handleDataNotFoundException(ex), null))
                .thenCompose(pair -> {
                    if (pair.getKey() == TxnStatus.UNKNOWN) {
                        return validateCompletedTxn(txId, commit, context).thenApply(status -> new SimpleEntry<>(status, null));
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
                                                                             final String writerId, final long timestamp,
                                                                             OperationContext context) {
        return getActiveTx(epoch, txId, context).thenCompose(data -> {
            ActiveTxnRecord txnRecord = data.getObject();
            Version dataVersion = version.orElseGet(data::getVersion);
            TxnStatus status = txnRecord.getTxnStatus();
            switch (status) {
                case OPEN:
                    return sealActiveTx(epoch, txId, commit, txnRecord, dataVersion, writerId, timestamp, context).thenApply(y ->
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
                                                    final Version version, final String writerId, final long timestamp,
                                                    OperationContext context) {
        CompletableFuture<ActiveTxnRecord> future;
        if (commit) {
            // if request is for commit --> add a new entry to orderer. 
            // update record
            if (!previous.getTxnStatus().equals(TxnStatus.COMMITTING)) {
                future = addTxnToCommitOrder(txId, context)
                        .thenApply(position -> new ActiveTxnRecord(previous.getTxCreationTimestamp(),
                                previous.getLeaseExpiryTime(),
                                previous.getMaxExecutionExpiryTime(),
                                TxnStatus.COMMITTING,
                                writerId,
                                timestamp,
                                position));
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
            return updateActiveTx(epoch, txId, data, context);
        });
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
     * @return A completableFuture, which when completed will have marks reported for all transactions in the committing 
     * transaction record for which a writer with time and position information is available. 
     */
    CompletableFuture<Void> generateMarksForTransactions(OperationContext context,
                                                         Map<String, TxnWriterMark> writerMarks) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        Preconditions.checkArgument(writerMarks != null);
        
        // For each writerId we will take the transaction with the time and position pair (which is to take
        // max of all transactions for the said writer).
        // Note: if multiple transactions from same writer have same time, we will take any one arbitrarily and
        // use its position for watermarks. Other positions and times would be ignored.
        val noteTimeFutures = writerMarks
                .entrySet()
                .stream().map(x -> Futures.exceptionallyExpecting(
                        noteWriterMark(x.getKey(), x.getValue().getTimestamp(), x.getValue().getPosition(), context),
                        DATA_NOT_FOUND_PREDICATE, null)).collect(Collectors.toList());
        return Futures.allOf(noteTimeFutures);
    }

    @VisibleForTesting
    public CompletableFuture<TxnStatus> commitTransaction(final UUID txId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        int epoch = RecordHelper.getTransactionEpoch(txId);

        return checkTransactionStatus(txId, context)
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
                                    String.format("Stream: %s Transaction: %s State: %s",
                                            getName(), txId.toString(), x.toString()));
                        case UNKNOWN:
                        default:
                            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                                    String.format("Stream: %s Transaction: %s", getName(), txId.toString()));
                    }
                }).thenCompose(x -> {
                    if (x.equals(TxnStatus.COMMITTING)) {
                        return createCompletedTxEntry(txId, new CompletedTxnRecord(System.currentTimeMillis(),
                                        TxnStatus.COMMITTED),
                                context);
                    } else {
                        return CompletableFuture.completedFuture(null); // already committed, do nothing
                    }
                }).thenCompose(x -> removeActiveTxEntry(epoch, txId, context)).thenApply(x -> TxnStatus.COMMITTED);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final UUID txId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        int epoch = RecordHelper.getTransactionEpoch(txId);
        return checkTransactionStatus(txId, context).thenApply(x -> {
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
                return createCompletedTxEntry(txId, new CompletedTxnRecord(System.currentTimeMillis(), TxnStatus.ABORTED),
                        context);
            } else {
                return CompletableFuture.completedFuture(null); // already aborted, do nothing
            }
        }).thenCompose(y -> removeActiveTxEntry(epoch, txId, context)).thenApply(y -> TxnStatus.ABORTED);
    }
    
    @SneakyThrows
    private TxnStatus handleDataNotFoundException(Throwable ex) {
        if (Exceptions.unwrap(ex) instanceof DataNotFoundException) {
            return TxnStatus.UNKNOWN;
        } else {
            throw ex;
        }
    }

    private CompletableFuture<TxnStatus> validateCompletedTxn(UUID txId, boolean commit, OperationContext context) {
        return getCompletedTxnStatus(txId, context).thenApply(status -> {
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
    public CompletableFuture<EpochRecord> getActiveEpoch(boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getCurrentEpochRecordData(ignoreCached, context).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<EpochRecord> getEpochRecord(int epoch, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getEpochRecordData(epoch, context).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> setColdMarker(long segmentId, long timestamp, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getMarkerData(segmentId, context).thenCompose(x -> {
            if (x != null) {
                final VersionedMetadata<Long> data = new VersionedMetadata<>(timestamp, x.getVersion());
                return Futures.toVoid(updateMarkerData(segmentId, data, context));
            } else {
                return createMarkerData(segmentId, timestamp, context);
            }
        });
    }

    @Override
    public CompletableFuture<Long> getColdMarker(long segmentId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getMarkerData(segmentId, context)
                .thenApply(x -> (x != null) ? x.getObject() : 0L);
    }

    @Override
    public CompletableFuture<Void> removeColdMarker(long segmentId, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return removeMarkerData(segmentId, context);
    }

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(Map<Long, Long> streamCut, Optional<StreamCutRecord> reference,
                                                        OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        Map<Long, Long> referenceStreamCut = reference.map(StreamCutRecord::getStreamCut).orElse(Collections.emptyMap());
        return segmentsBetweenStreamCuts(referenceStreamCut, streamCut, context)
                .thenCompose(segmentsInBetween -> sizeBetweenStreamCuts(referenceStreamCut, streamCut, segmentsInBetween, context)
                        .thenApply(sizeBetween -> sizeBetween + reference.map(StreamCutRecord::getRecordingSize).orElse(0L)));
    }

    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(StreamCutRecord record, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getRetentionSetData(context)
                .thenCompose(data -> {
                    RetentionSet retention = data.getObject();
                    RetentionSet update = RetentionSet.addReferenceToStreamCutIfLatest(retention, record);
                    return createStreamCutRecordData(record.getRecordingTime(), record, context)
                            .thenCompose(v -> Futures.toVoid(updateRetentionSetData(
                                    new VersionedMetadata<>(update, data.getVersion()),
                                    context)));
                });
    }

    @Override
    public CompletableFuture<RetentionSet> getRetentionSet(OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getRetentionSetData(context)
                .thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<StreamCutRecord> getStreamCutRecord(StreamCutReferenceRecord record, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getStreamCutRecordData(record.getRecordingTime(), context).thenApply(VersionedMetadata::getObject);
    }

    @Override
    public CompletableFuture<Void> deleteStreamCutBefore(StreamCutReferenceRecord record, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getRetentionSetData(context)
                .thenCompose(data -> {
                    RetentionSet retention = data.getObject();
                    RetentionSet update = RetentionSet.removeStreamCutBefore(retention, record);
                    List<StreamCutReferenceRecord> toRemove = retention.retentionRecordsBefore(record);
                    return Futures.allOf(toRemove.stream().map(x -> deleteStreamCutRecordData(x.getRecordingTime(), context))
                                                 .collect(Collectors.toList()))
                                  .thenCompose(x -> Futures.toVoid(updateRetentionSetData(
                                          new VersionedMetadata<>(update, data.getVersion()),
                                          context)));
                });
    }

    @Override
    public CompletableFuture<Map.Entry<VersionedMetadata<CommittingTransactionsRecord>, List<VersionedTransactionData>>> startCommittingTransactions(
            int limit, OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getVersionedCommitTransactionsRecord(context)
                .thenCompose(versioned -> {
                    if (versioned.getObject().equals(CommittingTransactionsRecord.EMPTY)) {
                        return getOrderedCommittingTxnInLowestEpoch(limit, context)
                                .thenCompose(list -> {
                                    if (list.isEmpty()) {
                                        List<VersionedTransactionData> emptyTransactionData = new LinkedList<>();
                                        return CompletableFuture.completedFuture(new SimpleEntry<>(versioned, emptyTransactionData));
                                    } else {
                                        ImmutableList.Builder<UUID> txIdList = ImmutableList.builder();
                                        list.forEach(x -> {
                                            txIdList.add(x.getId());
                                        });
                                        List<Long> positions = list.stream().map(VersionedTransactionData::getCommitOrder)
                                                                   .collect(Collectors.toList());
                                        int epoch = RecordHelper.getTransactionEpoch(list.get(0).getId());
                                        CommittingTransactionsRecord record =
                                                new CommittingTransactionsRecord(epoch, txIdList.build());
                                        return updateCommittingTxnRecord(new VersionedMetadata<>(record, versioned.getVersion()),
                                                context)
                                                // now that we have included transactions from positions for commit, we
                                                // can safely remove the position references in orderer. 
                                                .thenCompose(version -> removeTxnsFromCommitOrder(positions, context)
                                                        .thenApply(v -> new SimpleEntry<>(
                                                                new VersionedMetadata<>(record, version), list)));
                                    }
                                });
                    } else {
                        List<String> transactionsToCommit = versioned.getObject().getTransactionsToCommit()
                                .stream().map(UUID::toString).collect(Collectors.toList());
                        return getVersionedTransactionRecords(versioned.getObject().getEpoch(), transactionsToCommit, context)
                                .thenApply(x -> new SimpleEntry<>(versioned, x));
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getVersionedCommitTransactionsRecord(
            OperationContext context) {
        Preconditions.checkNotNull(context, "Operation context cannot be null");
        return getCommitTxnRecord(context)
                .thenApply(r -> new VersionedMetadata<>(r.getObject(), r.getVersion()));
    }

    @Override
    public CompletableFuture<Void> completeCommittingTransactions(VersionedMetadata<CommittingTransactionsRecord> record,
                                                                  OperationContext context,
                                                                  Map<String, TxnWriterMark> writerMarks) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        // Chain all transaction commit futures one after the other. This will ensure that order of commit
        // if honoured and is based on the order in the list.
        CompletableFuture<Void> future = generateMarksForTransactions(context, writerMarks);
        for (UUID txnId : record.getObject().getTransactionsToCommit()) {
            log.debug(context.getRequestId(), "Committing transaction {} on stream {}/{}", txnId, scope, name);
            // commit transaction in segment store
            future = future
                    // mark transaction as committed in metadata store.
                    .thenCompose(x -> commitTransaction(txnId, context)
                            .thenAccept(done -> {
                                log.debug(context.getRequestId(),
                                        "transaction {} on stream {}/{} committed successfully", txnId, scope, name);
                            }));
        }
        return future
                .thenCompose(x -> Futures.toVoid(updateCommittingTxnRecord(
                        new VersionedMetadata<>(CommittingTransactionsRecord.EMPTY,
                        record.getVersion()), context)));
    }

    @Override
    public CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return createWaitingRequestNodeIfAbsent(processorName, context);
    }

    @Override
    public CompletableFuture<String> getWaitingRequestProcessor(OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getWaitingRequestNode(context)
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
    public CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getWaitingRequestProcessor(context)
                .thenCompose(waitingRequest -> {
                    if (waitingRequest != null && waitingRequest.equals(processorName)) {
                        return deleteWaitingRequestNode(context);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<WriterTimestampResponse> noteWriterMark(String writer, long timestamp, Map<Long, Long> position,
                                                                     OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        // Remember: a writer position is not a stream cut.  
        // For sanity check we will check that the request advances the time and position.
        // if time is not advanced --> respond with invalid time.
        // if position is not advanced --> respond with invalid position
        ImmutableMap<Long, Long> newPosition = ImmutableMap.copyOf(position);
        return Futures.exceptionallyExpecting(getWriterMarkRecord(writer, context), DATA_NOT_FOUND_PREDICATE, null)
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
                        return createWriterMarkRecord(writer, timestamp, newPosition, context)
                                .exceptionally(e -> {
                                    if (Exceptions.unwrap(e) instanceof StoreException.DataExistsException) {
                                        throw StoreException.create(StoreException.Type.WRITE_CONFLICT,
                                                "writer mark exists");
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
                        return updateWriterMarkRecord(writer, timestamp, newPosition, true, record.getVersion(), context)
                                .thenApply(v -> WriterTimestampResponse.SUCCESS);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> shutdownWriter(String writer, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getWriterMarkRecord(writer, context)
               .thenCompose(record -> updateWriterMarkRecord(writer, record.getObject().getTimestamp(),
                       record.getObject().getPosition(),
                       false, record.getVersion(), context));
    }

    @Override
    public CompletableFuture<Void> removeWriter(String writer, WriterMark writerMark, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return Futures.exceptionallyExpecting(getWriterMarkRecord(writer, context), DATA_NOT_FOUND_PREDICATE, null)
                      .thenCompose(record -> {
                          if (record == null) {
                              // do nothing
                              return CompletableFuture.completedFuture(null);
                          } else {
                              if (writerMark.equals(record.getObject())) {
                                  return removeWriterRecord(writer, record.getVersion(), context);
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
    public CompletableFuture<WriterMark> getWriterMark(String writer, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getWriterMarkRecord(writer, context).thenApply(VersionedMetadata::getObject);
    }
    
    protected CompletableFuture<List<VersionedTransactionData>> getOrderedCommittingTxnInLowestEpochHelper(
            ZkOrderedStore txnCommitOrderer, int limit, Executor executor, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

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

                          // We will opportunistically identify ordered positions that are stale (either transaction
                          // is no longer active)
                          // or its a duplicate entry or transaction is aborting. 
                          ConcurrentSkipListSet<Long> toPurge = new ConcurrentSkipListSet<>();
                          ConcurrentSkipListSet<VersionedTransactionData> transactionsData = new ConcurrentSkipListSet<>(
                                  Comparator.comparingLong(VersionedTransactionData::getCommitOrder));

                          // Collect transactions that are in committing state from smallest available epoch 
                          // smallest epoch has transactions in committing state, we should break, else continue.
                          // also remove any transaction order references which are invalid.
                          return Futures.loop(() -> iterator.hasNext() && transactionsData.isEmpty(),
                                  () -> processTransactionsInEpoch(iterator.next(), toPurge, transactionsData,
                                  limit, executor, context), executor)
                                  .thenCompose(v -> txnCommitOrderer.removeEntities(getScope(), getName(), toPurge))
                                  .thenApply(v -> transactionsData.stream().collect(Collectors.toList()));
                      });
    }

    protected CompletableFuture<Map<Long, UUID>> getAllOrderedCommittingTxnsHelper(ZkOrderedStore txnCommitOrderer,
                                                                                   OperationContext context) {
        return Futures.exceptionallyExpecting(txnCommitOrderer.getEntitiesWithPosition(getScope(), getName()),
                DATA_NOT_FOUND_PREDICATE, Collections.emptyMap())
                      .thenApply(map -> map.entrySet().stream()
                                           .collect(Collectors.toMap(Map.Entry::getKey, x -> UUID.fromString(x.getValue()))));
    }

    CompletableFuture<List<VersionedTransactionData>> getVersionedTransactionRecords(int epoch, List<String> txnIds, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return Futures.allOfWithResults(txnIds.stream().map(txnIdStr -> {
            UUID txnId = UUID.fromString(txnIdStr);
            return Futures.exceptionallyExpecting(getTransactionData(txnId, context),
                    DATA_NOT_FOUND_PREDICATE, VersionedTransactionData.EMPTY);
        }).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> processTransactionsInEpoch(Map.Entry<Integer, List<Map.Entry<Long, String>>> nextEpoch,
                                                             ConcurrentSkipListSet<Long> toPurge,
                                                             ConcurrentSkipListSet<VersionedTransactionData> transactionsMap,
                                                             int limit,
                                                             Executor executor, OperationContext context) {
        int epoch = nextEpoch.getKey();
        List<Long> orders = new ArrayList<>();
        List<String> txnIds = new ArrayList<>();
        nextEpoch.getValue().stream().sorted(Comparator.comparingLong(Map.Entry::getKey)).forEach(x -> {
            orders.add(x.getKey());
            txnIds.add(x.getValue());
        });

        AtomicInteger from = new AtomicInteger(0);
        AtomicInteger till = new AtomicInteger(Math.min(limit, txnIds.size()));
        return Futures.loop(() -> from.get() < txnIds.size() && transactionsMap.size() < limit, 
                () -> getVersionedTransactionRecords(epoch, txnIds.subList(from.get(), till.get()), context).thenAccept(txns -> {
                for (int i = 0; i < txns.size() && transactionsMap.size() < limit; i++) {
                VersionedTransactionData txnRecord = txns.get(i);
                int index = from.get() + i;
                UUID txnId = UUID.fromString(txnIds.get(index));
                long order = orders.get(index);
                switch (txnRecord.getStatus()) {
                    case COMMITTING:
                        if (txnRecord.getCommitOrder() == order) {
                            // if entry matches record's position then include it
                            transactionsMap.add(txnRecord);
                        } else {
                            log.debug(context.getRequestId(), "duplicate txn {} at position {}. removing {}",
                                    txnId, txnRecord.getCommitOrder(), order);
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
                        log.debug(context.getRequestId(), "stale txn {} with status {}. removing {}",
                                txnId, txnRecord.getStatus(), order);
                        toPurge.add(order);
                        break;
                }
            }
            from.set(till.get());
            till.set(Math.min(from.get() + limit, txnIds.size()));
        }), executor);
    }

    CompletableFuture<List<ActiveTxnRecord>> getTransactionRecords(int epoch, List<String> txnIds, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return Futures.allOfWithResults(txnIds.stream().map(txnIdStr -> {
            UUID txnId = UUID.fromString(txnIdStr);
            return Futures.exceptionallyExpecting(getActiveTx(epoch, txnId, context).thenApply(VersionedMetadata::getObject),
                    DATA_NOT_FOUND_PREDICATE, ActiveTxnRecord.EMPTY);
        }).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> verifyLegalState(OperationContext context) {
        return getState(false, context).thenApply(state -> {
            if (state == null || state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                throw StoreException.create(StoreException.Type.ILLEGAL_STATE,
                        "Stream: " + getName() + " State: " + state.name());
            }
            return null;
        });
    }

    private CompletableFuture<Void> createEpochRecord(EpochRecord epoch, OperationContext context) {
        return createEpochRecordDataIfAbsent(epoch.getEpoch(), epoch, context);
    }

    private CompletableFuture<Void> updateCurrentEpochRecord(int newActiveEpoch, OperationContext context) {
        return getEpochRecord(newActiveEpoch, context)
                .thenCompose(epochRecord -> getCurrentEpochRecordData(true, context)
                        .thenCompose(currentEpochRecordData -> {
                            EpochRecord existing = currentEpochRecordData.getObject();
                            if (existing.getEpoch() < newActiveEpoch) {
                                return Futures.toVoid(updateCurrentEpochRecordData(
                                        new VersionedMetadata<>(epochRecord, currentEpochRecordData.getVersion()), context));
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }));
    }

    private CompletableFuture<Void> createSealedSegmentSizeMapShardIfAbsent(int shardNumber, OperationContext context) {
        SealedSegmentsMapShard shard = SealedSegmentsMapShard.builder().shardNumber(shardNumber)
                                                             .sealedSegmentsSizeMap(Collections.emptyMap()).build();
        return createSealedSegmentSizesMapShardDataIfAbsent(shardNumber, shard, context);
    }

    @Override
    public CompletableFuture<SealedSegmentsMapShard> getSealedSegmentSizeMapShard(int shard, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getSealedSegmentSizesMapShardData(shard, context)
                .handle((r, e) -> {
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof DataNotFoundException) {
                            return SealedSegmentsMapShard.builder().shardNumber(shard)
                                                         .sealedSegmentsSizeMap(Collections.emptyMap()).build();
                        }
                        throw new CompletionException(e);
                    } else {
                        return r.getObject();
                    }
                });
    }

    private CompletableFuture<Void> updateSealedSegmentSizes(Map<Long, Long> sealedSegmentSizes, OperationContext context) {
        Map<Integer, List<Long>> shards = sealedSegmentSizes.keySet().stream()
                                                            .collect(Collectors.groupingBy(this::getShardNumber));
        return Futures.allOf(shards.entrySet().stream().map(x -> {
            int shard = x.getKey();
            List<Long> segments = x.getValue();

            return Futures.exceptionallyComposeExpecting(getSealedSegmentSizesMapShardData(shard, context),
                    DATA_NOT_FOUND_PREDICATE, () -> createSealedSegmentSizeMapShardIfAbsent(shard, context)
                            .thenCompose(v -> getSealedSegmentSizesMapShardData(shard, context)))
                    .thenCompose(mapShardData -> {
                        SealedSegmentsMapShard mapShard = mapShardData.getObject();
                        segments.forEach(z -> mapShard.addSealedSegmentSize(z, sealedSegmentSizes.get(z)));
                        return updateSealedSegmentSizesMapShardData(shard,
                                new VersionedMetadata<>(mapShard, mapShardData.getVersion()), context);
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
    CompletableFuture<List<EpochRecord>> fetchEpochs(int fromEpoch, int toEpoch, boolean ignoreCache, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        // fetch history time series chunk corresponding to from.
        // read entries till either last entry or till to
        // if to is not in this chunk fetch the next chunk and read till to
        // keep doing this until all records till to have been read.
        // keep computing history record from history time series by applying delta on previous.
        return getActiveEpochRecord(ignoreCache, context)
                .thenApply(currentEpoch -> currentEpoch.getEpoch() / historyChunkSize.get())
                .thenCompose(latestChunkNumber -> Futures.allOfWithResults(
                        IntStream.range(fromEpoch / historyChunkSize.get(), toEpoch / historyChunkSize.get() + 1)
                                 .mapToObj(i -> {
                                     int firstEpoch = Math.max(i * historyChunkSize.get(), fromEpoch);

                                     boolean ignoreCached = i >= latestChunkNumber;
                                     return getEpochsFromHistoryChunk(i, firstEpoch, toEpoch, ignoreCached, context);
                                 }).collect(Collectors.toList())))
                .thenApply(c -> c.stream().flatMap(Collection::stream).collect(Collectors.toList()));
    }

    private CompletableFuture<List<EpochRecord>> getEpochsFromHistoryChunk(int chunk, int firstEpoch, int toEpoch,
                                                                           boolean ignoreCached, OperationContext context) {
        return getEpochRecord(firstEpoch, context)
                .thenCompose(first -> getHistoryTimeSeriesChunk(chunk, ignoreCached, context)
                        .thenCompose(x -> {
                            List<CompletableFuture<EpochRecord>> identity = new ArrayList<>();
                            identity.add(CompletableFuture.completedFuture(first));
                            return Futures.allOfWithResults(x.getHistoryRecords().stream()
                                             .filter(r -> r.getEpoch() > firstEpoch && r.getEpoch() <= toEpoch)
                                             .reduce(identity, (r, s) -> {
                                                 CompletableFuture<EpochRecord> next = newEpochRecord(r.get(r.size() - 1),
                                                         s.getEpoch(), s.getReferenceEpoch(), s.getSegmentsCreated(),
                                                         s.getSegmentsSealed().stream().map(StreamSegmentRecord::segmentId)
                                                          .collect(Collectors.toList()), s.getScaleTime(), context);
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
                                                          final Collection<Long> sealedSegments, final long time,
                                                          OperationContext context) {
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
                ImmutableList<StreamSegmentRecord> epochSegments = segmentsBuilder.build();
                if (!lastRecord.hasSplitMergeCounts()) {
                    return new EpochRecord(epoch, referenceEpoch, epochSegments, time, EpochRecord.DEFAULT_COUNT_VALUE, EpochRecord.DEFAULT_COUNT_VALUE);
                } else {
                    return new EpochRecord(epoch, referenceEpoch, epochSegments, time,
                            getNewEpochSplitCount(lastRecord.getSplits(), lastRecord.getSegments(), epochSegments),
                            getNewEpochMergeCount(lastRecord.getMerges(), lastRecord.getSegments(), epochSegments));
                }
            });
        } else {
            return getEpochRecord(epoch, context);
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
    CompletableFuture<Integer> findEpochAtTime(long timestamp, boolean ignoreCached, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getActiveEpoch(ignoreCached, context)
                .thenCompose(activeEpoch -> searchEpochAtTime(0, activeEpoch.getEpoch() / historyChunkSize.get(),
                        x -> x == activeEpoch.getEpoch() / historyChunkSize.get(), timestamp, context)
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

    private CompletableFuture<Integer> searchEpochAtTime(int lowest, int highest, Predicate<Integer> ignoreCached,
                                                         long timestamp, OperationContext context) {
        final int middle = (lowest + highest) / 2;

        if (lowest > highest) {
            // either return epoch 0 or latest epoch
            return CompletableFuture.completedFuture(-1);
        }

        return getHistoryTimeSeriesChunk(middle, ignoreCached.test(middle), context)
                .thenCompose(chunk -> {
                    List<HistoryTimeSeriesRecord> historyRecords = chunk.getHistoryRecords();
                    long rangeLow = historyRecords.get(0).getScaleTime();
                    long rangeHigh = historyRecords.get(historyRecords.size() - 1).getScaleTime();
                    if (timestamp >= rangeLow && timestamp <= rangeHigh) {
                        // found
                        int index = CollectionHelpers.findGreatestLowerBound(historyRecords,
                                x -> Long.compare(timestamp, x.getScaleTime()));
                        assert index >= 0;
                        return CompletableFuture.completedFuture(historyRecords.get(index).getEpoch());
                    } else if (timestamp < rangeLow) {
                        return searchEpochAtTime(lowest, middle - 1, ignoreCached, timestamp, context);
                    } else {
                        return searchEpochAtTime(middle + 1, highest, ignoreCached, timestamp, context);
                    }
                });
    }

    @Override
    public CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber, OperationContext context) {
        Preconditions.checkNotNull(context, "operation context cannot be null");

        return getHistoryTimeSeriesChunk(chunkNumber, true, context);
    }
    
    private CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber, boolean ignoreCached,
                                                                           OperationContext context) {
        return getHistoryTimeSeriesChunkData(chunkNumber, ignoreCached, context)
                .thenCompose(x -> {
                    HistoryTimeSeries timeSeries = x.getObject();
                    // we should only retrieve the chunk from cache once the chunk is full to capacity and hence immutable. 
                    if (!ignoreCached && timeSeries.getHistoryRecords().size() < historyChunkSize.get()) {
                        return getHistoryTimeSeriesChunk(chunkNumber, true, context);
                    }
                    return CompletableFuture.completedFuture(timeSeries);
                });
    }

    // region abstract methods
    //region create delete
    abstract CompletableFuture<CreateStreamResponse> checkStreamExists(final StreamConfiguration configuration,
                                                                       final long creationTime, final int startingSegmentNumber,
                                                                       OperationContext context);

    abstract CompletableFuture<Void> createStreamMetadata(OperationContext context);
    
    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime, OperationContext context);

    abstract CompletableFuture<Void> deleteStream(OperationContext context);
    // endregion

    // region configuration
    abstract CompletableFuture<Void> createConfigurationIfAbsent(final StreamConfigurationRecord data, OperationContext context);

    abstract CompletableFuture<Version> setConfigurationData(final VersionedMetadata<StreamConfigurationRecord> configuration,
                                                             OperationContext context);

    abstract CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationData(boolean ignoreCached,
                                                                                                  OperationContext context);
    // endregion

    // region truncation
    abstract CompletableFuture<Void> createTruncationDataIfAbsent(final StreamTruncationRecord truncation, OperationContext context);

    abstract CompletableFuture<Version> setTruncationData(final VersionedMetadata<StreamTruncationRecord> truncationRecord,
                                                          OperationContext context);

    abstract CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationData(boolean ignoreCached,
                                                                                            OperationContext context);
    // endregion

    // region state
    abstract CompletableFuture<Void> createStateIfAbsent(final StateRecord state, OperationContext context);

    abstract CompletableFuture<Version> setStateData(final VersionedMetadata<StateRecord> state, OperationContext context);

    abstract CompletableFuture<VersionedMetadata<StateRecord>> getStateData(boolean ignoreCached, OperationContext context);
    // endregion

    // region subscriber
    // invoke this when creating a new Stream or when moving a Stream to CBR Retention Policy
    abstract CompletableFuture<Void> createSubscribersRecordIfAbsent(OperationContext context);

    abstract CompletableFuture<Version> setSubscriberData(final VersionedMetadata<StreamSubscriber> subscriber,
                                                          OperationContext context);
    // endregion

    // region retention
    abstract CompletableFuture<Void> createRetentionSetDataIfAbsent(RetentionSet data, OperationContext context);

    abstract CompletableFuture<Void> createStreamCutRecordData(long key, StreamCutRecord record, OperationContext context);

    abstract CompletableFuture<VersionedMetadata<StreamCutRecord>> getStreamCutRecordData(long recordingTime, OperationContext context);

    abstract CompletableFuture<Void> deleteStreamCutRecordData(long recordingTime, OperationContext context);

    abstract CompletableFuture<Version> updateRetentionSetData(VersionedMetadata<RetentionSet> tData, OperationContext context);

    abstract CompletableFuture<VersionedMetadata<RetentionSet>> getRetentionSetData(OperationContext context);
    // endregion

    // region history
    abstract CompletableFuture<Void> createHistoryTimeSeriesChunkDataIfAbsent(int chunkNumber, HistoryTimeSeries data,
                                                                              OperationContext context);

    abstract CompletableFuture<VersionedMetadata<HistoryTimeSeries>> getHistoryTimeSeriesChunkData(int chunkNumber,
                                                                                                   boolean ignoreCached,
                                                                                                   OperationContext context);

    abstract CompletableFuture<Version> updateHistoryTimeSeriesChunkData(int historyChunk,
                                                                         VersionedMetadata<HistoryTimeSeries> tData,
                                                                         OperationContext context);

    abstract CompletableFuture<Void> createCurrentEpochRecordDataIfAbsent(EpochRecord data, OperationContext context);

    abstract CompletableFuture<Version> updateCurrentEpochRecordData(VersionedMetadata<EpochRecord> data,
                                                                     OperationContext context);

    abstract CompletableFuture<VersionedMetadata<EpochRecord>> getCurrentEpochRecordData(boolean ignoreCached,
                                                                                         OperationContext context);

    abstract CompletableFuture<Void> createEpochRecordDataIfAbsent(int epoch, EpochRecord data, OperationContext context);

    abstract CompletableFuture<VersionedMetadata<EpochRecord>> getEpochRecordData(int epoch, OperationContext context);

    abstract CompletableFuture<Void> createSealedSegmentSizesMapShardDataIfAbsent(int shardNumber, SealedSegmentsMapShard data,
                                                                                  OperationContext context);

    abstract CompletableFuture<VersionedMetadata<SealedSegmentsMapShard>> getSealedSegmentSizesMapShardData(
            int shard, OperationContext context);

    abstract CompletableFuture<Version> updateSealedSegmentSizesMapShardData(
            int shard, VersionedMetadata<SealedSegmentsMapShard> data, OperationContext context);

    abstract CompletableFuture<Void> createSegmentSealedEpochRecords(Collection<Long> segmentToSeal, int epoch,
                                                                     OperationContext context);

    abstract CompletableFuture<VersionedMetadata<Integer>> getSegmentSealedRecordData(long segmentId, OperationContext context);
    // endregion

    // region transactions
    abstract CompletableFuture<Version> createNewTransaction(final int epoch, final UUID txId, final ActiveTxnRecord record,
                                                             OperationContext context);

    abstract CompletableFuture<VersionedMetadata<ActiveTxnRecord>> getActiveTx(final int epoch, final UUID txId,
                                                                               OperationContext context);

    abstract CompletableFuture<Version> updateActiveTx(final int epoch,
                                                       final UUID txId,
                                                       final VersionedMetadata<ActiveTxnRecord> data, OperationContext context);

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
    abstract CompletableFuture<Long> addTxnToCommitOrder(final UUID txId, OperationContext context);

    abstract CompletableFuture<Void> removeTxnsFromCommitOrder(final List<Long> positions, OperationContext context);

    abstract CompletableFuture<VersionedMetadata<CompletedTxnRecord>> getCompletedTx(final UUID txId, OperationContext context);

    abstract CompletableFuture<Void> removeActiveTxEntry(final int epoch, final UUID txId, OperationContext context);

    abstract CompletableFuture<Void> createCompletedTxEntry(final UUID txId, CompletedTxnRecord data, OperationContext context);

    abstract CompletableFuture<Map<UUID, ActiveTxnRecord>> getTxnInEpoch(int epoch, OperationContext context);

    /**
     * This method finds transactions to commit in lowest epoch and returns a sorted list of transaction ids, 
     * sorted by their order of commits. 
     * @param limit number of txns to fetch.
     * @return CompletableFuture which when completed will return ordered list of transaction ids and records.
     */
    abstract CompletableFuture<List<VersionedTransactionData>> getOrderedCommittingTxnInLowestEpoch(
            int limit, OperationContext context);
    
    @VisibleForTesting
    abstract CompletableFuture<Map<Long, UUID>> getAllOrderedCommittingTxns(OperationContext context);
    // endregion

    // region marker
    abstract CompletableFuture<Void> createMarkerData(long segmentId, long timestamp, OperationContext context);

    abstract CompletableFuture<Version> updateMarkerData(long segmentId, VersionedMetadata<Long> data, OperationContext context);

    abstract CompletableFuture<Void> removeMarkerData(long segmentId, OperationContext context);

    abstract CompletableFuture<VersionedMetadata<Long>> getMarkerData(long segmentId, OperationContext context);
    // endregion

    // region scale
    abstract CompletableFuture<Void> createEpochTransitionIfAbsent(EpochTransitionRecord epochTransition, OperationContext context);

    abstract CompletableFuture<Version> updateEpochTransitionNode(VersionedMetadata<EpochTransitionRecord> epochTransition,
                                                                  OperationContext context);

    abstract CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransitionNode(OperationContext context);
    // endregion

    // region txn commit
    abstract CompletableFuture<Void> createCommitTxnRecordIfAbsent(CommittingTransactionsRecord committingTxns,
                                                                   OperationContext context);

    abstract CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getCommitTxnRecord(OperationContext context);

    abstract CompletableFuture<Version> updateCommittingTxnRecord(VersionedMetadata<CommittingTransactionsRecord> data,
                                                                  OperationContext context);
    // endregion

    // region processor
    abstract CompletableFuture<Void> createWaitingRequestNodeIfAbsent(String data, OperationContext context);

    abstract CompletableFuture<String> getWaitingRequestNode(OperationContext context);

    abstract CompletableFuture<Void> deleteWaitingRequestNode(OperationContext context);
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
    abstract CompletableFuture<Void> createWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position,
                                                            OperationContext context);

    /**
     * Method to get writer mark record. 
     * @param writer writer id
     * @return CompletableFuture which when completed will contain writer's last reported mark.  
     */
    abstract CompletableFuture<VersionedMetadata<WriterMark>> getWriterMarkRecord(String writer, OperationContext context);

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
    abstract CompletableFuture<Void> updateWriterMarkRecord(String writer, long timestamp, ImmutableMap<Long, Long> position,
                                                            boolean isAlive, Version version, OperationContext context);

    /**
     * Method to delete existing writer mark record conditionally. 
     * 
     * @param writer writer id
     * @param version version of last record
     * @return CompletableFuture which when completed will have writer mark deleted.
     * Can throw Write Conflict if delete version mismatches.
     */
    abstract CompletableFuture<Void> removeWriterRecord(String writer, Version version, OperationContext context);
    // endregion
    // endregion
}
