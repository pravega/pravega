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
package io.pravega.controller.server.bucket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.SegmentWithRange;
import io.pravega.shared.watermarks.Watermark;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.ParametersAreNonnullByDefault;

import lombok.Synchronized;
import org.slf4j.LoggerFactory;

public class PeriodicWatermarking implements AutoCloseable {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PeriodicWatermarking.class));
    private static final int MAX_CACHE_SIZE = 500;
    private final StreamMetadataStore streamMetadataStore;
    private final BucketStore bucketStore;
    private final ScheduledExecutorService executor;
    private final LoadingCache<Stream, WatermarkClient> watermarkClientCache;
    private final LoadingCache<String, SynchronizerClientFactory> syncFactoryCache;
    private final RequestTracker requestTracker;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;

    public PeriodicWatermarking(StreamMetadataStore streamMetadataStore, BucketStore bucketStore,
                                ClientConfig clientConfig, ScheduledExecutorService executor, RequestTracker requestTracker) {
        this(streamMetadataStore, bucketStore, s -> SynchronizerClientFactory.withScope(s, clientConfig), executor, requestTracker);
    }

    @VisibleForTesting
    PeriodicWatermarking(StreamMetadataStore streamMetadataStore, BucketStore bucketStore,
                                 Function<String, SynchronizerClientFactory> synchronizerClientFactoryFactory,
                                 ScheduledExecutorService executor, RequestTracker requestTracker) {
        this.streamMetadataStore = streamMetadataStore;
        this.bucketStore = bucketStore;
        this.executor = executor;
        this.syncFactoryCache = CacheBuilder.newBuilder()
                                                .maximumSize(MAX_CACHE_SIZE)
                                                .expireAfterAccess(10, TimeUnit.MINUTES)
                                                .removalListener((RemovalListener<String, SynchronizerClientFactory>) notification -> {
                                                    notification.getValue().close();
                                                })
                                                .build(new CacheLoader<String, SynchronizerClientFactory>() {
                                                    @ParametersAreNonnullByDefault
                                                    @Override
                                                    public SynchronizerClientFactory load(final String scope) {
                                                        return synchronizerClientFactoryFactory.apply(scope);
                                                    }
                                                });
        this.watermarkClientCache = CacheBuilder.newBuilder()
                                                .maximumSize(MAX_CACHE_SIZE)
                                                .expireAfterAccess(10, TimeUnit.MINUTES)
                                                .removalListener((RemovalListener<Stream, WatermarkClient>) notification -> {
                                                    notification.getValue().close();
                                                })
                                                .build(new CacheLoader<Stream, WatermarkClient>() {
                                                    @ParametersAreNonnullByDefault
                                                    @Override
                                                    public WatermarkClient load(final Stream stream) {
                                                        return new WatermarkClient(stream, syncFactoryCache.getUnchecked(stream.getScope()));
                                                    }
                                                });
        this.requestTracker = requestTracker;
    }

    @Override
    public void close() {
        this.syncFactoryCache.invalidateAll();
        this.watermarkClientCache.invalidateAll();
    }

    /**
     * This method computes and emits a new watermark for the given stream.
     * It collects all the known writers for the given stream and includes only writers that are active (have reported
     * their marks recently). If all active writers have reported marks greater than the previously emitted watermark,
     * then new watermark is computed and emitted. If not, the window for considering writers as active is progressed.
     * @param stream stream for which watermark should be computed.
     * @return Returns a completableFuture which when completed will have completed another iteration of periodic watermark
     * computation.
     */
    public CompletableFuture<Void> watermark(Stream stream) {
        String scope = stream.getScope();
        String streamName = stream.getStreamName();
        long requestId = requestIdGenerator.get();
        String requestDescriptor = RequestTracker.buildRequestDescriptor("watermark", stream.getScope(),
                stream.getStreamName());
        requestTracker.trackRequest(requestDescriptor, requestId);
        OperationContext context = streamMetadataStore.createStreamContext(scope, streamName, requestId);

        if (scope.equals(NameUtils.INTERNAL_SCOPE_NAME)) {
            return CompletableFuture.completedFuture(null); 
        }
        
        log.debug(requestId, "Periodic background processing for watermarking called for stream {}/{}",
                scope, streamName);

        CompletableFuture<Map<String, WriterMark>> allWriterMarks = Futures.exceptionallyExpecting(
                streamMetadataStore.getAllWriterMarks(scope, streamName, context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, Collections.emptyMap());
        
        return allWriterMarks.thenCompose(writers -> {
            WatermarkClient watermarkClient = watermarkClientCache.getUnchecked(stream);

            try {
                watermarkClient.reinitialize();
            } catch (Exception e) {
                log.warn(requestId, "Watermarking client for stream {} threw exception {} during reinitialize.",
                        stream, Exceptions.unwrap(e).getClass());
                if (Exceptions.unwrap(e) instanceof NoSuchSegmentException) {
                    log.info(requestId, "Invalidating the watermark client in cache for stream {}.", stream);
                    watermarkClientCache.invalidate(stream);
                }
                throw e;
            }
            return streamMetadataStore.getConfiguration(scope, streamName, context, executor)
                .thenCompose(config -> filterWritersAndComputeWatermark(scope, streamName, context, watermarkClient, writers, config));
        }).exceptionally(e -> {
            log.warn(requestId, "Exception thrown while trying to perform periodic watermark computation. Logging and ignoring.", e);
            return null;
        });
    }

    private CompletionStage<Void> filterWritersAndComputeWatermark(String scope, String streamName, OperationContext context,
                                                                   WatermarkClient watermarkClient, Map<String, WriterMark> writers,
                                                                   StreamConfiguration config) {
        // 1. filter writers that are active.
        List<Entry<String, WriterMark>> activeWriters = new ArrayList<>();
        List<Entry<String, WriterMark>> inactiveWriters = new ArrayList<>();
        AtomicBoolean allActiveAreParticipating = new AtomicBoolean(true);
        writers.entrySet().forEach(x -> {
            if (watermarkClient.isWriterActive(x, config.getTimestampAggregationTimeout())) {
                activeWriters.add(x);
                if (!watermarkClient.isWriterParticipating(x.getValue().getTimestamp())) {
                    allActiveAreParticipating.set(false);
                }
            } else {
                inactiveWriters.add(x);
            }
        });

        // Stop all inactive writers that have been shutdown.
        CompletableFuture<List<Void>> removeInactiveWriters = 
                Futures.allOfWithResults(inactiveWriters.stream().map(x ->
                        Futures.exceptionallyExpecting(
                                streamMetadataStore.removeWriter(scope, streamName, x.getKey(),
                                x.getValue(), context, executor).thenAccept(v -> watermarkClient.untrackWriterInactivity(x.getKey())), 
                                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, null))
                                                        .collect(Collectors.toList()));

        if (activeWriters.isEmpty()) {
            // this will prevent the periodic cycles being spent in running watermarking workflow for a silent stream. 
            // as soon as any writer reports its mark, stream will be added to bucket and background 
            // periodic processing will resume.
            return removeInactiveWriters
                    .thenCompose(v -> bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.WatermarkingService,
                            scope, streamName, executor));
        }

        CompletableFuture<Watermark> watermarkFuture;
        if (!allActiveAreParticipating.get()) {
            // there are active writers that have not reported their marks. We should wait 
            // until they either report or become inactive. So we will complete this iteration without 
            // emitting any watermark (null) and in subsequent iterations if these writers have made progress
            // we will emit watermark or evict writers from watermark computation. 
            watermarkFuture = CompletableFuture.completedFuture(null);
        } else {
            // compute new mark
            watermarkFuture = computeWatermark(scope, streamName, context, activeWriters, 
                    watermarkClient.getPreviousWatermark());
        }

        // we will compute watermark and remove inactive writers concurrently
        return CompletableFuture.allOf(removeInactiveWriters, watermarkFuture.thenAccept(watermarkClient::completeIteration));
    }

    /**
     * This method takes marks (time + position) of active writers and finds greatest lower bound on time and 
     * least upper bound on positions and returns the watermark object composed of the two. 
     * The least upper bound computed from positions may not result in a consistent and complete stream cut. 
     * So, a positional upper bound is then converted into a stream cut by including segments from higher epoch. 
     * Also, it is possible that in an effort to fill missing range, we may end up creating an upper bound that 
     * is composed of segments from highest epoch. In next iteration, from new writer positions, we may be able to 
     * compute a tighter upper bound. But since watermark has to advance position and time, we will take the upper bound
     * of previous stream cut and new stream cut. 
     * 
     * @param scope scope
     * @param streamName stream name
     * @param context operation context
     * @param activeWriters marks for all active writers. 
     * @param previousWatermark previous watermark that was emitted. 
     * @return CompletableFuture which when completed will contain watermark to be emitted. 
     */
    private CompletableFuture<Watermark> computeWatermark(String scope, String streamName, OperationContext context,
                                                          List<Map.Entry<String, WriterMark>> activeWriters, Watermark previousWatermark) {
        long requestId = context.getRequestId();
        Watermark.WatermarkBuilder builder = Watermark.builder();
        ConcurrentHashMap<SegmentWithRange, Long> upperBound = new ConcurrentHashMap<>();
        
        // We are deliberately making two passes over writers - first to find lowest time. Second loop will convert writer 
        // positions to StreamSegmentRecord objects by retrieving ranges from store. And then perform computation on those 
        // objects. 

        LongSummaryStatistics summarized = activeWriters.stream().collect(Collectors.summarizingLong(x -> x.getValue().getTimestamp()));
        long lowerBoundOnTime = summarized.getMin();
        long upperBoundOnTime = summarized.getMax();
                    
        if (lowerBoundOnTime > previousWatermark.getLowerTimeBound()) {
            CompletableFuture<List<Map<SegmentWithRange, Long>>> positionsFuture = Futures.allOfWithResults(
                    activeWriters.stream().map(x -> {
                        return Futures.keysAllOfWithResults(
                                x.getValue().getPosition().entrySet().stream()
                                 .collect(Collectors.toMap(y -> getSegmentWithRange(scope, streamName, context, y.getKey()),
                                         Entry::getValue)));
                    }).collect(Collectors.toList()));
            log.debug(requestId, "Emitting watermark for stream {}/{} with time {}", scope, streamName, lowerBoundOnTime);
            return positionsFuture.thenAccept(listOfPositions -> listOfPositions.forEach(position -> {
                // add writer positions to upperBound map. 
                addToUpperBound(position, upperBound);
            })).thenCompose(v -> computeStreamCut(scope, streamName, context, upperBound, previousWatermark)
                    .thenApply(streamCut -> builder.lowerTimeBound(lowerBoundOnTime).upperTimeBound(upperBoundOnTime)
                               .streamCut(ImmutableMap.copyOf(streamCut)).build()));
        } else {
            // new time is not advanced. No watermark to be emitted. 
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<SegmentWithRange> getSegmentWithRange(String scope, String streamName, OperationContext context, long segmentId) {
        return streamMetadataStore.getSegment(scope, streamName, segmentId, context, executor)
                .thenApply(this::transform);
    }

    /**
     * Method that updates the supplied upperBound by comparing it with supplied position such that resultant upperbound
     * is an upper bound on current position and all previously considered positions. 
     * This method should be called with each writer's position iteratively and it will update the upperBound accordingly. 
     * Note: This method is not thread safe, even though upperBound is a concurrent hash map. 
     * This method looks at the state in the map and then either adds or removes entries from the map. 
     * If this was called concurrently, then the behaviour is unpredictable. 
     * @param position position be included while computing new upper bound
     * @param upperBound existing upper bound
     */
    private void addToUpperBound(Map<SegmentWithRange, Long> position, Map<SegmentWithRange, Long> upperBound) {
        for (Map.Entry<SegmentWithRange, Long> writerPos : position.entrySet()) {
            SegmentWithRange segment = writerPos.getKey();
            long offset = writerPos.getValue();
            if (upperBound.containsKey(segment)) { // update offset if the segment is already present. 
                long newOffset = Math.max(offset, upperBound.get(segment));
                upperBound.put(segment, newOffset);
            } else if (!hasSuccessors(segment, upperBound.keySet())) { // only include segment if it doesnt have a successor already included in the set. 
                Set<SegmentWithRange> included = upperBound.keySet();
                included.forEach(x -> {
                    // remove all predecessors of `segment` from upper bound. 
                    if (segment.overlaps(x) && segment.getSegmentId() > x.getSegmentId()) {
                        upperBound.remove(x);     
                    }
                });
                // add segment to upperBound. 
                upperBound.put(segment, offset);
            }
        }
    }

    private boolean hasSuccessors(SegmentWithRange segment, Set<SegmentWithRange> included) {
        return included.stream().anyMatch(x -> segment.overlaps(x) && segment.getSegmentId() < x.getSegmentId());
    }

    /**
     * This method fills in missing ranges in the upper bound such that it composes a consistent and complete streamcut. 
     * The segments from highest epoch are included to fill in missing ranges. This could result in exclusion of segments
     * from upper bound if incoming segment also covers range from an existing segment. This could result in new missing 
     * ranges being created. So this method loops until all missing ranges are filled up. 
     * In worst case, all segments from highest epoch will end up being included in the final stream cut which is a correct
     * but not the tightest upper bound. 
     * 
     * @param scope scope
     * @param stream stream name
     * @param context operation context
     * @param upperBound upper bound of writer positions. 
     * @param previousWatermark previous watermark
     * @return CompletableFuture which when completed will contain a stream cut which completes missing ranges from upper bound
     * if any.  
     */
    private CompletableFuture<Map<SegmentWithRange, Long>> computeStreamCut(String scope, String stream, OperationContext context,
                                                                            Map<SegmentWithRange, Long> upperBound, Watermark previousWatermark) {
        ConcurrentHashMap<SegmentWithRange, Long> streamCut = new ConcurrentHashMap<>(upperBound);
        AtomicReference<Map<Double, Double>> missingRanges = new AtomicReference<>(findMissingRanges(streamCut));

        if (previousWatermark != null && !previousWatermark.equals(Watermark.EMPTY)) {
            // super impose previous watermark on the computed upper bound so that we take greatest upper bound which
            // is greater than or equal to previous watermark. 

            addToUpperBound(previousWatermark.getStreamCut(), streamCut);
        }

        return Futures.doWhileLoop(() -> {
            int highestEpoch = streamCut.keySet().stream().mapToInt(x -> NameUtils.getEpoch(x.getSegmentId()))
                                        .max().orElse(-1);
            assert highestEpoch >= 0;
            
            return streamMetadataStore.getEpoch(scope, stream, highestEpoch, context, executor)
                                       .thenApply(epochRecord -> {
                                           missingRanges.get().entrySet().forEach(missingRange -> {
                                               List<SegmentWithRange> replacement = findSegmentsForMissingRange(epochRecord, missingRange);
                                               Map<SegmentWithRange, Long> replacementSegmentOffsetMap =
                                                       replacement.stream().collect(Collectors.toMap(x -> x, x -> 0L));
                                               addToUpperBound(replacementSegmentOffsetMap, streamCut);
                                           });

                                           return missingRanges.updateAndGet(x -> findMissingRanges(streamCut));
                                       });
        }, map -> !map.isEmpty(), executor)
                .thenApply(v -> streamCut);
    }
    
    private SegmentWithRange transform(StreamSegmentRecord segment) {
        return SegmentWithRange.builder().segmentId(segment.segmentId()).rangeLow(segment.getKeyStart()).rangeHigh(segment.getKeyEnd()).build();
    }
    
    private List<SegmentWithRange> findSegmentsForMissingRange(EpochRecord epochRecord, Map.Entry<Double, Double> missingRange) {
        return epochRecord.getSegments().stream().filter(x -> x.overlaps(missingRange.getKey(), missingRange.getValue()))
                          .map(this::transform).collect(Collectors.toList());
    }

    private Map<Double, Double> findMissingRanges(Map<SegmentWithRange, Long> streamCut) {
        Map<Double, Double> missingRanges = new HashMap<>();
        List<Map.Entry<SegmentWithRange, Long>> sorted = streamCut
                .entrySet().stream().sorted(Comparator.comparingDouble(x -> x.getKey().getRangeLow())).collect(Collectors.toList());
        Map.Entry<SegmentWithRange, Long> previous = sorted.get(0);
        
        if (previous.getKey().getRangeLow() > 0.0) {
            missingRanges.put(0.0, previous.getKey().getRangeLow());
        }
        
        for (int i = 1; i < sorted.size(); i++) {
            Map.Entry<SegmentWithRange, Long> next = sorted.get(i);
            if (previous.getKey().getRangeHigh() != next.getKey().getRangeLow()) {
                missingRanges.put(previous.getKey().getRangeHigh(), next.getKey().getRangeLow());
            }
            previous = next;
        }

        if (previous.getKey().getRangeHigh() < 1.0) {
            missingRanges.put(previous.getKey().getRangeHigh(), 1.0);
        }

        return missingRanges;
    }

    @VisibleForTesting
    boolean checkExistsInCache(Stream stream) {
        return watermarkClientCache.asMap().containsKey(stream);
    }

    @VisibleForTesting
    boolean checkExistsInCache(String scope) {
        return syncFactoryCache.asMap().containsKey(scope);
    }

    @VisibleForTesting
    void evictFromCache(String scope) {
        syncFactoryCache.invalidate(scope);
    }
        
    static class WatermarkClient implements Closeable {
        private final RevisionedStreamClient<Watermark> client;
        
        private final AtomicReference<Map.Entry<Revision, Watermark>> previousWatermark = new AtomicReference<>();
        private final AtomicReference<Revision> markRevision = new AtomicReference<>();

        /**
         * Map to track inactive writers for their timeouts. This map records wall clock time when
         * this writer was found to be inactive. If in subsequent cycles
         * it continues to be inactive and the time elapsed is greater than
         * {@link StreamConfiguration#getTimestampAggregationTimeout()}, then it is declared timedout.
         */
        private final ConcurrentHashMap<String, Long> inactiveWriters;
        
        @VisibleForTesting
        WatermarkClient(Stream stream, SynchronizerClientFactory clientFactory) {
            this.client = clientFactory.createRevisionedStreamClient(
                    NameUtils.getMarkStreamForStream(stream.getStreamName()), 
                    new WatermarkSerializer(), SynchronizerConfig.builder().build());
            this.inactiveWriters = new ConcurrentHashMap<>();
        }

        Watermark getPreviousWatermark() {
            Map.Entry<Revision, Watermark> watermark = previousWatermark.get();
            return watermark == null ? Watermark.EMPTY : watermark.getValue(); 
        }

        /**
         * This method is called every time a new watermark computation needs to be performed. 
         * 
         * Only Writers that have reported time after previous watermark are considered to participate
         * in next watermark computation. Active writers are those that have either not been explicitly shutdown or timed out. 
         * 
         * A watermark is emitted only if all active writers have reported a time greater than previous watermark. 
         * The watermark computation is postponed if not all active writers have reported their marks.
         * 
         * Active Writer: Writer which has reported a time greater than the active previous watermark OR has not been 
         * timed out or shutdown. 
         * Inactive writer: Writer which has not reported a mark for at least {@link StreamConfiguration#getTimestampAggregationTimeout()}}
         * or has been explicitly shutdown.
         */
        @Synchronized
        void reinitialize() {
            Revision revision = client.getMark();
            // revision can be null if no window has been set yet. 
            if (revision == null) {
                markRevision.set(client.fetchOldestRevision());
                client.compareAndSetMark(null, markRevision.get());
            } else {
                markRevision.set(revision);
            }

            ArrayList<Entry<Revision, Watermark>> entries = Lists.newArrayList(client.readFrom(markRevision.get()));
            if (!entries.isEmpty()) {
                previousWatermark.set(entries.get(entries.size() - 1));
            } else {
                previousWatermark.set(null);
            }
        }

        /**
         * This method should be called to complete the current iteration. 
         * @param newWatermark new watermark to emit
         */
        void completeIteration(Watermark newWatermark) {
            Map.Entry<Revision, Watermark> previous = previousWatermark.get();
            if (newWatermark != null) {
                // conditional update
                Revision revision = previous == null ? markRevision.get() : previous.getKey();
                Revision newRevision = client.writeConditionally(revision, newWatermark);
                if (newRevision == null) {
                    return;
                } else if (previous != null) {
                    client.compareAndSetMark(markRevision.get(), previous.getKey());
                }
            }
        }
        
        boolean isWriterActive(Entry<String, WriterMark> writerMark, long timeout) {
            if (!isWriterParticipating(writerMark.getValue().getTimestamp())) {
                // if writer is behind previous watermark we will start tracking it for inactivity and time it out. 
                long currentTime = System.currentTimeMillis();
                inactiveWriters.putIfAbsent(writerMark.getKey(), currentTime);
                Long time = inactiveWriters.getOrDefault(writerMark.getKey(), currentTime);

                boolean timedOut = currentTime - time >= timeout;
                return writerMark.getValue().isAlive() && !timedOut;
            } else {
                inactiveWriters.remove(writerMark.getKey());
                return true;
            }
        }

        boolean isWriterParticipating(long time) {
            Map.Entry<Revision, Watermark> latest = previousWatermark.get();
            
            if (latest == null) {
                return true;
            }
            
            return time > latest.getValue().getLowerTimeBound();
        }

        private void untrackWriterInactivity(String writerId) {
            inactiveWriters.remove(writerId);
        }
        
        @VisibleForTesting
        boolean isWriterTracked(String writerId) {
            return inactiveWriters.containsKey(writerId);
        }

        @Override
        public void close() {
            this.client.close();
        }
    }
}
