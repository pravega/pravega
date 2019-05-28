/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import lombok.Synchronized;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.AbstractMap.*;

public class PeriodicWatermarking {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PeriodicWatermarking.class));
    private static final int MAX_CACHE_SIZE = 1000;
    public static final long DELAY = 30000L;
    private final StreamMetadataStore streamMetadataStore;
    private final BucketStore bucketStore;
    private final ScheduledExecutorService executor;
    private final LoadingCache<Stream, WatermarkClient> watermarkClientCache;

    public PeriodicWatermarking(StreamMetadataStore streamMetadataStore, BucketStore bucketStore,
                                ClientConfig clientConfig, ScheduledExecutorService executor) {
        this(streamMetadataStore, bucketStore, stream -> new WatermarkClient(stream, clientConfig), executor);
    }

    @VisibleForTesting
    public PeriodicWatermarking(StreamMetadataStore streamMetadataStore, BucketStore bucketStore,
                                Function<Stream, WatermarkClient> watermarkClientSupplier, ScheduledExecutorService executor) {
        this.streamMetadataStore = streamMetadataStore;
        this.bucketStore = bucketStore;
        this.executor = executor;
        this.watermarkClientCache = CacheBuilder.newBuilder()
                                                .maximumSize(MAX_CACHE_SIZE)
                                                .expireAfterAccess(10, TimeUnit.MINUTES)
                                                .removalListener((RemovalListener<Stream, WatermarkClient>) notification -> {
                                                    notification.getValue().client.close();
                                                })
                                                .build(new CacheLoader<Stream, WatermarkClient>() {
                                                    @ParametersAreNonnullByDefault
                                                    @Override
                                                    public WatermarkClient load(final Stream stream) {
                                                        return watermarkClientSupplier.apply(stream);
                                                    }
                                                });
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
        OperationContext context = streamMetadataStore.createContext(scope, streamName);

        log.debug("Periodic background processing for watermarking called for stream {}/{}",
                scope, streamName);
        
        return Futures.exceptionallyExpecting(streamMetadataStore.getAllWritersMarks(scope, streamName, context, executor),
                           e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, Collections.emptyMap())
                           .thenCompose(writers -> {
                               WatermarkClient watermarkClient = watermarkClientCache.getUnchecked(stream);
                               watermarkClient.reinitialize();

                               // 1. filter writers that are active.
                               List<Entry<String, WriterMark>> activeWriters = new ArrayList<>();
                               List<Entry<String, WriterMark>> stoppedAndInactive = new ArrayList<>();
                               AtomicBoolean allActiveAreParticipating = new AtomicBoolean(true);
                               writers.entrySet().forEach(x -> {
                                   if (watermarkClient.isWriterActive(x.getValue().getTimestamp())) {
                                       activeWriters.add(x);
                                       if (!watermarkClient.isWriterParticipating(x.getValue().getTimestamp())) {
                                           allActiveAreParticipating.set(false);
                                       }
                                   } else if (!x.getValue().isAlive()){
                                       stoppedAndInactive.add(x);
                                   }
                               });

                               // Stop all inactive writers that have been shutdown.
                               CompletableFuture<List<Void>> removeStoppedAndInactiveWriters = 
                                       Futures.allOfWithResults(stoppedAndInactive.stream().map(x ->
                                               Futures.exceptionallyExpecting(
                                                       streamMetadataStore.removeWriter(scope, streamName, x.getKey(),
                                                       x.getValue(), context, executor), 
                                                       e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, null))
                                                                                  .collect(Collectors.toList()));
                               
                               if (activeWriters.isEmpty()) {
                                   // this will prevent the periodic cycles being spent in running watermarking workflow for a silent stream. 
                                   // as soon as any writer reports its mark, stream will be added to bucket and background 
                                   // periodic processing will resume.
                                   return removeStoppedAndInactiveWriters
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
                               return CompletableFuture.allOf(removeStoppedAndInactiveWriters, watermarkFuture.thenAccept(watermarkClient::completeIteration));
                           })
                .exceptionally(e -> {
                    log.warn("Exception thrown while trying to perform periodic watermark computation. Logging and ignoring.", e); 
                    return null;
                });
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
        Watermark.WatermarkBuilder builder = Watermark.builder();
        ConcurrentHashMap<StreamSegmentRecord, Long> upperBound = new ConcurrentHashMap<>();
        
        // We are deliberately making two passes over writers - first to find lowest time. Second loop will convert writer 
        // positions to StreamSegmentRecord objects by retrieving ranges from store. And then perform computation on those 
        // objects. 

        long lowerBoundOnTime = activeWriters.stream().min(Comparator.comparingLong(x -> x.getValue().getTimestamp()))
                     .map(x -> x.getValue().getTimestamp()).orElse(Long.MIN_VALUE);
                    
        if (lowerBoundOnTime > previousWatermark.getTimestamp()) {
            CompletableFuture<List<Map<StreamSegmentRecord, Long>>> positionsFuture = Futures.allOfWithResults(
                    activeWriters.stream().map(x -> {
                        return Futures.keysAllOfWithResults(
                                x.getValue().getPosition().entrySet().stream()
                                 .collect(Collectors.toMap(y -> streamMetadataStore.getSegment(scope, streamName, y.getKey(), context, executor),
                                         Entry::getValue)));
                    }).collect(Collectors.toList()));
            
            return positionsFuture.thenAccept(listOfPositions -> listOfPositions.forEach(position -> {
                // add writer positions to upperBound map. 
                addToUpperBound(position, upperBound);
            })).thenCompose(v -> computeStreamCut(scope, streamName, context, upperBound, previousWatermark)
                    .thenApply(streamCut -> builder.timestamp(lowerBoundOnTime).streamCut(ImmutableMap.copyOf(streamCut)).build()));
        } else {
            // new time is not advanced. No watermark to be emitted. 
            return CompletableFuture.completedFuture(null);
        }
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
    private void addToUpperBound(Map<StreamSegmentRecord, Long> position, Map<StreamSegmentRecord, Long> upperBound) {
        for (Map.Entry<StreamSegmentRecord, Long> writerPos : position.entrySet()) {
            StreamSegmentRecord segment = writerPos.getKey();
            long offset = writerPos.getValue();
            if (upperBound.containsKey(segment)) { // update offset if the segment is already present. 
                long newOffset = Math.max(offset, upperBound.get(segment));
                upperBound.put(segment, newOffset);
            } else if (!hasSuccessors(segment, upperBound.keySet())) { // only include segment if it doesnt have a successor already included in the set. 
                Set<StreamSegmentRecord> included = upperBound.keySet();
                included.forEach(x -> {
                    // remove all predecessors of `segment` from upper bound. 
                    if (segment.overlaps(x) && segment.segmentId() > x.segmentId()) {
                        upperBound.remove(x);     
                    }
                });
                // add segment to upperBound. 
                upperBound.put(segment, offset);
            }
        }
    }

    private boolean hasSuccessors(StreamSegmentRecord segment, Set<StreamSegmentRecord> included) {
        return included.stream().anyMatch(x -> segment.overlaps(x) && segment.segmentId() < x.segmentId());
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
    private CompletableFuture<Map<Long, Long>> computeStreamCut(String scope, String stream, OperationContext context,
                                                                Map<StreamSegmentRecord, Long> upperBound, Watermark previousWatermark) {
        ConcurrentHashMap<StreamSegmentRecord, Long> streamCut = new ConcurrentHashMap<>(upperBound);
        AtomicReference<Map<Double, Double>> missingRanges = new AtomicReference<>(findMissingRanges(streamCut));

        CompletableFuture<Void> previousFuture;
        if (previousWatermark != null && !previousWatermark.equals(Watermark.EMPTY)) {
            // super impose previous watermark on the computed upper bound so that we take greatest upper bound which
            // is greater than or equal to previous watermark. 
            previousFuture = Futures.keysAllOfWithResults(
                    previousWatermark.getStreamCut().entrySet().stream()
                                     .collect(Collectors.toMap(x -> streamMetadataStore.getSegment(scope, stream, x.getKey(), context, executor),
                                             Entry::getValue)))
                    .thenAccept(previous -> addToUpperBound(previous, streamCut));
        } else {
            previousFuture = CompletableFuture.completedFuture(null);
        }

        return previousFuture.thenCompose(previous -> Futures.doWhileLoop(() -> {
            int highestEpoch = streamCut.keySet().stream().mapToInt(StreamSegmentRecord::getCreationEpoch).max().orElse(-1);
            assert highestEpoch >= 0;
            
            return streamMetadataStore.getEpoch(scope, stream, highestEpoch, context, executor)
                                       .thenApply(epochRecord -> {
                                           missingRanges.get().entrySet().forEach(missingRange -> {
                                               List<StreamSegmentRecord> replacement = findSegmentsForMissingRange(epochRecord, missingRange);
                                               Map<StreamSegmentRecord, Long> replacementSegmentOffsetMap =
                                                       replacement.stream().collect(Collectors.toMap(x -> x, x -> 0L));
                                               addToUpperBound(replacementSegmentOffsetMap, streamCut);
                                           });

                                           missingRanges.set(findMissingRanges(streamCut));
                                           return missingRanges.get();
                                       });
        }, map -> !map.isEmpty(), executor)
                .thenApply(v -> streamCut.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), Entry::getValue))));
    }

    private List<StreamSegmentRecord> findSegmentsForMissingRange(EpochRecord epochRecord, Map.Entry<Double, Double> missingRange) {
        return epochRecord.getSegments().stream().filter(x -> x.overlaps(missingRange.getKey(), missingRange.getValue()))
                .collect(Collectors.toList());
    }

    private Map<Double, Double> findMissingRanges(Map<StreamSegmentRecord, Long> streamCut) {
        Map<Double, Double> missingRanges = new HashMap<>();
        List<Map.Entry<StreamSegmentRecord, Long>> sorted = streamCut
                .entrySet().stream().sorted(Comparator.comparingDouble(x -> x.getKey().getKeyStart())).collect(Collectors.toList());
        Map.Entry<StreamSegmentRecord, Long> previous = sorted.get(0);
        
        if (previous.getKey().getKeyStart() > 0.0) {
            missingRanges.put(0.0, previous.getKey().getKeyStart());
        }
        
        for (int i = 1; i < sorted.size(); i++) {
            Map.Entry<StreamSegmentRecord, Long> next = sorted.get(i);
            if (previous.getKey().getKeyEnd() != next.getKey().getKeyStart()) {
                missingRanges.put(previous.getKey().getKeyEnd(), next.getKey().getKeyStart());
            }
            previous = next;
        }

        if (previous.getKey().getKeyEnd() < 1.0) {
            missingRanges.put(previous.getKey().getKeyEnd(), 1.0);
        }

        return missingRanges;
    }

    static class WatermarkClient {
        private static final int WINDOW_SIZE = 2;
        private final RevisionedStreamClient<Watermark> client;
        
        @GuardedBy("$Lock")
        private List<Map.Entry<Revision, Watermark>> entries;
        private AtomicInteger windowStart;
        private final int windowSize;

        WatermarkClient(Stream stream, ClientConfig clientConfig) {
            this(stream, SynchronizerClientFactory.withScope(stream.getScope(), clientConfig));
        }
        
        @VisibleForTesting
        WatermarkClient(Stream stream, SynchronizerClientFactory clientFactory) {
            this.client = clientFactory.createRevisionedStreamClient(
                    StreamSegmentNameUtils.getMarkSegmentForStream(stream.getScope(), stream.getStreamName()), 
                    new WatermarkSerializer(), SynchronizerConfig.builder().build());
            this.windowStart = new AtomicInteger();
            windowSize = WINDOW_SIZE;
        }

        @Synchronized
        private Map.Entry<Revision, Watermark> getLatestEntry() {
            return entries.isEmpty() ? null : entries.get(entries.size() - 1);
        }

        @Synchronized
        Watermark getPreviousWatermark() {
            Map.Entry<Revision, Watermark> watermark = getLatestEntry();
            return watermark == null ? Watermark.EMPTY : watermark.getValue(); 
        }

        @Synchronized
        private Map.Entry<Revision, Watermark> getEntryAt(int index) {
            return index < 0 ? null : entries.get(index);
        }

        /**
         * This method is called every time a new watermark computation needs to be performed. It resets the window 
         * and latest watermark. 
         * We maintain an Active window of a maximum window size of 2. It spans from latest watermark to up to two preceding 
         * watermarks. 
         * 
         * Only Writers that have reported time after the start of active window are considered to be active and participate
         * in next watermark computation.
         * 
         * A watermark is emitted only if all active writers have reported a time greater than previous watermark. 
         * The watermark computation is postponed if not all active writers have reported their marks.
         * We keep shrinking the active window in each iteration. 
         * This may result in writers becoming inactive and hence not a participant in current watermark computation.
         * If active window only contains latest mark and none of the writers have progressed beyond last watermark, 
         * then we shutdown the stream from periodic watermarking computation. 
         * 
         * Active Writer: Writer which has reported a time greater than the active window. 
         * Active window: A reference to an existing older watermark.   
         * Only active writers are considered for emitting watermarks. A watermark is emitted only if all participating 
         * writers have reported times greater than last watermark.  
         * Active window is progressed forward in each iteration conditionally. We try to maintain an active window of 
         * last two watermarks. 
         * Bootstrap: If there are less than 2 watermarks, active window is null. If no new watermark is emitted in current 
         * iteration then active window is progressed by one. If watermark is emitted and the distance between active window
         * watermark and latest watermark is `2` or more, active window is progressed.  
         * If distance between active window watermark and latest window watermark is less than 2, 
         * active window is not progressed if a new watermark is emitted. 
         */
        @Synchronized
        void reinitialize() {
            Revision revision = client.getMark();
            // revision can be null if no window has been set yet. 
            entries = Lists.newArrayList(client.readFrom(revision));
            
            // If there are no watermarks or there is no previous window recorded, in either case we set watermark start 
            // to MIN_VALUE
            int index = entries.isEmpty() || revision == null ? Integer.MIN_VALUE : 0;
            
            if (entries.size() > windowSize) {
                index = entries.size() - windowSize - 1;
            }
            
            windowStart.set(index);
        }

        /**
         * This method should be called to complete the current iteration. 
         * if watermark is emitted, progress window. 
         * @param newWatermark
         */
        void completeIteration(Watermark newWatermark) {
            Map.Entry<Revision, Watermark> latest = getLatestEntry();
            Revision newRevision;
            if (newWatermark != null) {
                // conditional update
                Revision revision = latest == null ? null : latest.getKey();
                newRevision = client.writeConditionally(revision, newWatermark);
                if (newRevision == null) {
                    return;
                }
            }

            int start = windowStart.get();
            int newWindowStart = entries.size() - windowSize;
            
            Map.Entry<Revision, Watermark> window = getEntryAt(start);

            Revision key = window == null ? null : window.getKey();
            
            int nextIndex = start;

            if (newWatermark != null) {
                // watermark has been emitted. 
                // if window = null. set window start if number of entries are sufficient.  

                // if previous window doesn't exist - set window if progressing window maintains at least a distance of `windowSize`.
                if (start < 0 && newWindowStart >= 0) {
                    nextIndex = newWindowStart;
                }

                if (start >= 0 && entries.size() - start >= windowSize) {
                    nextIndex = entries.size() - windowSize;
                }
            } else {
                // no watermark is emitted. We should progress window.
                nextIndex = start + 1 < entries.size() ? start + 1 : start;
                
                nextIndex = nextIndex < 0 ? 0 : nextIndex;
            }

            Map.Entry<Revision, Watermark> next = getEntryAt(nextIndex);
            if (next != null) {
                // set the window to next.revision
                client.compareAndSetMark(key, next.getKey());
            }
        }
        
        boolean isWriterActive(long time) {
            Map.Entry<Revision, Watermark> active = getEntryAt(windowStart.get());
            
            if (active == null) {
                return true;
            }
            
            return time > active.getValue().getTimestamp();
        }

        boolean isWriterParticipating(long time) {
            Map.Entry<Revision, Watermark> latest = getLatestEntry();
            
            if (latest == null) {
                return true;
            }
            
            return time > latest.getValue().getTimestamp();
        }
    }
}
