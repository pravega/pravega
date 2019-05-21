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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import lombok.Synchronized;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        this.streamMetadataStore = streamMetadataStore;
        this.bucketStore = bucketStore;
        this.executor = executor;
        this.watermarkClientCache = CacheBuilder.newBuilder()
                                                .maximumSize(MAX_CACHE_SIZE)
                                                .expireAfterAccess(10, TimeUnit.MINUTES)
                                                .build(new CacheLoader<Stream, WatermarkClient>() {
                                                    @ParametersAreNonnullByDefault
                                                    @Override
                                                    public WatermarkClient load(final Stream stream) {
                                                        return new WatermarkClient(stream, clientConfig);
                                                    }
                                                });
    }
    
    public CompletableFuture<Void> watermark(Stream stream) {
        String scope = stream.getScope();
        String streamName = stream.getStreamName();
        OperationContext context = streamMetadataStore.createContext(scope, streamName);

        log.debug("Periodic background processing for watermarking called for stream {}/{}",
                scope, streamName);

        // TODO: shivesh
        WatermarkClient watermarkClient = watermarkClientCache.getUnchecked(stream);
        watermarkClient.reinitialize();
        
        return Futures.exceptionallyExpecting(streamMetadataStore.getAllWritersMarks(scope, streamName, context, executor),
                           e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, Collections.emptyMap())
                           .thenCompose(writers -> {
                               // 1. filter writers that are active.
                               List<Map.Entry<String, WriterMark>> activeWriters = new ArrayList<>();
                               AtomicBoolean allActiveAreParticipating = new AtomicBoolean(true);
                               writers.entrySet().forEach(x -> {
                                   if (watermarkClient.isWriterActive(x.getValue().getTimestamp())) {
                                       activeWriters.add(x);
                                       if (!watermarkClient.isWriterParticipating(x.getValue().getTimestamp())) {
                                           allActiveAreParticipating.set(false);
                                       }
                                   }
                               });

                               if (activeWriters.isEmpty()) {
                                   // this will prevent the periodic cycles being spent in running watermarking workflow for a silent stream. 
                                   // as soon as any writer reports its mark, stream will be added to bucket and background 
                                   // periodic processing will resume. 
                                   return bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.WatermarkingService,
                                           scope, streamName, executor);
                               } 

                               if (!allActiveAreParticipating.get()) {
                                   // there are active writers that have not reported their marks. We should wait 
                                   // until they either report or become inactive. So we will complete this iteration and
                                   // check if these writers have made progress. 
                                   watermarkClient.complete(null);
                                   return CompletableFuture.completedFuture(null);
                               } else {
                                   // compute new mark
                                   return computeWatermark(activeWriters).thenAccept(watermarkClient::complete);
                               }
                           })
                .exceptionally(e -> {
                    // TODO: shivesh
                    // log and ignore. watermarking will be attempted in the next iteration. 
                    return null;
                });
    }

    private CompletableFuture<Watermark> computeWatermark(List<Map.Entry<String, WriterMark>> activeWriters) {
        Watermark.WatermarkBuilder builder = Watermark.builder();
        Map<StreamSegmentRecord, Long> upperBound = new HashMap<>();
        
        AtomicLong lowestTime = new AtomicLong(Long.MAX_VALUE);
        activeWriters.stream().forEach(x -> {
            // take the lower bound on time on all writers
            if (x.getValue().getTimestamp() < lowestTime.get()) {
                lowestTime.set(x.getValue().getTimestamp());
            }
            
            x.getValue().getPosition().entrySet().stream().forEach(entry -> {
                if (currentOffsets.containsKey(entry.getKey())) {
                    
                }
            });
            
        });
                           
                           
    }

    void addToUpperBound(Map<StreamSegmentRecord, Long> segmentOffsetMap, Map<StreamSegmentRecord, Long> upperBound) {
        for (Entry<Segment, Long> entry : segmentOffsetMap) {
            Segment s = entry.key;
            Long offset = entry.value;
            if (upperBound.containsKey(s)) { // update offset if the segment is already present. 
                long newOffset = Math.max(p.getOffsetFor(s), upperBound.get(s));
                upperBound.put(s, newOffset);
            } else if (!hasSuccessors(s, upperBound.keys)) { // only include segment if it doesnt have a successor already included in the set. 
                list<Segment> toReplace = findPredecessors(s, upperBound.keys()); // replace any predecessor segments from the result set. 
                upperBound.removeAllKeys(toReplace);
                currentOffsets.put(s, offset);
            }
        }
    }

    static class WatermarkClient {
        private static final int WINDOW_SIZE = 2;
        private final RevisionedStreamClient<Watermark> client;
        
        @GuardedBy("$Lock")
        private List<Map.Entry<Revision, Watermark>> entries;
        private AtomicInteger windowStart;
        private final int windowSize;

        WatermarkClient(Stream stream, ClientConfig clientConfig) {
            SynchronizerClientFactory clientFactory = SynchronizerClientFactory.withScope(stream.getScope(), clientConfig);
            this.client =  clientFactory.createRevisionedStreamClient(stream.getScopedName(), 
                    new WatermarkSerializer(), SynchronizerConfig.builder().build());
            this.windowStart = new AtomicInteger();
            windowSize = WINDOW_SIZE;
        }

        @Synchronized
        private Map.Entry<Revision, Watermark> getLatestWatermark() {
            return entries.isEmpty() ? null : entries.get(entries.size() - 1);
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
            
            if (entries.size() >= windowSize) {
                index = entries.size() - windowSize;
            }
            
            windowStart.set(index);
        }

        /**
         * This method should be called to complete the current iteration. 
         * if watermark is emitted, progress window. 
         * @param newWatermark
         */
        void complete(Watermark newWatermark) {
            Map.Entry<Revision, Watermark> latest = getLatestWatermark();
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
                    nextIndex = entries.size() - start;
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
            
            return time >= active.getValue().getTimestamp();
        }

        boolean isWriterParticipating(long time) {
            Map.Entry<Revision, Watermark> latest = getLatestWatermark();
            
            if (latest == null) {
                return true;
            }
            
            return time >= latest.getValue().getTimestamp();
        }
    }
    
    static class WatermarkSerializer implements Serializer<Watermark> {

        @Override
        public ByteBuffer serialize(Watermark value) {
            return value.toByteBuf();
        }

        @Override
        public Watermark deserialize(ByteBuffer serializedValue) {
            return Watermark.fromByteBuf(serializedValue);
        }
    }
}
