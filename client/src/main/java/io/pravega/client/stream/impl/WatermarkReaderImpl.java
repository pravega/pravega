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
package io.pravega.client.stream.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.common.concurrent.LatestItemSequentialProcessor;
import io.pravega.shared.watermarks.Watermark;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

public class WatermarkReaderImpl implements AutoCloseable {

    private final Stream stream;
    private final WatermarkFetcher fetcher;
    private final LatestItemSequentialProcessor<Map<SegmentWithRange, Long>> processor;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final ArrayDeque<Watermark> inflight = new ArrayDeque<>();
    @GuardedBy("lock")
    private Long passedTimestamp = null;
    
    @RequiredArgsConstructor
    private static class WatermarkFetcher implements AutoCloseable {
        private final RevisionedStreamClient<Watermark> client; 
        @GuardedBy("$lock")
        private Revision location = null;
        @GuardedBy("$lock")
        private Iterator<Entry<Revision, Watermark>> iter = null;

        /**
         * This returns the next mark in the stream. It holds onto iterator between calls as this will save a metadata check on the length.
         */
        @Synchronized
        private Watermark fetchNextMark() {
            if (iter != null && iter.hasNext()) {
                Entry<Revision, Watermark> next = iter.next();
                location = next.getKey();
                return next.getValue();
            }
            if (location == null) {
                location = client.fetchOldestRevision();
            }
            iter = client.readFrom(location);
            if (iter.hasNext()) {
                Entry<Revision, Watermark> next = iter.next();
                location = next.getKey();
                return next.getValue();
            }
            return null;
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private final Consumer<Map<SegmentWithRange, Long>> processFunction = new Consumer<Map<SegmentWithRange, Long>>() {

        @Override
        public void accept(Map<SegmentWithRange, Long> position) {
            synchronized (lock) {
                while (!inflight.isEmpty()) {
                    int compare = compare(stream, position, inflight.getFirst());
                    if (compare > 0) {
                        passedTimestamp = inflight.removeFirst().getLowerTimeBound();
                    } else {
                        break;
                    }
                }
                while (inflight.isEmpty() || compare(stream, position, inflight.getLast()) >= 0) {
                    Watermark mark = fetcher.fetchNextMark();
                    if (mark == null) {
                        break;
                    }
                    if (compare(stream, position, mark) <= 0) {
                        inflight.addLast(mark);
                    } else {
                        passedTimestamp = mark.getLowerTimeBound();
                    }
                }
            }
        }
    };

    /**
     * Creates a Reader to keep track of the current time window for a given stream.
     * 
     * @param stream The stream to read the water marks for.
     * @param markClient The segment to read marks from.
     * @param executor A threadpool to perform background operations.
     */
    public WatermarkReaderImpl(Stream stream, RevisionedStreamClient<Watermark> markClient, Executor executor) {
        this.stream = Preconditions.checkNotNull(stream);       
        this.fetcher = new WatermarkFetcher(markClient);
        this.processor = new LatestItemSequentialProcessor<>(processFunction, Preconditions.checkNotNull(executor));     
    }
    
    /**
     * Advances the position in the watermark segment based on the provided position.
     * 
     * @param readerGroupPosition - The latest read positions of the open segments in the reader group.
     */
    public void advanceTo(Map<SegmentWithRange, Long> readerGroupPosition) {
        processor.updateItem(readerGroupPosition);      
    }

    
    /**
     * Returns the current time window of the reader group for the associated stream.
     * The value returned will only change after calling {@link #advanceTo(Map)}.
     * @return TimeWindow the bounds on the upper and lower times provided by the writer at the time of writing.
     */
    public TimeWindow getTimeWindow() {
        synchronized (lock) {
            Long upperBound = inflight.isEmpty() ? null : inflight.getLast().getUpperTimeBound();
            return new TimeWindow(passedTimestamp, upperBound);
        }
    }

    @VisibleForTesting
    static int compare(Stream stream, Map<SegmentWithRange, Long> readerGroupPosition, Watermark mark) {
        Map<SegmentWithRange, Long> left = readerGroupPosition;
        Map<SegmentWithRange, Long> right = new HashMap<>();
        for (Entry<io.pravega.shared.watermarks.SegmentWithRange, Long> entry : mark.getStreamCut().entrySet()) {
            Segment segment = new Segment(stream.getScope(), stream.getStreamName(), entry.getKey().getSegmentId());
            right.put(new SegmentWithRange(segment, entry.getKey().getRangeLow(), entry.getKey().getRangeHigh()),
                      entry.getValue());
        }
        boolean leftBelowRight = false;
        boolean leftAboveRight = false;
        for (Entry<SegmentWithRange, Long> entry : left.entrySet()) {
            List<SegmentWithOffset> matching = findOverlappingSegmentIn(entry.getKey(), right);
            for (SegmentWithOffset match : matching) {
                SegmentWithOffset leftSegment = new SegmentWithOffset(entry.getKey().getSegment(), entry.getValue());
                int comparison = leftSegment.compareTo(match);
                if (comparison > 0) {
                    leftAboveRight = true;
                } else if (comparison < 0) {
                    leftBelowRight = true;
                }
            }
        }
        if (leftBelowRight && leftAboveRight) {
            return 0;
        }
        if (leftBelowRight) {
            return -1;
        }
        if (leftAboveRight) {
            return 1;
        }
        return 1; // If the reader is exactly at a mark, time should advance.
    }
    
    @Data
    private static final class SegmentWithOffset implements Comparable<SegmentWithOffset> {
        private final Segment segment;
        private final long offset;
        
        @Override
        public int compareTo(SegmentWithOffset o) {
            int result = segment.compareTo(o.segment);
            if (result != 0) {
                return result;
            }
            if (offset <= -1 && o.offset >= 0) {
                return 1;
            }
            if (offset >= 0 && o.offset <= -1) {
                return -1;
            }
            return Long.compare(offset, o.offset);
        }
    }
    
    private static List<SegmentWithOffset> findOverlappingSegmentIn(SegmentWithRange segment, Map<SegmentWithRange, Long> ranges) {
        if (ranges.containsKey(segment)) {
            Long offset = ranges.get(segment);
            return Collections.singletonList(new SegmentWithOffset(segment.getSegment(), offset));
        }
        List<SegmentWithOffset> result = new ArrayList<>();
        for (Entry<SegmentWithRange, Long> entry : ranges.entrySet()) {
            if (segment.getRange() == null || entry.getKey().getRange().overlapsWith(segment.getRange())) {
                result.add(new SegmentWithOffset(entry.getKey().getSegment(), entry.getValue()));
            }
        }
        return result;
    }

    @Override
    public void close() {
        fetcher.close();
    }
   
}
