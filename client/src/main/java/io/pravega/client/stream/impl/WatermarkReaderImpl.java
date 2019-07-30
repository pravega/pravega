package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.concurrent.LatestItemSequentialProcessor;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.shared.watermarks.Watermark;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

public class WatermarkReaderImpl {

    private final Stream stream;
    private final WatermarkFetcher fetcher;
    private final LatestItemSequentialProcessor<Map<SegmentWithRange, Long>> processor;
    
    
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final ArrayDeque<Watermark> inflight = new ArrayDeque<>();
    @GuardedBy("lock")
    private long passedTimestamp;
    
    @RequiredArgsConstructor
    private static class WatermarkFetcher {
        private final RevisionedStreamClient<Watermark> client; 
        @GuardedBy("$lock")
        private Revision location = null;
        @GuardedBy("$lock")
        private Iterator<Entry<Revision, Watermark>> iter = null;
        
        /**
         * This returns the next mark in the stream. It holds onto iterator between calls as this will safe a metadata check on the length.
         */
        @Synchronized
        private Watermark fetchNextMark() {
            if (iter != null && iter.hasNext()) {
                Entry<Revision, Watermark> next = iter.next();
                location = next.getKey();
                return next.getValue();
            }
            iter = client.readFrom(location);
            if (iter.hasNext()) {
                Entry<Revision, Watermark> next = iter.next();
                location = next.getKey();
                return next.getValue();
            }
            return null;
        }
    }

    private final Consumer<Map<SegmentWithRange, Long>> processFunction = new Consumer<Map<SegmentWithRange, Long>>() {

        @Override
        public void accept(Map<SegmentWithRange, Long> position) {
            while (!inflight.isEmpty()) {
                int compare = compare(position, inflight.getFirst());
                if (compare > 0) {
                    passedTimestamp = inflight.removeFirst().getLowerTimeBound();
                } else {
                    break;
                }
            }
            while (inflight.isEmpty() || compare(position, inflight.getLast()) < 0) {
                Watermark mark = fetcher.fetchNextMark();
                if (mark == null) {
                    break;
                }
                if (compare(position, mark) <= 0) {
                    inflight.addLast(mark);
                }
            }
        }
    };

    
    /**
     * Creates a Reader to keep track of the current time window for a given stream.
     * 
     * @param stream The stream to read the water marks for.
     * @param clientFactory A factory to create the water mark reader. NOTE: This must use the same scope as the `stream` passed.
     * @param executor A threadpool to perform background operations.
     */
    public WatermarkReaderImpl(Stream stream, SynchronizerClientFactory clientFactory, Executor executor) {
        this.stream = Preconditions.checkNotNull(stream);       
        
        SynchronizerConfig config = SynchronizerConfig.builder().readBufferSize(4096).build();
        this.fetcher = new WatermarkFetcher(clientFactory.createRevisionedStreamClient(StreamSegmentNameUtils.getMarkForStream(stream.getStreamName()),
                                                                                       new WatermarkSerializer(),
                                                                                       config));
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
        if (inflight.isEmpty()) {
            return new TimeWindow(passedTimestamp, null);
        }
        
        return new TimeWindow(inflight.getFirst().getLowerTimeBound(), inflight.getLast().getUpperTimeBound());
    }

    
    //TODO watermarking: Need to add tests assert these properties.
    
    private int compare(Map<SegmentWithRange, Long> readerGroupPosition, Watermark mark) {
        Map<SegmentWithRange, Long> left = readerGroupPosition;
        Map<SegmentWithRange, Long> right = new HashMap<>();
        for (Entry<io.pravega.shared.watermarks.SegmentWithRange, Long> entry : mark.getStreamCut().entrySet()) {
            Segment segment = new Segment(stream.getScope(), stream.getStreamName(), entry.getKey().getSegmentId());
            right.put(new SegmentWithRange(segment, entry.getKey().getRangeLow(), entry.getKey().getRangeHigh()),
                      entry.getValue());
        }
        return compare(left, right);
    }
    
    private static int compare(Map<SegmentWithRange, Long> left, Map<SegmentWithRange, Long> right) {
        boolean leftBelowRight = false;
        boolean leftAboveRight = false;
        for (Entry<SegmentWithRange, Long> entry : left.entrySet()) {
            SegmentWithOffset matching = findOverlappingSegmentIn(entry.getKey(), right);
            if (matching != null) {
                SegmentWithOffset leftSegment = new SegmentWithOffset(entry.getKey().getSegment(), entry.getValue());
                int compairson = leftSegment.compareTo(matching);
                if (compairson > 0) {
                    leftAboveRight = true;
                } else if (compairson < 0) {
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
        return 0;
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
            return Long.compare(offset, o.offset);
        }
    }
    
    private static SegmentWithOffset findOverlappingSegmentIn(SegmentWithRange segment, Map<SegmentWithRange, Long> ranges) {
        for (Entry<SegmentWithRange, Long> entry : ranges.entrySet()) {
            if (entry.getKey().getRange().overlapsWith(segment.getRange())) {
                return new SegmentWithOffset(entry.getKey().getSegment(), entry.getValue());
            }
        }
        return null;
    }
   
}
