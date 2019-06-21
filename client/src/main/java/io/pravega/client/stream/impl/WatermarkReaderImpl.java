package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.shared.watermarks.Watermark;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WatermarkReaderImpl {

    private final Stream stream;
    private final RevisionedStreamClient<Watermark> client; //StreamSegmentNameUtils.getMarkForStream(streamName)    
    
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final ArrayDeque<Watermark> inflight = new ArrayDeque<>();
    @GuardedBy("lock")
    private long passedTimestamp;
    @GuardedBy("lock")
    private Revision location;
    
    /**
     * Advances the position in the watermark segment based on the provided position.
     * 
     * @param position - The position of the local reader.
     * @param minPeerTimestamp - The oldest timestamp reported by any reader in the reader group. (This forms a lower bound of 
     * 
     */
    public void advanceTo(PositionInternal position, long minPeerTimestamp) {
        // while inflight . first < position
        // remove item
        // while inflight window . last <= position 
        // add new item to list if overlaping with position 
        synchronized (lock) {
            while (!inflight.isEmpty()) {
                int compare = compare(position, inflight.getFirst());
                if (compare > 0) {
                    passedTimestamp = inflight.removeFirst().getLowerTimeBound();
                } else {
                    break;
                }
            }
        }
        while (doMoreMarksNeedToBeRead(position)) {
            Watermark mark = fetchNextMark();
            if (mark == null) {
                break;
            }
            if (compare(position, mark) <= 0) {
                synchronized (lock) {
                    inflight.addLast(mark);
                }
            }
        }
    }

    private boolean doMoreMarksNeedToBeRead(PositionInternal position) {
        synchronized (lock) {
            return inflight.isEmpty() || compare(position, inflight.getLast()) < 0;
        }
    }
    
    private Watermark fetchNextMark() {
client.readFrom(start)

//TODO: Problem with concurrent calls. THey really should be queued so that we don't need to run them in parallel. 
//Ideally this should not block the thread calling advance. A simple solution would be to use a thread pool to internally fetch the next mark and updat the inflight.
//So this whole thing needs to be made async to avoid blocking the thread pool thread. But revision stream client is not async.
        return null;
    }

    public long getLocalMinTimestamp() {
        return passedTimestamp;
    }
    
    public TimeWindow getTimeWindow() {
        // return min time for inflight.0 and max time for inflight[length-2]
    }
    
    //Need to deal with case where time windows skip over watermark in which case there is only one inflight.
    //It by definition is greater than the last seen in the inflight.
    //So instead we should have one pre-inflight and one post-inflight.
    //Then the timewindow min is the min of the pre-inflight max and the min inflight min
    //the and the window max is the max of the post-inflight min and the max inflight max
    //in the event we are at the tail there may be no post inflight and the Upper limit to the time window may be meaningless. 
    
    // Need to add tests assert these properties.
    
    private int compare(PositionInternal pos, Watermark mark) {
        Map<SegmentWithRange, Long> left = pos.getOwnedSegmentRangesWithOffsets();
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
