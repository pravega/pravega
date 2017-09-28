package io.pravega.client.batch;

import java.util.Iterator;

public interface SegmentIterator<T> extends Iterator<T>, AutoCloseable {

    /**
     * Provides an offset which can be used to re-create a segmentIterator at this position by
     * calling
     * {@link BatchClient#readSegment(io.pravega.client.segment.impl.Segment, io.pravega.client.stream.Serializer, long)}
     * 
     * @return The current offset in the segment
     */
    long getOffset();
    
    /**
     * Closes the iterator, freeing any resources associated with it.
     * @see java.lang.AutoCloseable#close()
     */
    void close();
    
}
