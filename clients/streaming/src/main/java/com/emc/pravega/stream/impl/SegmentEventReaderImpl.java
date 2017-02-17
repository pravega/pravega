/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;

/**
 * Reads items from the segmentInputStream.
 */
public class SegmentEventReaderImpl implements SegmentEventReader {

    private final Segment segmentId;
    @GuardedBy("in")
    private final SegmentInputStream in;

    SegmentEventReaderImpl(Segment segmentId, SegmentInputStream in) {
        this.segmentId = segmentId;
        this.in = in;
    }

    @Override
    public ByteBuffer getNextEvent(long timeout) throws EndOfSegmentException {
        ByteBuffer buffer;
        synchronized (in) { 
            buffer = in.read();
        }
        return buffer;
    }

    @Override
    public long getOffset() {
        synchronized (in) {
            return in.getOffset();
        }
    }

    @Override
    public void setOffset(long offset) {
        synchronized (in) {
            in.setOffset(offset);
        }
    }

    @Override
    public void close() {
        synchronized (in) {
            in.close();
        }
    }

    @Override
    public Segment getSegmentId() {
        return segmentId;
    }
    
    @Override
    public boolean canReadWithoutBlocking() {
        synchronized (in) {
            return in.canReadWithoutBlocking();
        }
    }
}
