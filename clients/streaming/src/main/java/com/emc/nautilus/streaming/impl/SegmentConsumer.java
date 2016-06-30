package com.emc.nautilus.streaming.impl;

import com.emc.nautilus.logclient.EndOfSegmentException;
import com.emc.nautilus.streaming.SegmentId;

public interface SegmentConsumer<Type> {
    SegmentId getLogId();

    Type getNextEvent(long timeout) throws EndOfSegmentException;

    long getOffset();

    void setOffset(long offset);

    void close();
}
