package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.StreamCut;
import java.util.Map;

public abstract class StreamCutInternal implements StreamCut {

    @Override
    public StreamCutInternal asImpl() {
        return this;
    }

    abstract Map<Segment, Long> getPositions();
    
}
