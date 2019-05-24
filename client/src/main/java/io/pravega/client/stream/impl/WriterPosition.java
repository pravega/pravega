package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.ToString;

@Builder
@ToString
public class WriterPosition {

    private final Map<Segment, Long> segments;

    public Map<Segment, Long> getSegmentsWithOffsets() {
        return Collections.unmodifiableMap(segments);
    }

}