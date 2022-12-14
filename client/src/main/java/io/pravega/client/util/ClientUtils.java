package io.pravega.client.util;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.shared.watermarks.SegmentWithRange;
import io.pravega.shared.watermarks.Watermark;

import java.util.LinkedHashMap;
import java.util.Map;

class ClientUtils {  // TODO : pkg protected
    public static StreamCut getStreamCutFromWaterMark(Watermark watermark) {
        Stream stream = Stream.of(watermark.getScope(), watermark.getStream());
        Map<Segment, Long> position = new LinkedHashMap<>();
        for (Map.Entry<SegmentWithRange, Long> entry : watermark.getStreamCut().entrySet()) {
            position.put(new Segment(watermark.getScope(), watermark.getStream(), entry.getKey().getSegmentId()), entry.getValue());
        }
        return new StreamCutImpl(stream, position);
    }
}
