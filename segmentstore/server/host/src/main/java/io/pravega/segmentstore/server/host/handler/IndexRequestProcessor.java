package io.pravega.segmentstore.server.host.handler;

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.NameUtils;
import java.time.Duration;

public final class IndexRequestProcessor {

    
    public static void locateOffsetForStream(StreamSegmentStore store, String stream, long targetOffset) {
        String indexSegmentName = NameUtils.getIndexSegmentName(stream);
        store.read(indexSegmentName, targetOffset, 16, Duration.)
    }
    
    
}
