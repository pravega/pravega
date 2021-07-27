package io.pravega.storage.chunk;

import io.pravega.segmentstore.storage.SegmentHandle;

public class ECSChunkSegmentHandle implements SegmentHandle {
    @Override
    public String getSegmentName() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
