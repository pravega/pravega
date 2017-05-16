package io.pravega.segmentstore.storage.impl.nfs;

import io.pravega.segmentstore.storage.SegmentHandle;

import java.nio.channels.AsynchronousFileChannel;

public class NFSSegmentHandle implements SegmentHandle {
    private final String segmentName;
    private final boolean isReadOnly;
    public final AsynchronousFileChannel channel;

    public NFSSegmentHandle(String streamSegmentName, AsynchronousFileChannel channel, boolean isReadOnly) {
        this.segmentName = streamSegmentName;
        this.channel = channel;
        this.isReadOnly = isReadOnly;
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    public static NFSSegmentHandle getReadHandle(String streamSegmentName, AsynchronousFileChannel channel) {
        return new NFSSegmentHandle(streamSegmentName, channel, true);
    }

    public static NFSSegmentHandle getWriteHandle(String streamSegmentName, AsynchronousFileChannel channel) {
        return new NFSSegmentHandle(streamSegmentName, channel, false);
    }
}
