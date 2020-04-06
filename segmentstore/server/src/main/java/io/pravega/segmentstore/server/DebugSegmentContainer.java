package io.pravega.segmentstore.server;

import java.util.concurrent.CompletableFuture;

public interface DebugSegmentContainer extends SegmentContainer{

    CompletableFuture<Void> createStreamSegment(String streamSegmentName, int length, boolean isSealed);
}