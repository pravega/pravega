package com.emc.pravega.stream.impl;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.concurrent.atomic.AtomicReference;

import com.emc.pravega.common.hash.HashHelper;
import com.emc.pravega.stream.EventRouter;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamSegments;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EventRouterImpl implements EventRouter {

    private final Stream stream;
    private final Controller controller;
    private final AtomicReference<StreamSegments> currentSegments = new AtomicReference<>();
    private final HashHelper hasher = HashHelper.seededWith("EventRouter");

    
    @Override
    public Segment getSegmentForEvent(String routingKey) {
        StreamSegments streamSegments = currentSegments.get();
        if (streamSegments == null) {
            refreshSegmentList();
            streamSegments = currentSegments.get();
        }
        return streamSegments.getSegmentForKey(hasher.hashToRange(routingKey));
    }
    
    @Override
    public void refreshSegmentList() {
        currentSegments.set(getAndHandleExceptions(controller.getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new));
    }
 
}
