/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventPointer;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Sequence;

import lombok.Data;

@Data
public class EventReadImpl<T> implements EventRead<T> {
    private final Sequence eventSequence;
    private final T event;
    private final Position position;
    private final EventPointer eventPointer;
    private final boolean routingRebalance;
    private final String checkpointName;
    
    @Override
    public boolean isCheckpoint() {
        return checkpointName != null;
    }
}
