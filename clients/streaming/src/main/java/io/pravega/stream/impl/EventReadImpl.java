/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import io.pravega.stream.EventPointer;
import io.pravega.stream.EventRead;
import io.pravega.stream.Position;
import io.pravega.stream.Sequence;

import lombok.Data;

@Data
public class EventReadImpl<T> implements EventRead<T> {
    private final Sequence eventSequence;
    private final T event;
    private final Position position;
    private final EventPointer eventPointer;
    private final String checkpointName;
    
    @Override
    public boolean isCheckpoint() {
        return checkpointName != null;
    }
}
