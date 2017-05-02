/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.impl;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.Position;

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
