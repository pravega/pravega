/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.client.stream.impl;

import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.Segment;

import java.util.Map;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class CheckpointImpl implements Checkpoint {

    private static final long serialVersionUID = 1L;
    @Getter
    private final String name;
    @Getter(value = AccessLevel.PACKAGE)
    private final Map<Segment, Long> positions;
    
    @Override
    public CheckpointImpl asImpl() {
        return this;
    }

}
