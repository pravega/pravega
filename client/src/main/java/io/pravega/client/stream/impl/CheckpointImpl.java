/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class CheckpointImpl implements Checkpoint {

    private static final long serialVersionUID = 1L;
    @Getter
    private final String name;
    @Getter
    private final Map<Stream, StreamCut> positions;
    
    CheckpointImpl(String name, Map<Segment, Long> segmentPositions) {
        this.name = name;
        Map<Stream, ImmutableMap.Builder<Segment, Long>> streamPositions = new HashMap<>();
        for (Entry<Segment, Long> position : segmentPositions.entrySet()) {
            streamPositions.computeIfAbsent(position.getKey().getStream(),
                                            k -> new ImmutableMap.Builder<Segment, Long>())
                           .put(position);
        }
        ImmutableMap.Builder<Stream, StreamCut> positionBuilder = ImmutableMap.builder();
        for (Entry<Stream, Builder<Segment, Long>> streamPosition : streamPositions.entrySet()) {
            positionBuilder.put(streamPosition.getKey(),
                                new StreamCutImpl(streamPosition.getKey(), streamPosition.getValue().build()));
        }
        this.positions = positionBuilder.build();
    }
    
    @Override
    public CheckpointImpl asImpl() {
        return this;
    }    
}
