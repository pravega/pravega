/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This is an abstract class which acts an intermediate class to make the actual StreamCut implementation opaque.
 */
public abstract class StreamCutInternal implements StreamCut {
    
    /**
     * Get {@link Stream} for the StreamCut.
     * @return The stream.
     */
    public abstract Stream getStream();

    /**
     * Get a mapping of Segment and its offset.
     * @return Map of Segment to its offset.
     */
    public abstract Map<Segment, Long> getPositions();
    
    
    public static StreamCutInternal fromBytes(ByteBuffer cut) {
        return StreamCutImpl.fromBytes(cut);
    }

    public static StreamCutInternal from(String cut) {
        return StreamCutImpl.from(cut);
    }

}
