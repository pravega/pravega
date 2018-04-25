/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamCutInternal;
import io.pravega.common.Exceptions;
import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A set of segment/offset pairs for a single stream that represent a consistent position in the
 * stream. (IE: Segment 1 and 2 will not both appear in the set if 2 succeeds 1, and if 0 appears
 * and is responsible for keyspace 0-0.5 then other segments covering the range 0.5-1.0 will also be
 * included.)
 */
public interface StreamCut extends Serializable {

    /**
     * This is used represents an unbounded StreamCut. This is used when the user wants to refer to the current HEAD
     * of the stream or the current TAIL of the stream.
     */
    StreamCut UNBOUNDED = () -> null;

    /**
     * Used internally. Do not call.
     *
     * @return Implementation of EventPointer interface
     */
    StreamCutInternal asImpl();

    /**
     * Helper utility to create a {@link StreamCut} from its string representation.
     * @param textualRepresentation String representation of StreamCut.
     * @return StreamCut.
     */
    static StreamCut of(String textualRepresentation) {
        Exceptions.checkNotNullOrEmpty(textualRepresentation, "textualRepresentation");
        String[] split = textualRepresentation.split(";", 2);
        Preconditions.checkArgument(split.length == 2, "Invalid string representation of StreamCut");

        final Stream stream = Stream.of(split[0]);
        final Map<Segment, Long> positions = Splitter.on(';').omitEmptyStrings().trimResults().withKeyValueSeparator('=')
                                                     .split( Exceptions.checkNotNullOrEmpty(split[1], "positions"))
                                                     .entrySet().stream()
                                                     .collect(Collectors.toMap(o -> new Segment(stream.getScope(),
                                                                     stream.getStreamName(), Integer.valueOf(o.getKey())),
                                                             o -> {
                                                                 long offset = Long.valueOf(o.getValue());
                                                                 Preconditions.checkArgument(offset >= 0, "Offset should be >= 0");
                                                                 return offset;
                                                             }));
        return new StreamCutImpl(stream, positions);
    }
}
