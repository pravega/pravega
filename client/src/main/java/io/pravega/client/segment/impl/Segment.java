/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import com.google.common.base.Strings;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import java.io.Serializable;

import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Data;
import lombok.NonNull;

/**
 * An identifier for a segment of a stream.
 */
@Data
public class Segment implements Serializable, Comparable<Segment> {
    private static final long serialVersionUID = 1L;
    private final String scope;
    @NonNull
    private final String streamName;
    private final long segmentId;

    /**
     * Creates a new instance of Segment class.
     *
     * @param scope      The scope string the segment belongs to.
     * @param streamName The stream name that the segment belongs to.
     * @param number     ID number for the segment.
     */
    public Segment(String scope, String streamName, long number) {
        this.scope = scope;
        this.streamName = streamName;
        this.segmentId = number;
    }

    public String getScopedStreamName() {
        return StreamSegmentNameUtils.getScopedStreamName(scope, streamName);
    }

    public String getScopedName() {
        return StreamSegmentNameUtils.getScopedName(scope, streamName, segmentId);
    }

    public Stream getStream() {
        return new StreamImpl(scope, streamName);
    }

    /**
     * Parses fully scoped name, and creates the segment.
     *
     * @param qualifiedName Fully scoped segment name
     * @return Segment name.
     */
    public static Segment fromScopedName(String qualifiedName) {
        if (qua)
        String[] tokens = qualifiedName.split("[/#]");
        if (tokens.length == 3) {
            return new Segment(null, tokens[0], Integer.parseInt(tokens[1]));
        } else if (tokens.length >= 4) {
            return new Segment(tokens[0], tokens[1], StreamSegmentNameUtils.computeSegmentId(Integer.parseInt(tokens[2]), Integer.parseInt(tokens[2])));
        } else {
            throw new IllegalArgumentException("Not a valid segment name");
        }
    }

    @Override
    public int compareTo(Segment o) {
        int result = scope.compareTo(o.scope);
        if (result == 0) {
            result = streamName.compareTo(o.streamName);
        }
        if (result == 0) {
            result = Long.compare(segmentId, o.segmentId);
        }
        return result;
    }
}
