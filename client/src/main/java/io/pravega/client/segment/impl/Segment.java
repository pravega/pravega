/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;

import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Data;
import lombok.NonNull;

/**
 * An identifier for a segment of a stream.
 */
@Data
public class Segment implements Comparable<Segment>, Serializable {
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
     * @param id     ID number for the segment.
     */
    public Segment(String scope, String streamName, long id) {
        this.scope = scope;
        this.streamName = streamName;
        this.segmentId = id;
    }

    public String getScopedStreamName() {
        return StreamSegmentNameUtils.getScopedStreamName(scope, streamName);
    }

    public String getScopedName() {
        return StreamSegmentNameUtils.getQualifiedStreamSegmentName(scope, streamName, segmentId);
    }

    public Stream getStream() {
        return new StreamImpl(scope, streamName);
    }

    @Override
    public String toString() {
        return getScopedName();
    }
    
    /**
     * Parses fully scoped name, and creates the segment.
     *
     * @param qualifiedName Fully scoped segment name
     * @return Segment name.
     */
    public static Segment fromScopedName(String qualifiedName) {
        if (StreamSegmentNameUtils.isTransactionSegment(qualifiedName)) {
            String originalSegmentName = StreamSegmentNameUtils.getParentStreamSegmentName(qualifiedName);
            return fromScopedName(originalSegmentName);
        } else {
            List<String> tokens = StreamSegmentNameUtils.extractSegmentTokens(qualifiedName);
            if (tokens.size() == 2) { // scope not present
                String scope = null;
                String streamName = tokens.get(0);
                long segmentId = Long.parseLong(tokens.get(1));
                return new Segment(scope, streamName, segmentId);
            } else { // scope present
                String scope = tokens.get(0);
                String streamName = tokens.get(1);
                long segmentId = Long.parseLong(tokens.get(2));
                return new Segment(scope, streamName, segmentId);
            }
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
    
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(getScopedName());
    }
    
    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String value;
        Object readResolve() throws ObjectStreamException {
            return Segment.fromScopedName(value);
        }
    }
}
