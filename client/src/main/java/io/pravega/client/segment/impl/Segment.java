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
import java.io.ObjectStreamException;
import java.io.Serializable;
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
    private final int segmentNumber;

    /**
     * Creates a new instance of Segment class.
     *
     * @param scope      The scope string the segment belongs to.
     * @param streamName The stream name that the segment belongs to.
     * @param number     ID number for the segment.
     */
    public Segment(String scope, String streamName, int number) {
        this.scope = scope;
        this.streamName = streamName;
        this.segmentNumber = number;
    }

    public String getScopedStreamName() {
        StringBuffer sb = new StringBuffer();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        return sb.toString();
    }

    public String getScopedName() {
        return getScopedName(scope, streamName, segmentNumber);
    }

    public static String getScopedName(String scope, String streamName, int segmentNumber) {
        StringBuffer sb = new StringBuffer();
        if (!Strings.isNullOrEmpty(scope)) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        sb.append('/');
        sb.append(segmentNumber);
        return sb.toString();
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
        String[] tokens = qualifiedName.split("[/#]");
        if (tokens.length == 2) {
            return new Segment(null, tokens[0], Integer.parseInt(tokens[1]));
        } else if (tokens.length >= 3) {
            return new Segment(tokens[0], tokens[1], Integer.parseInt(tokens[2]));
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
            result = Integer.compare(segmentNumber, o.segmentNumber);
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
