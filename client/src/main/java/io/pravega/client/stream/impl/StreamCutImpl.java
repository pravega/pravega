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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ToStringUtils;
import java.io.ObjectStreamException;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

@Data
public class StreamCutImpl extends StreamCutInternal {

    private final Stream stream;
    @Getter(value = AccessLevel.PACKAGE)
    private final Map<Segment, Long> positions;

    @Override
    public String toString() {
        return ToStringUtils.mapToString(positions);
    }

    public static StreamCut fromString(String string) {
        Map<Segment, Long> positions = ToStringUtils.stringToMap(string, Segment::fromScopedName, Long::parseLong);
        Exceptions.checkNotNullOrEmpty(positions, "positions");
        return new StreamCutImpl(verifySameStream(positions), positions);
    }
    
    private static Stream verifySameStream(Map<Segment, Long> positions) {
        String scopedName = null;
        for (Segment s : positions.keySet()) {
            if (scopedName == null) {
                scopedName = s.getScopedStreamName();
            }
            if (!scopedName.equals(s.getScopedStreamName())) {
                throw new IllegalArgumentException("All segments must come from the same stream. Found: " + scopedName
                        + " and " + s);
            }
        }
        return Stream.fromScopedName(scopedName);
    }


    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(toString());
    }

    @Data
    private static class SerializedForm  {
        private final String value;
        Object readResolve() throws ObjectStreamException {
            return StreamCutImpl.fromString(value);
        }
    }

}
