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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCutImpl;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.*;

public class StreamCutTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    @Test
    public void testValidStreamCut() {
        StreamCut sc = new StreamCutImpl(Stream.of(SCOPE, STREAM),
                ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 10L));
        String textualRepresentation = "scope/stream;0=10";
        assertEquals(textualRepresentation, sc.toString());
        assertEquals(sc, StreamCut.of(textualRepresentation));

        StreamCut sc2 = new StreamCutImpl(Stream.of(SCOPE, STREAM),
                ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 10L,
                        new Segment(SCOPE, STREAM, 1), 15L));
        String textualRepresentation2 = "scope/stream;0=10;1=15";
        assertEquals(textualRepresentation2, sc2.toString());
        assertEquals(sc2, StreamCut.of(textualRepresentation2));
    }

    @Test
    public void testInvalidStreamCutRepresentation() {
        assertThrows(IllegalArgumentException.class, () -> StreamCut.of("scope/stream"));
        assertThrows(IllegalArgumentException.class, () -> StreamCut.of("scope/stream;"));
        assertThrows(IllegalArgumentException.class, () -> StreamCut.of("scope/stream;0"));
        assertThrows(IllegalArgumentException.class, () -> StreamCut.of("scopestream;0=20"));
        assertThrows(IllegalArgumentException.class, () -> StreamCut.of("scope/stream;0=-20"));
        assertThrows(IllegalArgumentException.class, () -> StreamCut.of("scope/stream;0=20;-1=10"));
    }
}
