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
import io.pravega.client.stream.EventPointer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventPointerTest {

    /**
     * Simple exercise of event pointers. The test case
     * creates an impl instance with some arbitrary values,
     * serializes the pointer, deserializes it, and asserts
     * that the values obtained are the expected ones.
     */
    @Test
    public void testEventPointerImpl() {
        String scope = "testScope";
        String stream = "testStream";
        int segmentId = 1;
        Segment segment = new Segment(scope, stream, segmentId);
        EventPointer pointer = new EventPointerImpl(segment, 10L, 10);
        ByteArrayOutputStream bos = null;

        // Serialize pointer
        try {
            bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(pointer);
            out.close();
        } catch (IOException e) {
            fail("Failed to serialize pointer object" + e.getMessage());
        }

        // Deserialize pointer
        pointer = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream in = new ObjectInputStream(bis);
            pointer = (EventPointer) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            fail("Failed to deserialize object" + e.getMessage());
        }

        // Verifies that we have been able to deserialize successfully
        if (pointer == null) {
            fail("Failed to deserialize object");
        } else {
            StringBuilder name = new StringBuilder();
            name.append(scope);
            name.append("/");
            name.append(stream);
            assertTrue("Scoped stream name: " + pointer.asImpl().getSegment().getScopedStreamName(),
                        pointer.asImpl().getSegment().getScopedStreamName().equals(name.toString()));

            name.append("/");
            name.append(segmentId);
            assertTrue("Scoped name: " + pointer.asImpl().getSegment().getScopedName(),
                        pointer.asImpl().getSegment().getScopedName().equals(name.toString()));

            assertTrue(pointer.asImpl().getEventStartOffset() == 10L);
            assertTrue(pointer.asImpl().getEventLength() == 10);
        }
    }
}
