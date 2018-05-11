/**
  * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import lombok.Cleanup;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SerializationTest {

    @Test
    public void testPosition() {
        PositionImpl pos = new PositionImpl(ImmutableMap.of(Segment.fromScopedName("foo/bar/1"), 2L));
        ByteBuffer bytes = pos.toBytes();
        Position pos2 = Position.fromBytes(bytes);
        assertEquals(pos, pos2);
    }
    
    @Test
    public void testStreamCut() {
        StreamCutImpl cut = new StreamCutImpl(Stream.of("Foo/Bar"), ImmutableMap.of(Segment.fromScopedName("Foo/Bar/1"), 3L));
        ByteBuffer bytes = cut.toBytes();
        StreamCut cut2 = StreamCut.fromBytes(bytes);
        assertEquals(cut, cut2);
        
        bytes = StreamCut.UNBOUNDED.toBytes();
        assertEquals(StreamCut.UNBOUNDED, StreamCut.fromBytes(bytes));
        assertNotEquals(cut, StreamCut.UNBOUNDED);
    }
    
    @Test
    public void testCheckpoint() {
        CheckpointImpl checkpoint = new CheckpointImpl("checkpoint", ImmutableMap.of(Segment.fromScopedName("Foo/Bar/1"), 3L));
        ByteBuffer bytes = checkpoint.toBytes();
        Checkpoint checkpoint2 = Checkpoint.fromBytes(bytes);
        assertEquals(checkpoint, checkpoint2);
    }
    
    @Test
    public void testEventPointer() {
        EventPointerImpl pointer = new EventPointerImpl(Segment.fromScopedName("foo/bar/1"), 1000L, 100);
        String string = pointer.toString();
        ByteBuffer bytes = pointer.toBytes();
        assertEquals(pointer, EventPointer.fromBytes(bytes));
        assertEquals(pointer, EventPointerImpl.fromString(string));
    }
    
    @Test
    public void testStream() {
        Stream stream = Stream.of("foo/bar");
        assertEquals("foo/bar", stream.getScopedName());  
    }
    
    @Test
    public void testSegment() {
        Segment segmnet = Segment.fromScopedName("foo/bar/2");
        assertEquals("foo/bar/2", segmnet.getScopedName());   
    }
    
    @Test
    public void testSequence() throws IOException, ClassNotFoundException {
        Sequence sequence = Sequence.create(1, 2);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oout = new ObjectOutputStream(bout);
        oout.writeObject(sequence);
        byte[] byteArray = bout.toByteArray();
        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        Object sequence2 = oin.readObject();
        assertEquals(sequence, sequence2);
    }
    
}


