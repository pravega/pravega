package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    
}


