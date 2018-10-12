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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ReaderGroupState.AcquireSegment;
import io.pravega.client.stream.impl.ReaderGroupState.AddReader;
import io.pravega.client.stream.impl.ReaderGroupState.CheckpointReader;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpointsBefore;
import io.pravega.client.stream.impl.ReaderGroupState.CompactReaderGroupState;
import io.pravega.client.stream.impl.ReaderGroupState.CompactReaderGroupState.CompactReaderGroupStateBuilder;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupInitSerializer;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateInit;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupUpdateSerializer;
import io.pravega.client.stream.impl.ReaderGroupState.ReleaseSegment;
import io.pravega.client.stream.impl.ReaderGroupState.RemoveReader;
import io.pravega.client.stream.impl.ReaderGroupState.SegmentCompleted;
import io.pravega.client.stream.impl.ReaderGroupState.UpdateDistanceToTail;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.util.ByteArraySegment;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import lombok.Cleanup;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SerializationTest {
    
    private final Random r = RandomFactory.create();

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
        Segment segmnet = Segment.fromScopedName("foo/bar/2.#epoch.0");
        assertEquals("foo/bar/2.#epoch.0", segmnet.getScopedName());
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

    @Test
    public void testReaderGroupInit() throws Exception {
        ReaderGroupInitSerializer initSerializer = new ReaderGroupInitSerializer();
        ReaderGroupConfig config = ReaderGroupConfig.builder()
                                                    .disableAutomaticCheckpoints()
                                                    .groupRefreshTimeMillis(r.nextInt(1000))
                                                    .stream(createSegment().getStream())
                                                    .build();
        verify(initSerializer, new ReaderGroupStateInit(config, createSegmentToLongMap(), createSegmentToLongMap()));
        CompactReaderGroupStateBuilder builder = new CompactReaderGroupState.CompactReaderGroupStateBuilder();
        builder.assignedSegments(createMap(this::createString, this::createSegmentToLongMap));
        builder.checkpointState(new CheckpointState.CheckpointStateBuilder().checkpoints(createList(this::createString))
                                                                            .lastCheckpointPosition(createSegmentToLongMap())
                                                                            .checkpointPositions(createMap(this::createString,
                                                                                                           this::createSegmentToLongMap))
                                                                            .uncheckpointedHosts(createMap(this::createString,
                                                                                                           this::createStringList))
                                                                            .build());
        builder.config(config);
        builder.distanceToTail(createMap(this::createString, r::nextLong));
        builder.endSegments(createSegmentToLongMap());
        builder.unassignedSegments(createSegmentToLongMap());
        builder.futureSegments(createMap(this::createSegment, () -> new HashSet<>(createLongList())));
        verify(initSerializer, builder.build());

    }
    
    @Test
    public void testReaderGroupUpdates() throws Exception {
        ReaderGroupUpdateSerializer serializer = new ReaderGroupUpdateSerializer();
        verify(serializer, new AddReader(createString()));
        verify(serializer, new RemoveReader(createString(), createSegmentToLongMap()));
        verify(serializer, new ReleaseSegment(createString(), createSegment(), r.nextLong()));
        verify(serializer, new AcquireSegment(createString(), createSegment()));
        verify(serializer, new UpdateDistanceToTail(createString(), r.nextLong()));
        verify(serializer, new SegmentCompleted(createString(), createSegment(),
                                                createMap(this::createSegment, this::createLongList)));
        verify(serializer, new CheckpointReader(createString(), createString(), createSegmentToLongMap()));
        verify(serializer, new CreateCheckpoint(createString(), RandomUtils.nextBoolean()));
        verify(serializer, new ClearCheckpointsBefore(createString()));
    }
    
    private void verify(ReaderGroupInitSerializer serializer, InitialUpdate<ReaderGroupState> value) throws IOException {
        ByteArraySegment bytes = serializer.serialize(value);
        Update<ReaderGroupState> deserialized = serializer.deserialize(bytes);
        assertEquals(value, deserialized);
    }
    
    private void verify(ReaderGroupUpdateSerializer serializer, Update<ReaderGroupState> value) throws IOException {
        ByteArraySegment bytes = serializer.serialize(value);
        Update<ReaderGroupState> deserialized = serializer.deserialize(bytes);
        assertEquals(value, deserialized);
    }

    private List<Long> createLongList() throws Exception {
        return createList(r::nextLong);
    }
    
    private List<String> createStringList() throws Exception {
        return createList(this::createString);
    }

    private <V> List<V> createList(Callable<V> valueGen) throws Exception {
        int size = r.nextInt(3);
        ImmutableList.Builder<V> builder = ImmutableList.builder();
        for (int i = 0; i < size; i++) {
            builder.add(valueGen.call());
        }
        return builder.build();
    }

    private Map<Segment, Long> createSegmentToLongMap() throws Exception {
        return createMap(this::createSegment, r::nextLong);
    }

    private <K, V> Map<K, V> createMap(Callable<K> keyGen, Callable<V> valueGen) throws Exception {
        int size = r.nextInt(3);
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (int i = 0; i < size; i++) {
            builder.put(keyGen.call(), valueGen.call());
        }
        return builder.build();
    }

    private Segment createSegment() {
        return new Segment(createString(), createString(), r.nextInt(100));
    }

    private String createString() {
        return RandomStringUtils.randomAlphabetic(5);
    }
}

