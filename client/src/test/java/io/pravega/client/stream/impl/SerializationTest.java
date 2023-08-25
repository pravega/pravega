/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
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
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupStateInit.ReaderGroupStateInitSerializer;
import io.pravega.client.stream.impl.ReaderGroupState.ReaderGroupUpdateSerializer;
import io.pravega.client.stream.impl.ReaderGroupState.ReleaseSegment;
import io.pravega.client.stream.impl.ReaderGroupState.RemoveReader;
import io.pravega.client.stream.impl.ReaderGroupState.SegmentCompleted;
import io.pravega.client.stream.impl.ReaderGroupState.UpdateDistanceToTail;
import io.pravega.client.stream.impl.ReaderGroupState.UpdateDistanceToTail.UpdateDistanceToTailSerializer;
import io.pravega.client.stream.impl.ReaderGroupState.UpdatingConfig;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
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
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.val;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class SerializationTest {
    
    private final Random r = RandomFactory.create();

    @Test
    public void testPosition() {
        PositionImpl pos = new PositionImpl(ImmutableMap.of(new SegmentWithRange(Segment.fromScopedName("foo/bar/0"), 0, 0.5), 9999999L,
                new SegmentWithRange(Segment.fromScopedName("foo/bar/1"), 0.5, 1.0), -1L));
        ByteBuffer bytes = pos.toBytes();
        Position pos1 = Position.fromBytes(bytes);
        assertEquals(pos, pos1);
    }

    @Test
    public void testPositionImplBackwardCompatibility() throws Exception {
        PositionImpl pos = new PositionImpl(ImmutableMap
                .of(new SegmentWithRange(Segment.fromScopedName("foo/bar/1"), 0, 1), 9999999L));

        // Serialize via the old serialization logic.
        // Note: the older serialization logic does not work with -1L as offset.
        final byte[] bufOld = new PositionSerializerR1().serialize(new PositionR1(pos)).array();
        // deserialize it using latest revision and ensure compatibility.
        Position newp = Position.fromBytes(ByteBuffer.wrap(bufOld));
        assertEquals(pos, newp);
    }

    @Test
    public void testPositionImplForwardCompatibility() throws Exception {
        PositionImpl pos = new PositionImpl(ImmutableMap
                .of(new SegmentWithRange(Segment.fromScopedName("foo/bar/1"), 0, 1), 9999999L));
        ByteBuffer newBuf = pos.toBytes();

        // deserialize it using the old serialization logic and ensure compatibility.
        // Note: The old serialization logic does not allow users to serialize / deserialize with -1L as offset.
        PositionSerializerR1 oldSerializer = new PositionSerializerR1();

        PositionR1 pos1 = new PositionR1(null, null);
        oldSerializer.deserialize(new ByteArraySegment(newBuf.array()), pos1);
        assertEquals(pos, pos1.getPostionImpl());
    }

    // Mutable class to validate older serialization.
    @AllArgsConstructor
    private class PositionR1 {

        private Map<Segment, Long> ownedSegments;
        private Map<Segment, SegmentWithRange.Range> segmentRanges;

        public PositionR1(PositionImpl impl) {
            this.ownedSegments = impl.getOwnedSegmentsWithOffsets();
            this.segmentRanges = impl.getOwnedSegmentRangesWithOffsets().keySet().stream()
                                     .collect(Collectors
                                             .toMap(SegmentWithRange::getSegment, SegmentWithRange::getRange));
        }

        public PositionImpl getPostionImpl() {
            return new PositionImpl(ownedSegments, segmentRanges, null);
        }
    }

    // Serializer to simiulate revision 1 of serialization.
    private static class PositionSerializerR1 extends VersionedSerializer.Direct<PositionR1> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {

            version(0).revision(0, this::write00, this::read00)
                      .revision(1, this::write01, this::read01);
        }

        private void write00(PositionR1 position, RevisionDataOutput revisionDataOutput) throws IOException {
            Map<Segment, Long> map = position.ownedSegments;
            revisionDataOutput.writeMap(map, (out, s) -> out.writeUTF(s.getScopedName()),
                    (out, offset) -> out.writeCompactLong(offset));

        }

        private void write01(PositionR1 position, RevisionDataOutput revisionDataOutput) throws IOException {
            Map<Segment, SegmentWithRange.Range> m1 = position.segmentRanges;
            revisionDataOutput
                    .writeMap(m1, (out, s) -> out.writeUTF(s.getScopedName()), PositionSerializerR1::writeRange);
        }

        private void read00(RevisionDataInput revisionDataInput, PositionR1 target) throws IOException {
            Map<Segment, Long> map = revisionDataInput
                    .readMap(in -> Segment.fromScopedName(in.readUTF()), RevisionDataInput::readCompactLong);
            target.ownedSegments = map;
        }

        private void read01(RevisionDataInput revisionDataInput, PositionR1 target) throws IOException {
            Map<Segment, SegmentWithRange.Range> map1 = revisionDataInput
                    .readMap(in -> Segment.fromScopedName(in.readUTF()), PositionSerializerR1::readRange);
            target.segmentRanges = map1;
        }

        private static void writeRange(RevisionDataOutput out, SegmentWithRange.Range range) throws IOException {
            double low, high;
            if (range == null) {
                low = -1;
                high = -1;
            } else {
                low = range.getLow();
                high = range.getHigh();
            }
            out.writeDouble(low);
            out.writeDouble(high);
        }

        private static SegmentWithRange.Range readRange(RevisionDataInput in) throws IOException {
            double low = in.readDouble();
            double high = in.readDouble();
            return (low < 0 || high < 0) ? null : new SegmentWithRange.Range(low, high);
        }
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
        verify(initSerializer, new ReaderGroupStateInit(config, createSegmentRangeMap(), createSegmentToLongMap(), false));
        CompactReaderGroupStateBuilder builder = new CompactReaderGroupState.CompactReaderGroupStateBuilder();
        builder.assignedSegments(createMap(this::createString, this::createSegmentRangeMap));
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
        builder.unassignedSegments(createSegmentRangeMap());
        builder.futureSegments(createMap(this::createSegmentWithRange, () -> new HashSet<>(createLongList())));
        builder.lastReadPosition(createSegmentRangeMap());
        verify(initSerializer, builder.build());

        ReaderGroupStateInitSerializer newSerializer = new ReaderGroupStateInit.ReaderGroupStateInitSerializer(); 
        ReaderGroupStateInitSerializer oldSerializer = new ReaderGroupStateInit.ReaderGroupStateInitSerializer() {
            @Override
            protected void declareVersions() {
                version(0).revision(0,  this::write00, this::read00);
            }
        };
        ReaderGroupStateInit init = new ReaderGroupStateInit(config, createSegmentRangeMap(), createSegmentToLongMap(), false);
        ReaderGroupStateInit oldFormat = newSerializer.deserialize(oldSerializer.serialize(init));
        assertEquals(init.getStartingSegments()
                         .keySet().stream().map(s -> s.getSegment()).collect(Collectors.toSet()),
                     oldFormat.getStartingSegments()
                         .keySet().stream().map(s -> s.getSegment()).collect(Collectors.toSet()));
        assertEquals(init.getEndSegments(), oldFormat.getEndSegments());
        assertEquals(init.getConfig(), oldFormat.getConfig());
    }
    
    @Test
    public void testReaderGroupUpdates() throws Exception {
        ReaderGroupUpdateSerializer serializer = new ReaderGroupUpdateSerializer();
        verify(serializer, new AddReader(createString()));
        verify(serializer, new RemoveReader(createString(), createSegmentToLongMap()));
        verify(serializer, new ReleaseSegment(createString(), createSegment(), r.nextLong()));
        verify(serializer, new AcquireSegment(createString(), createSegment()));
        verify(serializer, new UpdateDistanceToTail(createString(), r.nextLong(), createSegmentRangeMap()));
        verify(serializer, new SegmentCompleted(createString(), createSegmentWithRange(),
                                                createMap(this::createSegmentWithRange, this::createLongList)));
        verify(serializer, new CheckpointReader(createString(), createString(), createSegmentToLongMap()));
        verify(serializer, new CreateCheckpoint(createString()));
        verify(serializer, new ClearCheckpointsBefore(createString()));
        verify(serializer, new UpdatingConfig(r.nextBoolean()));
        verify(serializer, new ReaderGroupState.RemoveOutstandingCheckpoints());
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateDistanceToTail() throws Exception {
        UpdateDistanceToTailSerializer serializer = new UpdateDistanceToTail.UpdateDistanceToTailSerializer();
        UpdateDistanceToTail update = new UpdateDistanceToTail(createString(), r.nextLong(), createSegmentRangeMap());
        UpdateDistanceToTail deserialized = serializer.deserialize(serializer.serialize(update));
        assertEquals(deserialized, update);

        UpdateDistanceToTailSerializer oldSerializer = new UpdateDistanceToTailSerializer() {
            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }
        };
        UpdateDistanceToTail oldStyleUpdate = serializer.deserialize(oldSerializer.serialize(update));
        assertEquals(update.getReaderId(), oldStyleUpdate.getReaderId());
        assertEquals(update.getDistanceToTail(), oldStyleUpdate.getDistanceToTail());
        assertEquals(null, oldStyleUpdate.getLastReadPositions());

        // Change the state to reflect the update
        val segmentToOffsets = ImmutableMap.of(new SegmentWithRange(new Segment("scope", "stream", 0), 0.0, 1.0), 0L);
        ReaderGroupState state = new ReaderGroupState("_RGTest", mock(Revision.class), mock(ReaderGroupConfig.class),
                                                      segmentToOffsets, mock(Map.class), false);
        oldStyleUpdate.update(state); // ensure no exceptions are thrown.

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
    
    private Map<SegmentWithRange, Long> createSegmentRangeMap() throws Exception {
        return createMap(this::createSegmentWithRange, r::nextLong);
    }

    private <K, V> Map<K, V> createMap(Callable<K> keyGen, Callable<V> valueGen) throws Exception {
        int size = r.nextInt(3);
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (int i = 0; i < size; i++) {
            builder.put(keyGen.call(), valueGen.call());
        }
        return builder.build();
    }
    
    private SegmentWithRange createSegmentWithRange() {
        float r1 = r.nextFloat();
        float r2 = r.nextFloat();
        return new SegmentWithRange(createSegment(), Math.min(r1, r2), Math.max(r1, r2));
    }

    private Segment createSegment() {
        return new Segment(createString(), createString(), r.nextInt(100));
    }

    private String createString() {
        return RandomStringUtils.randomAlphabetic(5);
    }
}

