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
package io.pravega.client.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamCutInternal;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import lombok.Cleanup;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static io.pravega.test.common.AssertExtensions.assertThrows;

public class StreamCutTest {

    @Test
    public void testStreamCutSerialization() throws Exception {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(3, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(8, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(9, 2)), 60L)
                .put(new Segment("scope", "stream", computeSegmentId(10, 2)), -1L)
                .build();

        StreamCut sc = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);
        byte[] buf = serialize(sc);
        String base64 = sc.asText();
        assertEquals(sc, deSerializeStreamCut(buf));
        assertEquals(sc, StreamCut.fromBytes(sc.toBytes()));
        assertEquals(sc, StreamCut.from(base64));
    }

    @Test
    public void testUnboundedStreamCutSerialization() throws Exception {
        StreamCut sc = StreamCut.UNBOUNDED;
        final byte[] buf = serialize(sc);
        String base64 = sc.asText();
        assertEquals(sc, deSerializeStreamCut(buf));
        assertNull(deSerializeStreamCut(buf).asImpl());
        assertEquals(sc, StreamCut.fromBytes(sc.toBytes()));
        assertEquals(sc, StreamCut.from(base64));
    }

    // Versioned serializer to simulate version 0 of StreamCutImpl.
    private static class StreamCutSerializerV0 extends VersionedSerializer.Direct<StreamCutInternal> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        // serialize a StreamCut using the format used in Version 0.
        private void write00(StreamCutInternal cut, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(cut.getStream().getScopedName());
            Map<Segment, Long> map = cut.getPositions();
            revisionDataOutput.writeMap(map, (out, s) -> out.writeCompactLong(s.getSegmentId()),
                                        (out, offset) -> out.writeCompactLong(offset));
        }

        private void read00(RevisionDataInput revisionDataInput, StreamCutInternal target) {
            // NOP.
        }
    }

    @Test
    public void testStreamCutSerializationCompatabilityV0() throws Exception {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();
        StreamCut sc = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);

        // Obtain version 0 serialized data
        final byte[] bufV0 = new StreamCutSerializerV0().serialize(sc.asImpl()).array();
        // deserialize it using current version 1 serialization and ensure compatibility.
        assertEquals(sc, new StreamCutImpl.StreamCutSerializer().deserialize(bufV0));
    }

    @Test
    public void compareToNullThrowsNullPointerException() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();
        StreamCutImpl sc = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);
        assertThrows(NullPointerException.class, () -> {
            sc.compareTo(null);
        });
    }


    @Test
    public void compareToSelf() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        StreamCutImpl sc = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);
        assertEquals(0, sc.compareTo(sc));
    }

    @Test
    public void compareToStreamCutWithDifferentScopeName() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scopeOne", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scopeOne", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scopeTwo", "stream", computeSegmentId(1, 1)), 30L)
                .put(new Segment("scopeTwo", "stream", computeSegmentId(2, 1)), 40L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scopeOne", "stream"), segmentOffsetMap);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scopeTwo", "stream"), segmentOffsetMap2);

        Assertions.assertThrows(IllegalArgumentException.class, () -> sc1.compareTo(sc2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> sc2.compareTo(sc1));
    }

    @Test
    public void compareToStreamCutWithDifferentStreamName() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "streamOne", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "streamOne", computeSegmentId(2, 1)), 20L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "streamTwo", computeSegmentId(1, 1)), 30L)
                .put(new Segment("scope", "streamTwo", computeSegmentId(2, 1)), 40L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scope", "streamOne"), segmentOffsetMap);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scope", "streamTwo"), segmentOffsetMap2);

        Assertions.assertThrows(IllegalArgumentException.class, () -> sc1.compareTo(sc2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> sc2.compareTo(sc1));
    }

    @Test
    public void compareToStreamCutWithDifferentStreamScopeName() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scopeOne", "streamOne", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scopeOne", "streamOne", computeSegmentId(2, 1)), 20L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scopeTwo", "streamTwo", computeSegmentId(1, 1)), 30L)
                .put(new Segment("scopeTwo", "streamTwo", computeSegmentId(2, 1)), 40L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scopeOne", "streamOne"), segmentOffsetMap);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scopeTwo", "streamTwo"), segmentOffsetMap2);

        Assertions.assertThrows(IllegalArgumentException.class, () -> sc1.compareTo(sc2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> sc2.compareTo(sc1));
    }

    @Test
    public void compareToStreamCutOnlyAdvancingSegmentIDs() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        // Greater Epochs advance segment IDs
        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 2)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 2)), 20L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertEquals(-1, sc1.compareTo(sc2));
        assertEquals(1, sc2.compareTo(sc1));
    }

    @Test
    public void compareToStreamCutOnlyAdvancingOffsets() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 40L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertEquals(-1, sc1.compareTo(sc2));
        assertEquals(1, sc2.compareTo(sc1));
    }

    @Test
    public void compareToUnbounded() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        StreamCutImpl sc = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);

        assertEquals(-1, sc.compareTo(StreamCut.UNBOUNDED));
    }

    @Test
    public void compareUnboundedToItself() {
        assertEquals(0, StreamCut.UNBOUNDED.compareTo(StreamCut.UNBOUNDED));
    }

    @Test
    public void compareUnboundedToCopyOfItself() {
        ByteBuffer bytes = StreamCut.UNBOUNDED.toBytes();

        assertEquals(0, StreamCut.UNBOUNDED.compareTo(StreamCut.fromBytes(bytes)));
    }

    @Test
    public void compareCopyOfUnboundedToUnbounded() {
        ByteBuffer bytes = StreamCut.UNBOUNDED.toBytes();

        assertEquals(0, StreamCut.fromBytes(bytes).compareTo(StreamCut.UNBOUNDED));
    }

    @Test
    public void unboundedCompareTo() {
        ImmutableMap<Segment, Long> segmentOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        StreamCutImpl sc = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap);
        assertEquals(1, StreamCut.UNBOUNDED.compareTo(sc));
    }

    @Test
    public void compareToEqualStreamCutWithNegativeOne() {
        ImmutableMap<Segment, Long> segmentOffsetMap1 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(3, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(8, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(9, 2)), 60L)
                .put(new Segment("scope", "stream", computeSegmentId(10, 2)), -1L)
                .build();
        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(3, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(8, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(9, 2)), 60L)
                .put(new Segment("scope", "stream", computeSegmentId(10, 2)), -1L)
                .build();

        StreamCut sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap1);
        StreamCut sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertEquals(0, sc1.compareTo(sc2));
    }

    @Test
    public void compareToLesserStreamCutWithNegativeOne() {
        ImmutableMap<Segment, Long> segmentOffsetMap1 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(3, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(8, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(9, 2)), 60L)
                .put(new Segment("scope", "stream", computeSegmentId(10, 2)), -1L)
                .build();
        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(3, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(8, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(9, 2)), 50L)
                .put(new Segment("scope", "stream", computeSegmentId(10, 2)), -1L)
                .build();

        StreamCut sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap1);
        StreamCut sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertEquals(1, sc1.compareTo(sc2));
    }

    @Test
    public void compareToStreamCutWithDifferentNumberOfSegmentsAdvancingIdAndEpoch() {
        ImmutableMap<Segment, Long> segmentOffsetMap1 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(3, 2)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 2)), 40L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 2)), 50L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap1);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertEquals(-1, sc1.compareTo(sc2));
    }

    @Test
    public void compareToStreamCutWithDifferentNumberOfSegments() {
        ImmutableMap<Segment, Long> segmentOffsetMap1 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 40L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 1)), 50L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap1);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertThrows(RuntimeException.class, () -> {
            sc1.compareTo(sc2);
        });
    }

    @Test
    public void compareToStreamCutWithOverLappingSegments() {
        ImmutableMap<Segment, Long> segmentOffsetMap1 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(3, 1)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 1)), 40L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(3, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(4, 1)), 20L)
                .put(new Segment("scope", "stream", computeSegmentId(5, 2)), 30L)
                .put(new Segment("scope", "stream", computeSegmentId(6, 2)), 40L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap1);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertThrows(RuntimeException.class, () -> {
            sc1.compareTo(sc2);
        });
    }

    @Test
    public void compareToStreamCutWithDifferentEpochs() {
        ImmutableMap<Segment, Long> segmentOffsetMap1 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 1)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 1)), 20L)
                .build();

        ImmutableMap<Segment, Long> segmentOffsetMap2 = ImmutableMap.<Segment, Long>builder()
                .put(new Segment("scope", "stream", computeSegmentId(1, 2)), 10L)
                .put(new Segment("scope", "stream", computeSegmentId(2, 2)), 20L)
                .build();

        StreamCutImpl sc1 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap1);
        StreamCutImpl sc2 = new StreamCutImpl(Stream.of("scope", "stream"), segmentOffsetMap2);

        assertEquals(-1, sc1.compareTo(sc2));
        assertEquals(1, sc2.compareTo(sc1));
    }

    private byte[] serialize(StreamCut sc) throws IOException {
        @Cleanup
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(sc);
        return baos.toByteArray();
    }

    private StreamCut deSerializeStreamCut(final byte[] buf) throws Exception {
        @Cleanup
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        @Cleanup
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (StreamCut) ois.readObject();
    }
}
