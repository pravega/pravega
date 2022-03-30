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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.SegmentWithRange.Range;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ToStringUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;

import static io.pravega.common.io.serialization.RevisionDataOutput.COMPACT_LONG_MAX;

public final class PositionImpl extends PositionInternal {

    private static final PositionSerializer SERIALIZER = new PositionSerializer();
    private Map<Segment, Long> ownedSegments;
    private final Map<Segment, Range> segmentRanges;

    // If this field is set, it means that we will need to apply the updates on the ownedSegments.
    private transient List<Entry<Segment, Long>> updatesToSegmentOffsets;
    // This field represents the index up to which updatesToSegmentOffsets should be applied to ownedSegments.
    private transient long version;

    /**
     * Instantiates Position with current and future owned segments.
     *
     * @param segments Current segments that the position refers to.
     */
    public PositionImpl(Map<SegmentWithRange, Long> segments) {
        this.ownedSegments = new HashMap<>(segments.size());
        this.segmentRanges = new HashMap<>(segments.size());
        this.updatesToSegmentOffsets = null;
        this.version = 0;
        for (Entry<SegmentWithRange, Long> entry : segments.entrySet()) {
            SegmentWithRange s = entry.getKey();
            this.ownedSegments.put(s.getSegment(), entry.getValue());
            this.segmentRanges.put(s.getSegment(), s.getRange());
        }
    }

    /**
     * Builder to lazily construct a PositionImpl object. This builder allows as input both copies or references to
     * (structurally immutable) external collections to build its internal state. In the case of using references to
     * existing collections as input, the implementation of this class ensures that their contents will not be changed.
     * By using this builder, we are making the internal state of this object to be lazily constructed (i.e., only if
     * any method on the object is invoked, the internal state is built first based on the collections used as input).
     *
     * @param ownedSegments             Map of Segments and their current read offset.
     * @param segmentRanges             Map that relates Segments with their assigned keyspace ranges.
     * @param updatesToSegmentOffsets   Optional list of Segment offset updates. If this list is not null or empty,
     *                                   this class will replay the input list and update a copy of ownedSegments with
     *                                   the offsets in the list. This will happen before any other method invocation to
     *                                   build the internal state of the object.
     */
    @Builder(builderClassName = "PositionBuilder")
    PositionImpl(Map<Segment, Long> ownedSegments, Map<Segment, Range> segmentRanges, List<Entry<Segment, Long>> updatesToSegmentOffsets) {
        this.ownedSegments = Collections.unmodifiableMap(ownedSegments);
        this.updatesToSegmentOffsets = (updatesToSegmentOffsets != null) ? Collections.unmodifiableList(updatesToSegmentOffsets) : null;
        this.version = (updatesToSegmentOffsets != null) ? updatesToSegmentOffsets.size() : 0;
        if (segmentRanges == null) {
            this.segmentRanges = Collections.emptyMap();
        } else {
            this.segmentRanges = Collections.unmodifiableMap(segmentRanges);
        }
    }

    @Override
    public Set<Segment> getOwnedSegments() {
        applySegmentOffsetUpdatesIfNeeded();
        return Collections.unmodifiableSet(ownedSegments.keySet());
    }

    @Override
    public Map<Segment, Long> getOwnedSegmentsWithOffsets() {
        applySegmentOffsetUpdatesIfNeeded();
        return Collections.unmodifiableMap(ownedSegments);
    }
    
    @Override
    Map<SegmentWithRange, Long> getOwnedSegmentRangesWithOffsets() {
        applySegmentOffsetUpdatesIfNeeded();
        HashMap<SegmentWithRange, Long> result = new HashMap<>();
        for (Entry<Segment, Long> entry : ownedSegments.entrySet()) {
            result.put(new SegmentWithRange(entry.getKey(), segmentRanges.get(entry.getKey())), entry.getValue());
        }
        return result;
    }

    @Override
    public Set<Segment> getCompletedSegments() {
        applySegmentOffsetUpdatesIfNeeded();
        return ownedSegments.entrySet()
            .stream()
            .filter(x -> x.getValue() < 0)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @Override
    public Long getOffsetForOwnedSegment(Segment segmentId) {
        applySegmentOffsetUpdatesIfNeeded();
        return ownedSegments.get(segmentId);
    }

    @Override
    public PositionImpl asImpl() {
        applySegmentOffsetUpdatesIfNeeded();
        return this;
    }
    
    @Override
    public String toString() {
        applySegmentOffsetUpdatesIfNeeded();
        return ToStringUtils.mapToString(ownedSegments);
    }

    @Override
    public boolean equals(Object o) {
        applySegmentOffsetUpdatesIfNeeded();
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PositionImpl position = (PositionImpl) o;
        return ownedSegments.equals(position.getOwnedSegmentsWithOffsets()) && segmentRanges.equals(position.segmentRanges);
    }

    @Override
    public int hashCode() {
        applySegmentOffsetUpdatesIfNeeded();
        return Objects.hash(ownedSegments, segmentRanges);
    }

    static class PositionBuilder implements ObjectBuilder<PositionImpl> {
    }

    private static class PositionSerializer extends VersionedSerializer.WithBuilder<PositionImpl, PositionBuilder> {

        @Override
        protected PositionBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00)
                      .revision(1, this::write01, this::read01);
        }

        private void read00(RevisionDataInput revisionDataInput, PositionBuilder builder) throws IOException {
            Map<Segment, Long> map = revisionDataInput.readMap(in -> Segment.fromScopedName(in.readUTF()), in -> {
                long offset = in.readCompactLong();
                if (offset == COMPACT_LONG_MAX) {
                    return -1L;
                } else {
                    return offset;
                }
            });
            builder.ownedSegments(map);
        }

        private void write00(PositionImpl position, RevisionDataOutput revisionDataOutput) throws IOException {
            // Note: the older client does not serialize if the offset has -1L as value.
            Map<Segment, Long> map = position.getOwnedSegmentsWithOffsets();
            revisionDataOutput.writeMap(map, (out, s) -> out.writeUTF(s.getScopedName()),
                                        (out, offset) -> {
                                            if (offset < 0) {
                                                out.writeCompactLong(COMPACT_LONG_MAX);
                                            } else {
                                                out.writeCompactLong(offset);
                                            }
                                        });
        }
        
        private void read01(RevisionDataInput revisionDataInput, PositionBuilder builder) throws IOException {
            Map<Segment, Range> map = revisionDataInput.readMap(in -> Segment.fromScopedName(in.readUTF()), PositionSerializer::readRange);
            builder.segmentRanges(map);
        }

        private void write01(PositionImpl position, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeMap(position.segmentRanges, (out, s) -> out.writeUTF(s.getScopedName()), PositionSerializer::writeRange);
        }

        private static void writeRange(RevisionDataOutput out, Range range) throws IOException {
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
        
        private static Range readRange(RevisionDataInput in) throws IOException {
            double low = in.readDouble();
            double high = in.readDouble();
            return (low < 0 || high < 0) ? null : new Range(low, high);
        }
    }

    @Override
    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @SneakyThrows(IOException.class)
    public static Position fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }

    private synchronized void applySegmentOffsetUpdatesIfNeeded() {
        // No updates, so nothing to do.
        if (this.updatesToSegmentOffsets == null || this.updatesToSegmentOffsets.isEmpty()) {
            return;
        }
        // We create the new ownedSegments map based on the updatesToSegmentOffsets list and existing ownedSegments map.
        Map<Segment, Long> newOwnedSegments = new HashMap<>();
        // Apply only the most recent Segment offset updates starting at the point this event was read.
        for (int i = (int) this.version - 1; i >= 0; i--) {
            newOwnedSegments.putIfAbsent(this.updatesToSegmentOffsets.get(i).getKey(), this.updatesToSegmentOffsets.get(i).getValue());
            // We have the most recent updates on all the segments, no need to continue the loop.
            if (newOwnedSegments.size() == this.ownedSegments.size()) {
                break;
            }
        }
        // In case that there are segments without updates in updatesToSegmentOffsets, we apply the existing values in ownedSegments.
        if (newOwnedSegments.size() < this.ownedSegments.size()) {
            for (Entry<Segment, Long> s : this.ownedSegments.entrySet()) {
                newOwnedSegments.putIfAbsent(s.getKey(), s.getValue());
            }
        }
        // Build the final state of this PositionImpl object.
        this.ownedSegments = Collections.unmodifiableMap(newOwnedSegments);
        this.updatesToSegmentOffsets = null;
        this.version = 0;
    }
}
