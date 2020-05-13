/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class PositionImpl extends PositionInternal {

    private static final PositionSerializer SERIALIZER = new PositionSerializer();
    private final Map<Segment, Long> ownedSegments;
    private final Map<Segment, Range> segmentRanges;

    // If this field is set, it means that we will need to apply the updates on the ownedSegments.
    private transient List<Entry<Segment, Long>> updatesToSegmentOffsets;
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
    
    @Builder(builderClassName = "PositionBuilder")
    PositionImpl(Map<Segment, Long> ownedSegments, Map<Segment, Range> segmentRanges, List<Entry<Segment, Long>> updatesToSegmentOffsets) {
        this.ownedSegments = ownedSegments;
        this.updatesToSegmentOffsets = updatesToSegmentOffsets;
        this.version = (updatesToSegmentOffsets != null) ? updatesToSegmentOffsets.size() : 0;
        if (segmentRanges == null) {
            this.segmentRanges = Collections.emptyMap();
        } else {
            this.segmentRanges = segmentRanges;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
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
        if (this.updatesToSegmentOffsets == null) {
            return;
        }
        // Apply all the Segment offset updates up to the point in which this event was read.
        for (int i = 0; i < this.version; i++) {
            this.ownedSegments.put(this.updatesToSegmentOffsets.get(i).getKey(), this.updatesToSegmentOffsets.get(i).getValue());
        }
        this.updatesToSegmentOffsets = null;
        this.version = 0;
    }

}
