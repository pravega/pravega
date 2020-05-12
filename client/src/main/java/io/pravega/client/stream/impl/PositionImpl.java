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
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

import static io.pravega.common.io.serialization.RevisionDataOutput.COMPACT_LONG_MAX;

@EqualsAndHashCode(callSuper = false)
public class PositionImpl extends PositionInternal {

    private static final PositionSerializer SERIALIZER = new PositionSerializer();
    private final List<Entry<Segment, Long>> ownedSegments;
    private final Map<Segment, Range> segmentRanges;

    private final List<Entry<Segment, Long>> updatesToSegmentOffsets;
    private final long version;

    // TODO: We need to implement to lazily apply all the updates prior any other call on this class.
    /*checkForUpdatesToBeApplied() {
        //loop over updatesToSegmentOffsets and create a new hash map based on ownedSegments + apply updatesToSegmentOffsets
    }*/

    /**
     * Instantiates Position with current and future owned segments.
     *
     * @param segments Current segments that the position refers to.
     */
    public PositionImpl(Map<SegmentWithRange, Long> segments) {
        this.ownedSegments = new ArrayList<>(segments.size());
        this.segmentRanges = new HashMap<>(segments.size());
        this.updatesToSegmentOffsets = null;
        this.version = 0;
        for (Entry<SegmentWithRange, Long> entry : segments.entrySet()) {
            SegmentWithRange s = entry.getKey();
            this.ownedSegments.add(new SimpleEntry<>(s.getSegment(), entry.getValue()));
            this.segmentRanges.put(s.getSegment(), s.getRange());
        }
    }
    
    @Builder(builderClassName = "PositionBuilder")
    PositionImpl(List<Entry<Segment, Long>> ownedSegments, Map<Segment, Range> segmentRanges, List<Entry<Segment, Long>> updatesToSegmentOffsets) {
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
        return ownedSegments.stream().map(Entry::getKey).collect(Collectors.toSet());
    }

    @Override
    public Map<Segment, Long> getOwnedSegmentsWithOffsets() {
        return ownedSegments.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
    
    @Override
    Map<SegmentWithRange, Long> getOwnedSegmentRangesWithOffsets() {
        HashMap<SegmentWithRange, Long> result = new HashMap<>();
        for (Entry<Segment, Long> entry : ownedSegments) {
            result.put(new SegmentWithRange(entry.getKey(), segmentRanges.get(entry.getKey())), entry.getValue());
        }
        return result;
    }

    @Override
    public Set<Segment> getCompletedSegments() {
        return ownedSegments
            .stream()
            .filter(x -> x.getValue() < 0)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @Override
    public Long getOffsetForOwnedSegment(Segment segmentId) {
        return ownedSegments.stream().filter(e -> e.getKey().equals(segmentId)).map(Entry::getValue).findFirst().get();
    }

    @Override
    public PositionImpl asImpl() {
        return this;
    }
    
    @Override
    public String toString() {
        return ToStringUtils.mapToString(getOwnedSegmentsWithOffsets());
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
            builder.ownedSegments(new ArrayList<>(map.entrySet()));
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

}
