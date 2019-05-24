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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

@EqualsAndHashCode(callSuper = false)
public class PositionImpl extends PositionInternal {

    private static final PositionSerializer SERIALIZER = new PositionSerializer();
    private final Map<Segment, Long> ownedSegments;
    private final Map<Segment, Range> segmentRanges;

    /**
     * Instantiates Position with current and future owned segments.
     *
     * @param ownedSegments Current segments that the position refers to.
     */
    public PositionImpl(Map<SegmentWithRange, Long> segments) {
        this.ownedSegments = new HashMap<>(segments.size());
        this.segmentRanges = new HashMap<>(segments.size());
        for (Entry<SegmentWithRange, Long> entry : segments.entrySet()) {
            SegmentWithRange s = entry.getKey();
            this.ownedSegments.put(s.getSegment(), entry.getValue());
            this.segmentRanges.put(s.getSegment(), s.getRange());
        }
    }
    
    @Builder(builderClassName = "PositionBuilder")
    private PositionImpl(Map<Segment, Long> ownedSegments, Map<Segment, Range> segmentRanges) {
        this.ownedSegments = ownedSegments;
        if (segmentRanges == null) {
            this.segmentRanges = Collections.emptyMap();
        } else {
            this.segmentRanges = segmentRanges;
        }
    }

    static PositionImpl createEmptyPosition() {
        return new PositionImpl(new HashMap<>());
    }

    @Override
    public Set<Segment> getOwnedSegments() {
        return Collections.unmodifiableSet(ownedSegments.keySet());
    }

    @Override
    public Map<Segment, Long> getOwnedSegmentsWithOffsets() {
        return Collections.unmodifiableMap(ownedSegments);
    }

    @Override
    public Set<Segment> getCompletedSegments() {
        return ownedSegments.entrySet()
            .stream()
            .filter(x -> x.getValue() < 0)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @Override
    public Long getOffsetForOwnedSegment(Segment segmentId) {
        return ownedSegments.get(segmentId);
    }

    @Override
    public PositionImpl asImpl() {
        return this;
    }
    
    @Override
    public String toString() {
        return ToStringUtils.mapToString(ownedSegments);
    }

    private static class PositionBuilder implements ObjectBuilder<PositionImpl> {
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
            Map<Segment, Long> map = revisionDataInput.readMap(in -> Segment.fromScopedName(in.readUTF()), RevisionDataInput::readCompactLong);
            builder.ownedSegments(map);
        }

        private void write00(PositionImpl position, RevisionDataOutput revisionDataOutput) throws IOException {
            Map<Segment, Long> map = position.getOwnedSegmentsWithOffsets();
            revisionDataOutput.writeMap(map, (out, s) -> out.writeUTF(s.getScopedName()),
                                        (out, offset) -> out.writeCompactLong(offset));
        }
        
        private void read01(RevisionDataInput revisionDataInput, PositionBuilder builder) throws IOException {
            Map<Segment, Range> map = revisionDataInput.readMap(in -> Segment.fromScopedName(in.readUTF()), this::readRange);
            builder.segmentRanges(map);
        }

        private void write01(PositionImpl position, RevisionDataOutput revisionDataOutput) throws IOException {
            Map<Segment, Range> map = position.segmentRanges;
            revisionDataOutput.writeMap(map, (out, s) -> out.writeUTF(s.getScopedName()),
                                        (out, range) -> { out.writeDouble(range.getLow()); out.writeDouble(range.getHigh()); });
        }
        
        private Range readRange(RevisionDataInput dataInput) throws IOException {
            return new Range(dataInput.readDouble(), dataInput.readDouble());
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
