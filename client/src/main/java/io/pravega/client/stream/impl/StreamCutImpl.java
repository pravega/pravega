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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;

/**
 * Implementation of {@link io.pravega.client.stream.StreamCut} interface. {@link StreamCutInternal} abstract class is
 * used as in intermediate class to make StreamCut instances opaque.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public class StreamCutImpl extends StreamCutInternal {

    static final StreamCutSerializer SERIALIZER = new StreamCutSerializer();

    private final Stream stream;

    private final Map<Segment, Long> positions;

    @Builder(builderClassName="StreamCutBuilder")
    public StreamCutImpl(Stream stream, Map<Segment, Long> positions) {
        this.stream = stream;
        this.positions = positions;
    }

    @Override
    public Map<Segment, Long> getPositions() {
        return Collections.unmodifiableMap(positions);
    }

    @Override
    public Stream getStream() {
        return stream;
    }

    @Override
    public StreamCutInternal asImpl() {
        return this;
    }

    @VisibleForTesting
    public boolean validate(Set<String> segmentNames) {
        for (Segment s: positions.keySet()) {
            if (!segmentNames.contains(s.getScopedName())) {
                return false;
            }
        }

        return true;
    }
    
    private static class StreamCutBuilder implements ObjectBuilder<StreamCutInternal> {
    }
    
    public static class StreamCutSerializer extends VersionedSerializer.WithBuilder<StreamCutInternal, StreamCutBuilder> {
        @Override
        protected StreamCutBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StreamCutBuilder builder) throws IOException {
            Stream stream = Stream.of(revisionDataInput.readUTF());
            builder.stream(stream);
            Map<Segment, Long> map = revisionDataInput.readMap(in -> new Segment(stream.getScope(),
                                                                                 stream.getStreamName(), in.readCompactInt()),
                                                               in -> in.readCompactLong());
            builder.positions(map);
        }

        private void write00(StreamCutInternal cut, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(cut.getStream().getScopedName());
            Map<Segment, Long> map = cut.getPositions();
            revisionDataOutput.writeMap(map, (out, s) -> out.writeCompactInt(s.getSegmentNumber()),
                                        (out, offset) -> out.writeCompactLong(offset));
        }
    }

    @Override
    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }
    
    @SneakyThrows(IOException.class)
    public static StreamCutInternal fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }
}
