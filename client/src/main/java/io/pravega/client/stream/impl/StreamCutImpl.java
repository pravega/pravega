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
import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ToStringUtils;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

import static io.pravega.common.util.ToStringUtils.stringToMap;

/**
 * Implementation of {@link io.pravega.client.stream.StreamCut} interface. {@link StreamCutInternal} abstract class is
 * used as in intermediate class to make StreamCut instances opaque.
 */
@EqualsAndHashCode(callSuper = false)
public class StreamCutImpl extends StreamCutInternal {

    static final StreamCutSerializer SERIALIZER = new StreamCutSerializer();
    private static final char TO_STRING_VERSION = 0;

    private final Stream stream;

    private final Map<Segment, Long> positions;

    @Builder(builderClassName = "StreamCutBuilder")
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

    @Override
    public String toString() {
        return stream.getScopedName() + ":" + TO_STRING_VERSION + ":"
                + ToStringUtils.mapToString(positions.entrySet()
                                                     .stream()
                                                     .collect(Collectors.toMap(e -> e.getKey().getSegmentId(),
                                                                               e -> e.getValue())));
    }

    public static StreamCutInternal from(String textualRepresentation) {
        Exceptions.checkNotNullOrEmpty(textualRepresentation, "textualRepresentation");
        String[] split = textualRepresentation.split(":", 3);
        Preconditions.checkArgument(split.length == 3, "Invalid string representation of StreamCut");

        final Stream stream = Stream.of(split[0]);
        final Map<Segment, Long> positions = stringToMap(split[2],
                                                         s -> new Segment(stream.getScope(), stream.getStreamName(), Long.valueOf(s)),
                                                         Long::valueOf);
        return new StreamCutImpl(stream, positions);
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
                                                                                 stream.getStreamName(), in.readCompactLong()),
                                                               in -> in.readCompactLong());
            builder.positions(map);
        }

        private void write00(StreamCutInternal cut, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(cut.getStream().getScopedName());
            Map<Segment, Long> map = cut.getPositions();
            revisionDataOutput.writeMap(map, (out, s) -> out.writeCompactLong(s.getSegmentId()),
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

    @SneakyThrows(IOException.class)
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(SERIALIZER.serialize(this).getCopy());
    }

    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] value;
        @SneakyThrows(IOException.class)
        Object readResolve() throws ObjectStreamException {
            return SERIALIZER.deserialize(new ByteArraySegment(value));
        }
    }

}
