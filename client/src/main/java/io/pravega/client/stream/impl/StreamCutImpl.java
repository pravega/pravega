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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ToStringUtils;
import io.pravega.shared.NameUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.SneakyThrows;

import static io.pravega.common.util.ToStringUtils.compressToBase64;
import static io.pravega.common.util.ToStringUtils.decompressFromBase64;
import static io.pravega.common.util.ToStringUtils.listToString;
import static io.pravega.common.util.ToStringUtils.stringToList;

/**
 * Implementation of {@link io.pravega.client.stream.StreamCut} interface. {@link StreamCutInternal} abstract class is
 * used as in intermediate class to make StreamCut instances opaque.
 */
@EqualsAndHashCode(callSuper = false)
public final class StreamCutImpl extends StreamCutInternal {

    static final StreamCutSerializer SERIALIZER = new StreamCutSerializer();
    private static final int TO_STRING_VERSION = 0;

    private final Stream stream;

    private final Map<Segment, Long> positions;

    @Builder(builderClassName = "StreamCutBuilder")
    public StreamCutImpl(Stream stream, Map<Segment, Long> positions) {
        this.stream = stream;
        this.positions = ImmutableMap.copyOf(positions);
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
        return stream.getScopedName() + ":"
                + ToStringUtils.mapToString(positions.entrySet()
                                                     .stream()
                                                     .collect(Collectors.toMap(e -> e.getKey().getSegmentId(),
                                                                               e -> e.getValue())));
    }

    @Override
    public String asText() {
        return compressToBase64(getText());
    }

    private String getText() {
        StringBuilder builder = new StringBuilder(Integer.toString(TO_STRING_VERSION)).append(":");
        builder.append(stream.getScopedName()).append(":"); // append Stream name.

        //split segmentNumbers, epochs and offsets into separate lists.
        List<Integer> segmentNumbers = new ArrayList<>();
        List<Integer> epochs = new ArrayList<>();
        List<Long> offsets = new ArrayList<>();
        positions.forEach((segmentId, offset) -> {
            segmentNumbers.add(NameUtils.getSegmentNumber(segmentId.getSegmentId()));
            epochs.add(NameUtils.getEpoch(segmentId.getSegmentId()));
            offsets.add(offset);
        });

        // append segmentsNumbers, epochs and offsets.
        builder.append(listToString(segmentNumbers)).append(":");
        builder.append(listToString(epochs)).append(":");
        builder.append(listToString(offsets));

        return builder.toString();
    }

    /**
     * Obtains the a StreamCut object from its compact Base64 representation obtained via {@link StreamCutImpl#asText()}.
     * @param base64String Compact Base64 representation of StreamCut.
     * @return The StreamCut object.
     */
    public static StreamCutInternal from(String base64String) {
        Exceptions.checkNotNullOrEmpty(base64String, "base64String");
        String[] split = decompressFromBase64(base64String).split(":", 5);
        Preconditions.checkArgument(split.length == 5, "Invalid string representation of StreamCut");

        final Stream stream = Stream.of(split[1]);
        List<Integer> segmentNumbers = stringToList(split[2], Integer::valueOf);
        List<Integer> epochs = stringToList(split[3], Integer::valueOf);
        List<Long> offsets = stringToList(split[4], Long::valueOf);

        final Map<Segment, Long> positions = IntStream.range(0, segmentNumbers.size()).boxed()
                .collect(Collectors.toMap(i ->  new Segment(stream.getScope(), stream.getStreamName(),
                                                            NameUtils.computeSegmentId(segmentNumbers.get(i), epochs.get(i))),
                                          offsets::get));
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
            return 1;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
            version(1).revision(0, this::write10, this::read10);
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

        private void read10(RevisionDataInput revisionDataInput, StreamCutBuilder builder) throws IOException {
            Stream stream = Stream.of(revisionDataInput.readUTF());
            builder.stream(stream);
            Map<Segment, Long> map = revisionDataInput.readMap(in -> new Segment(stream.getScope(),
                                                                                 stream.getStreamName(), in.readCompactLong()),
                                                               RevisionDataInput::readCompactSignedLong);
            builder.positions(map);
        }

        private void write10(StreamCutInternal cut, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(cut.getStream().getScopedName());
            Map<Segment, Long> map = cut.getPositions();
            revisionDataOutput.writeMap(map, (out, s) -> out.writeCompactLong(s.getSegmentId()),
                                        RevisionDataOutput::writeCompactSignedLong);
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
    private Object writeReplace() {
        return new SerializedForm(SERIALIZER.serialize(this).getCopy());
    }

    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] value;
        @SneakyThrows(IOException.class)
        Object readResolve() {
            return SERIALIZER.deserialize(new ByteArraySegment(value));
        }
    }

    @Override
    public int compareTo(@NonNull StreamCut o) {
        Preconditions.checkArgument(stream.getScope().compareTo(o.asImpl().getStream().getScope()) == 0,
                "StreamCuts must be in the same Scope.");
        Preconditions.checkArgument(stream.getStreamName().compareTo(o.asImpl().getStream().getStreamName()) == 0,
                "StreamCuts must be for the same Stream.");

        // Unbounded is greater than everything
        if (o == (StreamCut.UNBOUNDED)) {
            return -1;
        }

        final Map<Segment, Long> otherPositions = o.asImpl().getPositions();

        //check offsets for overlapping segments.
        // TODO check UNBOUNDED, check non-comparable (unmatches AND overlapping) case
        List<Integer> comparisons = (ArrayList<Integer>) positions.keySet()
                .stream()
                .filter(otherPositions::containsKey)
                .mapToInt(s -> {
                    if (positions.get(s) == -1) {
                        if (otherPositions.get(s) == -1) {
                            return 0;
                        }
                        return -1;
                    }
                    if (positions.get(s) < otherPositions.get(s)) {
                        return -1;
                    }
                    if (positions.get(s) > otherPositions.get(s)) {
                        return 1;
                    }
                    return 0;
                }).boxed().collect(Collectors.toList());

        Set<Segment> ourSegments = new HashSet<>(positions.keySet());
        Set<Segment> theirSegments = new HashSet<>(otherPositions.keySet());

        // If true we have unmatched number segments
        if (ourSegments.size() != theirSegments.size()) {
            // invalid comparison, even if streamcuts are named the same, if they are unmatched (or overlapping) then they
            // can be considered to be different streams, and we cant compare streamcuts from diff streams
            // if one side is empty, it violates pravega's successor predecessor rules (sclaing events)
            throw new RuntimeException("StreamCuts containing unmatched segment IDs cannot be compared - Considered to " +
                    "be different streams");
        }

        ourSegments.removeAll(otherPositions.keySet());
        theirSegments.removeAll(positions.keySet());

        Set<List<Segment>> products = Sets.cartesianProduct(ourSegments, theirSegments);

        for (List<Segment> product : products) {
            if (product.get(0).getSegmentId() < product.get(1).getSegmentId())
                comparisons.add(-1);
            if (product.get(0).getSegmentId() > product.get(1).getSegmentId())
                comparisons.add(1);
        }


        boolean hasPositive = false, hasNegative = false;
        for (Integer comparison : comparisons) {
            if (comparison > 0) hasPositive = true;
            if (comparison < 0) hasNegative = true;
        }
        if (hasPositive && hasNegative) throw new RuntimeException("Overlapping Segments");
        if (hasPositive && !hasNegative) return +1;
        if (hasNegative && !hasPositive) return -1;

        return 0;
    }
}