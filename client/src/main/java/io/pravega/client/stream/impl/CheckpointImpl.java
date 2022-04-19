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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;

@EqualsAndHashCode
public final class CheckpointImpl implements Checkpoint {

    private static final CheckpointSerializer SERIALIZER = new CheckpointSerializer();
    @Getter
    private final String name;
    @Getter
    private final Map<Stream, StreamCut> positions;
    
    CheckpointImpl(String name, Map<Segment, Long> segmentPositions) {
        this.name = name;
        Map<Stream, ImmutableMap.Builder<Segment, Long>> streamPositions = new HashMap<>();
        for (Entry<Segment, Long> position : segmentPositions.entrySet()) {
            streamPositions.computeIfAbsent(position.getKey().getStream(),
                                            k -> new ImmutableMap.Builder<Segment, Long>())
                           .put(position);
        }
        ImmutableMap.Builder<Stream, StreamCut> positionBuilder = ImmutableMap.builder();
        for (Entry<Stream, Builder<Segment, Long>> streamPosition : streamPositions.entrySet()) {
            positionBuilder.put(streamPosition.getKey(),
                                new StreamCutImpl(streamPosition.getKey(), streamPosition.getValue().build()));
        }
        this.positions = positionBuilder.build();
    }
    
    @lombok.Builder(builderClassName = "CheckpointBuilder")
    private CheckpointImpl(Map<Stream, StreamCut> positions, String name) {
        this.name = name;
        this.positions = positions;
    }
    
    @Override
    public CheckpointImpl asImpl() {
        return this;
    }

    private static class CheckpointBuilder implements ObjectBuilder<CheckpointImpl> {
    }
    
    private static class CheckpointSerializer extends VersionedSerializer.WithBuilder<CheckpointImpl, CheckpointBuilder> {

        @Override
        protected CheckpointBuilder newBuilder() {
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

        private void read00(RevisionDataInput revisionDataInput, CheckpointBuilder builder) throws IOException {
            builder.name(revisionDataInput.readUTF());
            Map<Stream, StreamCut> map = revisionDataInput.readMap(in -> Stream.of(in.readUTF()),
                                                                   StreamCutImpl.SERIALIZER::deserialize);
            builder.positions(map);
        }

        private void write00(CheckpointImpl checkpoint, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(checkpoint.getName());
            Map<Stream, StreamCut> map = checkpoint.getPositions();
            revisionDataOutput.writeMap(map, (out, s) -> out.writeUTF(s.getScopedName()),
                                        (out, cut) -> StreamCutImpl.SERIALIZER.serialize(out, cut.asImpl()));
        }
    }

    @Override
    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }
    
    @SneakyThrows(IOException.class)
    public static Checkpoint fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }
}
