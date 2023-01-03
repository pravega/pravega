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
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.stream.Collectors;

/* This is a data class that represents a writer mark. Writers send mark information
 * for watermarking purposes, containing time and position.  A data object of this
 * class records such writer information to be serialized and and store in the
 * underlying metadata store.
 */
@Data
public class WriterMark {
    public static final WriterMarkSerializer SERIALIZER = new WriterMarkSerializer();
    public static final WriterMark EMPTY = new WriterMark(Long.MIN_VALUE, ImmutableMap.of(), false);
    private final long timestamp;
    private final ImmutableMap<Long, Long> position;
    private final boolean isAlive;
    
    public WriterMark(long timestamp, ImmutableMap<Long, Long> position) {
        this.timestamp = timestamp;
        this.position = position;
        this.isAlive = true;
    }

    @Builder
    public WriterMark(long timestamp, ImmutableMap<Long, Long> position, boolean isAlive) {
        this.timestamp = timestamp;
        this.position = position;
        this.isAlive = isAlive;
    }

    public static class WriterMarkBuilder implements ObjectBuilder<WriterMark> {

    }
    
    @SneakyThrows(IOException.class)
    public static WriterMark fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "timestamp", timestamp) + "\n" +
                String.format("%s = %s", "position", position.keySet().stream()
                        .map(key -> key + ":" + position.get(key))
                        .collect(Collectors.joining(", ", "{", "}"))) + "\n" +
                String.format("%s = %s", "isAlive", isAlive);
    }

    private static class WriterMarkSerializer
            extends VersionedSerializer.WithBuilder<WriterMark, WriterMark.WriterMarkBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            WriterMark.WriterMarkBuilder builder) throws IOException {
            builder.timestamp(revisionDataInput.readLong());

            ImmutableMap.Builder<Long, Long> mapBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, mapBuilder);
            builder.position(mapBuilder.build());
            builder.isAlive(revisionDataInput.readBoolean());
        }
        
        private void write00(WriterMark writerMark, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(writerMark.timestamp);
            revisionDataOutput.writeMap(writerMark.position, DataOutput::writeLong, DataOutput::writeLong);
            revisionDataOutput.writeBoolean(writerMark.isAlive);
        }

        @Override
        protected WriterMark.WriterMarkBuilder newBuilder() {
            return WriterMark.builder();
        }
    }

}
