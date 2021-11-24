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
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Data class for storing information about stream's subscribers.
 */
@Data
@Slf4j
public class StreamSubscriber {
    public static final StreamSubscriberSerializer SERIALIZER = new StreamSubscriberSerializer();

    private final String subscriber;

    private final long generation;

    /**
     * Truncation Stream cut published by this subscriber
     */
    private final ImmutableMap<Long, Long> truncationStreamCut;

    /**
     * Time when this record was last created/updated.
     */
    private final long updateTime;

    @Builder
    public StreamSubscriber(@NonNull final String subscriber, final long generation, @NonNull ImmutableMap<Long, Long> truncationStreamCut, final long updationTime) {
        this.subscriber = subscriber;
        this.generation = generation;
        this.truncationStreamCut = truncationStreamCut;
        this.updateTime = updationTime;
    }

    private static class StreamSubscriberBuilder implements ObjectBuilder<StreamSubscriber> {
    }

    @SneakyThrows(IOException.class)
    public static StreamSubscriber fromBytes(final byte[] record) {
        return SERIALIZER.deserialize(record);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "subscriber", subscriber) + "\n" +
                String.format("%s = %s", "generation", generation) + "\n" +
                String.format("%s = %s", "truncationStreamCut", truncationStreamCut.keySet().stream()
                        .map(key -> key + " : " + truncationStreamCut.get(key))
                        .collect(Collectors.joining(", ", "{", "}"))) + "\n" +
                String.format("%s = %s", "updateTime", updateTime);
    }
    
    private static class StreamSubscriberSerializer
            extends VersionedSerializer.WithBuilder<StreamSubscriber, StreamSubscriberBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StreamSubscriberBuilder recordBuilder)
                throws IOException {
            recordBuilder.subscriber(revisionDataInput.readUTF());
            recordBuilder.generation(revisionDataInput.readLong());
            recordBuilder.updationTime(revisionDataInput.readLong());
            ImmutableMap.Builder<Long, Long> streamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, streamCutBuilder);
            recordBuilder.truncationStreamCut(streamCutBuilder.build());
        }

        private void write00(StreamSubscriber subscriberRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(subscriberRecord.getSubscriber());
            revisionDataOutput.writeLong(subscriberRecord.getGeneration());
            revisionDataOutput.writeLong(subscriberRecord.getUpdateTime());
            revisionDataOutput.writeMap(subscriberRecord.getTruncationStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected StreamSubscriberBuilder newBuilder() {
            return StreamSubscriber.builder();
        }
    }
}
