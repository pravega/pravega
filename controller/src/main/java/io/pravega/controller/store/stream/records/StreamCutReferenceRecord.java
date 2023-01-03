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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/*
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 */

/**
 * Data Class representing individual retention set records where recording time and recording sizes are stored.
 */
@Data
@Builder
@AllArgsConstructor
@Slf4j
public class StreamCutReferenceRecord {
    public static final StreamCutReferenceRecordSerializer SERIALIZER = new StreamCutReferenceRecordSerializer();

    /**
     * Time when this stream cut was recorded.
     */
    final long recordingTime;
    /**
     * Amount of data in the stream preceding this cut.
     */
    final long recordingSize;

    public static class StreamCutReferenceRecordBuilder implements ObjectBuilder<StreamCutReferenceRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StreamCutReferenceRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "recordingTime", recordingTime) + "\n" +
                String.format("%s = %s", "recordingSize", recordingSize);
    }

    static class StreamCutReferenceRecordSerializer
            extends VersionedSerializer.WithBuilder<StreamCutReferenceRecord, StreamCutReferenceRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StreamCutReferenceRecordBuilder retentionRecordBuilder)
                throws IOException {
            retentionRecordBuilder.recordingSize(revisionDataInput.readLong());
            retentionRecordBuilder.recordingTime(revisionDataInput.readLong());
        }

        private void write00(StreamCutReferenceRecord retentionRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(retentionRecord.getRecordingSize());
            revisionDataOutput.writeLong(retentionRecord.getRecordingTime());
        }

        @Override
        protected StreamCutReferenceRecordBuilder newBuilder() {
            return StreamCutReferenceRecord.builder();
        }
    }
}
