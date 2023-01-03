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
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 * This record is indexed in retentionSet using the recording time and recording size.
 */
@Data
@Slf4j
public class StreamCutRecord {
    public static final RetentionStreamCutRecordSerializer SERIALIZER = new RetentionStreamCutRecordSerializer();

    /**
     * Time when this stream cut was recorded.
     */
    final long recordingTime;
    /**
     * Amount of data in the stream preceeding this cut.
     */
    final long recordingSize;
    /**
     * Actual Stream cut.
     */
    final ImmutableMap<Long, Long> streamCut;

    @Builder
    public StreamCutRecord(long recordingTime, long recordingSize, @NonNull ImmutableMap<Long, Long> streamCut) {
        this.recordingTime = recordingTime;
        this.recordingSize = recordingSize;
        this.streamCut = streamCut;
    }

    public StreamCutReferenceRecord getReferenceRecord() {
        return new StreamCutReferenceRecord(recordingTime, recordingSize);
    }

    public Map<Long, Long> getStreamCut() {
        return Collections.unmodifiableMap(streamCut);
    }

    private static class StreamCutRecordBuilder implements ObjectBuilder<StreamCutRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StreamCutRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "recordingTime", recordingTime) + "\n" +
                String.format("%s = %s", "recordingSize", recordingSize) + "\n" +
                String.format("%s = %s", "streamCut", streamCut.keySet().stream()
                        .map(key -> key + " : " + streamCut.get(key))
                        .collect(Collectors.joining(", ", "{", "}")));
    }

    public static class RetentionStreamCutRecordSerializer
            extends VersionedSerializer.WithBuilder<StreamCutRecord, StreamCutRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, StreamCutRecordBuilder streamCutRecordBuilder)
                throws IOException {
            streamCutRecordBuilder.recordingTime(revisionDataInput.readLong())
                                  .recordingSize(revisionDataInput.readLong());
            ImmutableMap.Builder<Long, Long> streamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, streamCutBuilder);
            streamCutRecordBuilder.streamCut(streamCutBuilder.build());
        }

        private void write00(StreamCutRecord streamCutRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(streamCutRecord.getRecordingTime());
            revisionDataOutput.writeLong(streamCutRecord.getRecordingSize());
            revisionDataOutput.writeMap(streamCutRecord.getStreamCut(), DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected StreamCutRecordBuilder newBuilder() {
            return StreamCutRecord.builder();
        }
    }

}
