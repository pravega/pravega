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
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.shared.controller.event.RGStreamCutRecord;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class ReaderGroupConfigRecord {
    public static final ReaderGroupConfigRecord.ConfigurationRecordSerializer SERIALIZER = new ReaderGroupConfigRecord.ConfigurationRecordSerializer();

    private final long groupRefreshTimeMillis;
    private final long automaticCheckpointIntervalMillis;
    private final int maxOutstandingCheckpointRequest;
    private final int retentionTypeOrdinal;
    private final long generation;
    private final Map<String, RGStreamCutRecord> startingStreamCuts;
    private final Map<String, RGStreamCutRecord> endingStreamCuts;
    private final boolean updating;

    public static class ReaderGroupConfigRecordBuilder implements ObjectBuilder<ReaderGroupConfigRecord> {

    }

    public static ReaderGroupConfigRecord update(ReaderGroupConfig rgConfig, long generation, boolean isUpdating) {
        Map<String, RGStreamCutRecord> startStreamCuts = rgConfig.getStartingStreamCuts().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                        e -> new RGStreamCutRecord(ModelHelper.getStreamCutMap(e.getValue()))));
        Map<String, RGStreamCutRecord> endStreamCuts = rgConfig.getEndingStreamCuts().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                        e -> new RGStreamCutRecord(ModelHelper.getStreamCutMap(e.getValue()))));
        return ReaderGroupConfigRecord.builder()
                .generation(generation)
                .groupRefreshTimeMillis(rgConfig.getGroupRefreshTimeMillis())
                .automaticCheckpointIntervalMillis(rgConfig.getAutomaticCheckpointIntervalMillis())
                .maxOutstandingCheckpointRequest(rgConfig.getMaxOutstandingCheckpointRequest())
                .retentionTypeOrdinal(rgConfig.getRetentionType().ordinal())
                .startingStreamCuts(startStreamCuts)
                .endingStreamCuts(endStreamCuts)
                .updating(isUpdating)
                .build();
    }

    public static ReaderGroupConfigRecord complete(ReaderGroupConfigRecord rgConfigRecord) {
        return ReaderGroupConfigRecord.builder()
                .generation(rgConfigRecord.getGeneration())
                .groupRefreshTimeMillis(rgConfigRecord.getGroupRefreshTimeMillis())
                .automaticCheckpointIntervalMillis(rgConfigRecord.getAutomaticCheckpointIntervalMillis())
                .maxOutstandingCheckpointRequest(rgConfigRecord.getMaxOutstandingCheckpointRequest())
                .retentionTypeOrdinal(rgConfigRecord.getRetentionTypeOrdinal())
                .startingStreamCuts(rgConfigRecord.getStartingStreamCuts())
                .endingStreamCuts(rgConfigRecord.getEndingStreamCuts())
                .updating(false)
                .build();
    }

    @SneakyThrows(IOException.class)
    public static ReaderGroupConfigRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class ConfigurationRecordSerializer
            extends VersionedSerializer.WithBuilder<ReaderGroupConfigRecord, ReaderGroupConfigRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(ReaderGroupConfigRecord configurationRecord) {
            Preconditions.checkNotNull(configurationRecord);
            Preconditions.checkNotNull(configurationRecord.startingStreamCuts);
            Preconditions.checkNotNull(configurationRecord.endingStreamCuts);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            ReaderGroupConfigRecordBuilder configurationRecordBuilder)
                throws IOException {
            configurationRecordBuilder.groupRefreshTimeMillis(revisionDataInput.readLong());
            configurationRecordBuilder.automaticCheckpointIntervalMillis(revisionDataInput.readLong());
            configurationRecordBuilder.maxOutstandingCheckpointRequest(revisionDataInput.readInt());
            configurationRecordBuilder.retentionTypeOrdinal(revisionDataInput.readCompactInt());
            configurationRecordBuilder.generation(revisionDataInput.readLong());

            ImmutableMap.Builder<String, RGStreamCutRecord> startStreamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readUTF, RGStreamCutRecord.SERIALIZER::deserialize, startStreamCutBuilder);
            configurationRecordBuilder.startingStreamCuts(startStreamCutBuilder.build());

            ImmutableMap.Builder<String, RGStreamCutRecord> endStreamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readUTF, RGStreamCutRecord.SERIALIZER::deserialize, endStreamCutBuilder);
            configurationRecordBuilder.endingStreamCuts(endStreamCutBuilder.build());

            configurationRecordBuilder.updating(revisionDataInput.readBoolean());
        }

        private void write00(ReaderGroupConfigRecord rgConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeLong(rgConfigurationRecord.groupRefreshTimeMillis);
            revisionDataOutput.writeLong(rgConfigurationRecord.automaticCheckpointIntervalMillis);
            revisionDataOutput.writeInt(rgConfigurationRecord.maxOutstandingCheckpointRequest);
            revisionDataOutput.writeCompactInt(rgConfigurationRecord.retentionTypeOrdinal);
            revisionDataOutput.writeLong(rgConfigurationRecord.generation);
            revisionDataOutput.writeMap(rgConfigurationRecord.startingStreamCuts, DataOutput::writeUTF,
                    RGStreamCutRecord.SERIALIZER::serialize);
            revisionDataOutput.writeMap(rgConfigurationRecord.endingStreamCuts, DataOutput::writeUTF,
                    RGStreamCutRecord.SERIALIZER::serialize);
            revisionDataOutput.writeBoolean(rgConfigurationRecord.isUpdating());
        }

        @Override
        protected ReaderGroupConfigRecordBuilder newBuilder() {
            return ReaderGroupConfigRecord.builder();
        }
    }
}
