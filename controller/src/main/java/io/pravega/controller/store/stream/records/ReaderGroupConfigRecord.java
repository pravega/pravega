package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
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

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class ReaderGroupConfigRecord {
    public static final ReaderGroupConfigRecord.ConfigurationRecordSerializer SERIALIZER = new ReaderGroupConfigRecord.ConfigurationRecordSerializer();

    private final long groupRefreshTimeMillis;
    private final long automaticCheckpointIntervalMillis;
    private final long maxOutstandingCheckpointRequest;
    private final int retentionTypeOrdinal;
    private final long generation;
    private final Map<String, RGStreamCutRecord> startingStreamCuts;
    private final Map<String, RGStreamCutRecord> endingStreamCuts;
    private final boolean updating;

    public static class ReaderGroupConfigRecordBuilder implements ObjectBuilder<ReaderGroupConfigRecord> {

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
            configurationRecordBuilder.maxOutstandingCheckpointRequest(revisionDataInput.readLong());
            configurationRecordBuilder.retentionTypeOrdinal(revisionDataInput.readCompactInt());
            configurationRecordBuilder.generation(revisionDataInput.readLong());

            ImmutableMap.Builder<String, RGStreamCutRecord> startStreamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readUTF, RGStreamCutRecord.SERIALIZER::deserialize , startStreamCutBuilder);
            configurationRecordBuilder.startingStreamCuts(startStreamCutBuilder.build());

            ImmutableMap.Builder<String, RGStreamCutRecord> endStreamCutBuilder = ImmutableMap.builder();
            revisionDataInput.readMap(DataInput::readUTF, RGStreamCutRecord.SERIALIZER::deserialize , endStreamCutBuilder);
            configurationRecordBuilder.endingStreamCuts(endStreamCutBuilder.build());

            configurationRecordBuilder.updating(revisionDataInput.readBoolean());
        }

        private void write00(ReaderGroupConfigRecord rgConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeLong(rgConfigurationRecord.groupRefreshTimeMillis);
            revisionDataOutput.writeLong(rgConfigurationRecord.automaticCheckpointIntervalMillis);
            revisionDataOutput.writeLong(rgConfigurationRecord.maxOutstandingCheckpointRequest);
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
