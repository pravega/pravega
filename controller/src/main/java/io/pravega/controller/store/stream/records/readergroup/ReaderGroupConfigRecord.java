package io.pravega.controller.store.stream.records.readergroup;

import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

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
    private final long retentionTypeOrdinal;
    private final long generation;
    private final Map<String, Map<Long, Long>> startingStreamCuts;
    private final Map<String, Map<Long, Long>> endingStreamCuts;
    private final boolean updating;

    public static class ReaderGroupConfigRecordBuilder implements ObjectBuilder<StreamConfigurationRecord> {

    }
    private static class ConfigurationRecordSerializer
            extends VersionedSerializer.WithBuilder<StreamConfigurationRecord,
            ReaderGroupConfigRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00)
                    .revision(1, this::write01, this::read01);
        }

        @Override
        protected void beforeSerialization(StreamConfigurationRecord streamConfigurationRecord) {
            Preconditions.checkNotNull(streamConfigurationRecord);
            Preconditions.checkNotNull(streamConfigurationRecord.getStreamConfiguration());
        }

        private void read00(RevisionDataInput revisionDataInput,
                            ReaderGroupConfigRecordBuilder configurationRecordBuilder)
                throws IOException {
            configurationRecordBuilder.scope(revisionDataInput.readUTF())
                    .streamName(revisionDataInput.readUTF());
            StreamConfiguration.StreamConfigurationBuilder streamConfigurationBuilder = StreamConfiguration.builder();
            streamConfigurationBuilder.scalingPolicy(StreamConfigurationRecord.ScalingPolicyRecord.SERIALIZER.deserialize(revisionDataInput).getScalingPolicy())
                    .retentionPolicy(StreamConfigurationRecord.RetentionPolicyRecord.SERIALIZER.deserialize(revisionDataInput).getRetentionPolicy());
            configurationRecordBuilder.streamConfiguration(streamConfigurationBuilder.build())
                    .updating(revisionDataInput.readBoolean());
        }

        private void write00(StreamConfigurationRecord streamConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(streamConfigurationRecord.getScope());
            revisionDataOutput.writeUTF(streamConfigurationRecord.getStreamName());
            StreamConfigurationRecord.ScalingPolicyRecord.SERIALIZER.serialize(revisionDataOutput,
                    new StreamConfigurationRecord.ScalingPolicyRecord(streamConfigurationRecord.getStreamConfiguration().getScalingPolicy()));
            StreamConfigurationRecord.RetentionPolicyRecord.SERIALIZER.serialize(revisionDataOutput,
                    new StreamConfigurationRecord.RetentionPolicyRecord(streamConfigurationRecord.getStreamConfiguration().getRetentionPolicy()));
            revisionDataOutput.writeBoolean(streamConfigurationRecord.isUpdating());
        }

        private void read01(RevisionDataInput revisionDataInput,
                            StreamConfigurationRecordBuilder configurationRecordBuilder)
                throws IOException {
            StreamConfiguration.StreamConfigurationBuilder streamConfigurationBuilder = StreamConfiguration.builder();
            streamConfigurationBuilder.scalingPolicy(configurationRecordBuilder.streamConfiguration.getScalingPolicy())
                    .retentionPolicy(configurationRecordBuilder.streamConfiguration.getRetentionPolicy())
                    .timestampAggregationTimeout(revisionDataInput.readLong());
            configurationRecordBuilder.streamConfiguration(streamConfigurationBuilder.build());
        }

        private void write01(StreamConfigurationRecord streamConfigurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeLong(streamConfigurationRecord.streamConfiguration.getTimestampAggregationTimeout());
        }

        @Override
        protected ReaderGroupConfigRecordBuilder newBuilder() {
            return ReaderGroupConfigRecord.builder();
        }
    }
}
