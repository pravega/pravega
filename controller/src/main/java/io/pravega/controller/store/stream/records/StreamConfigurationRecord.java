/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class StreamConfigurationRecord {

    public static final ConfigurationRecordSerializer SERIALIZER = new ConfigurationRecordSerializer();

    @NonNull
    private final String scope;
    @NonNull
    private final String streamName;
    private final StreamConfiguration streamConfiguration;
    private final boolean updating;

    public static StreamConfigurationRecord update(String scope, String streamName, StreamConfiguration streamConfig) {
        return StreamConfigurationRecord.builder().scope(scope).streamName(streamName).streamConfiguration(streamConfig)
                                        .updating(true).build();
    }

    public static StreamConfigurationRecord complete(String scope, String streamName, StreamConfiguration streamConfig) {
        return StreamConfigurationRecord.builder().scope(scope).streamName(streamName).streamConfiguration(streamConfig)
                                        .updating(false).build();
    }

    public static class StreamConfigurationRecordBuilder implements ObjectBuilder<StreamConfigurationRecord> {

    }

    @SneakyThrows(IOException.class)
    public static StreamConfigurationRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Data
    @Builder
    @Slf4j
    @AllArgsConstructor
    public static class ScalingPolicyRecord {

        public static final ScalingPolicyRecordSerializer SERIALIZER = new ScalingPolicyRecordSerializer();

        private final ScalingPolicy scalingPolicy;

        public static class ScalingPolicyRecordBuilder implements ObjectBuilder<ScalingPolicyRecord> {

        }

        private static class ScalingPolicyRecordSerializer extends
                VersionedSerializer.WithBuilder<StreamConfigurationRecord.ScalingPolicyRecord,
                        StreamConfigurationRecord.ScalingPolicyRecord.ScalingPolicyRecordBuilder> {
            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, StreamConfigurationRecord.ScalingPolicyRecord.ScalingPolicyRecordBuilder scalingPolicyRecordBuilder)
                    throws IOException {
                boolean exists = revisionDataInput.readBoolean();
                if (exists) {
                    int ordinal = revisionDataInput.readCompactInt();
                    scalingPolicyRecordBuilder.scalingPolicy(ScalingPolicy.builder()
                                                                          .scaleType(ScalingPolicy.ScaleType.values()[ordinal])
                                                                          .targetRate(revisionDataInput.readInt())
                                                                          .scaleFactor(revisionDataInput.readInt())
                                                                          .minNumSegments(revisionDataInput.readInt()).build());
                } else {
                    scalingPolicyRecordBuilder.scalingPolicy(null);
                }
            }

            private void write00(StreamConfigurationRecord.ScalingPolicyRecord scalingPolicyRecord, RevisionDataOutput revisionDataOutput) throws IOException {
                if (scalingPolicyRecord == null || scalingPolicyRecord.getScalingPolicy() == null) {
                    revisionDataOutput.writeBoolean(false);
                } else {
                    revisionDataOutput.writeBoolean(true);
                    ScalingPolicy scalingPolicy = scalingPolicyRecord.getScalingPolicy();
                    revisionDataOutput.writeCompactInt(scalingPolicy.getScaleType().ordinal());
                    revisionDataOutput.writeInt(scalingPolicy.getTargetRate());
                    revisionDataOutput.writeInt(scalingPolicy.getScaleFactor());
                    revisionDataOutput.writeInt(scalingPolicy.getMinNumSegments());
                }
            }

            @Override
            protected StreamConfigurationRecord.ScalingPolicyRecord.ScalingPolicyRecordBuilder newBuilder() {
                return StreamConfigurationRecord.ScalingPolicyRecord.builder();
            }
        }
    }

    @Data
    @Builder
    @Slf4j
    @AllArgsConstructor
    public static class RetentionPolicyRecord {

        public static final RetentionPolicyRecordSerializer SERIALIZER = new RetentionPolicyRecordSerializer();

        private final RetentionPolicy retentionPolicy;

        public static class RetentionPolicyRecordBuilder implements ObjectBuilder<RetentionPolicyRecord> {

        }

        private static class RetentionPolicyRecordSerializer extends
                VersionedSerializer.WithBuilder<StreamConfigurationRecord.RetentionPolicyRecord, StreamConfigurationRecord.RetentionPolicyRecord.RetentionPolicyRecordBuilder> {
            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, StreamConfigurationRecord.RetentionPolicyRecord.RetentionPolicyRecordBuilder retentionPolicyRecordBuilder)
                    throws IOException {
                boolean exists = revisionDataInput.readBoolean();
                if (exists) {
                    retentionPolicyRecordBuilder.retentionPolicy(
                            RetentionPolicy.builder().retentionType(
                                    RetentionPolicy.RetentionType.values()[revisionDataInput.readCompactInt()])
                                           .retentionParam(revisionDataInput.readLong()).build());
                } else {
                    retentionPolicyRecordBuilder.retentionPolicy(null);
                }
            }

            private void write00(StreamConfigurationRecord.RetentionPolicyRecord retentionPolicyRecord, RevisionDataOutput revisionDataOutput)
                    throws IOException {
                if (retentionPolicyRecord == null || retentionPolicyRecord.getRetentionPolicy() == null) {
                    revisionDataOutput.writeBoolean(false);
                } else {
                    revisionDataOutput.writeBoolean(true);
                    RetentionPolicy retentionPolicy = retentionPolicyRecord.getRetentionPolicy();
                    revisionDataOutput.writeCompactInt(retentionPolicy.getRetentionType().ordinal());
                    revisionDataOutput.writeLong(retentionPolicy.getRetentionParam());
                }
            }

            @Override
            protected StreamConfigurationRecord.RetentionPolicyRecord.RetentionPolicyRecordBuilder newBuilder() {
                return StreamConfigurationRecord.RetentionPolicyRecord.builder();
            }
        }
    }

    private static class ConfigurationRecordSerializer
            extends VersionedSerializer.WithBuilder<StreamConfigurationRecord,
            StreamConfigurationRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(StreamConfigurationRecord streamConfigurationRecord) {
            Preconditions.checkNotNull(streamConfigurationRecord);
            Preconditions.checkNotNull(streamConfigurationRecord.getStreamConfiguration());
        }

        private void read00(RevisionDataInput revisionDataInput,
                            StreamConfigurationRecordBuilder configurationRecordBuilder)
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

        @Override
        protected StreamConfigurationRecordBuilder newBuilder() {
            return StreamConfigurationRecord.builder();
        }
    }
}
