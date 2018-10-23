/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class ConfigurationRecord {

    public static final ConfigurationRecordSerializer SERIALIZER = new ConfigurationRecordSerializer();

    private final StreamConfiguration streamConfiguration;
    private final boolean updating;

    public static ConfigurationRecord update(StreamConfiguration streamConfig) {
        return ConfigurationRecord.builder().streamConfiguration(streamConfig)
                                  .updating(true).build();
    }

    public static ConfigurationRecord complete(StreamConfiguration streamConfig) {
        return ConfigurationRecord.builder().streamConfiguration(streamConfig)
                                  .updating(false).build();
    }

    public static class ConfigurationRecordBuilder implements ObjectBuilder<ConfigurationRecord> {

    }

    @SneakyThrows(IOException.class)
    public static ConfigurationRecord fromBytes(final byte[] data) {
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
                VersionedSerializer.WithBuilder<ConfigurationRecord.ScalingPolicyRecord,
                        ConfigurationRecord.ScalingPolicyRecord.ScalingPolicyRecordBuilder> {
            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, ConfigurationRecord.ScalingPolicyRecord.ScalingPolicyRecordBuilder scalingPolicyRecordBuilder)
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

            private void write00(ConfigurationRecord.ScalingPolicyRecord scalingPolicyRecord, RevisionDataOutput revisionDataOutput) throws IOException {
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
            protected ConfigurationRecord.ScalingPolicyRecord.ScalingPolicyRecordBuilder newBuilder() {
                return ConfigurationRecord.ScalingPolicyRecord.builder();
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
                VersionedSerializer.WithBuilder<ConfigurationRecord.RetentionPolicyRecord, ConfigurationRecord.RetentionPolicyRecord.RetentionPolicyRecordBuilder> {
            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, ConfigurationRecord.RetentionPolicyRecord.RetentionPolicyRecordBuilder retentionPolicyRecordBuilder)
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

            private void write00(ConfigurationRecord.RetentionPolicyRecord retentionPolicyRecord, RevisionDataOutput revisionDataOutput)
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
            protected ConfigurationRecord.RetentionPolicyRecord.RetentionPolicyRecordBuilder newBuilder() {
                return ConfigurationRecord.RetentionPolicyRecord.builder();
            }
        }
    }

    private static class ConfigurationRecordSerializer
            extends VersionedSerializer.WithBuilder<ConfigurationRecord,
            ConfigurationRecord.ConfigurationRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(ConfigurationRecord configurationRecord) {
            Preconditions.checkNotNull(configurationRecord);
            Preconditions.checkNotNull(configurationRecord.getStreamConfiguration());
        }

        private void read00(RevisionDataInput revisionDataInput,
                            ConfigurationRecord.ConfigurationRecordBuilder configurationRecordBuilder)
                throws IOException {
            StreamConfiguration.StreamConfigurationBuilder streamConfigurationBuilder = StreamConfiguration.builder();
            streamConfigurationBuilder.scope(revisionDataInput.readUTF())
                                      .streamName(revisionDataInput.readUTF())
                                      .scalingPolicy(ConfigurationRecord.ScalingPolicyRecord.SERIALIZER.deserialize(revisionDataInput).getScalingPolicy())
                                      .retentionPolicy(ConfigurationRecord.RetentionPolicyRecord.SERIALIZER.deserialize(revisionDataInput).getRetentionPolicy());
            configurationRecordBuilder.streamConfiguration(streamConfigurationBuilder.build())
                                            .updating(revisionDataInput.readBoolean());
        }

        private void write00(ConfigurationRecord configurationRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            revisionDataOutput.writeUTF(configurationRecord.getStreamConfiguration().getScope());
            revisionDataOutput.writeUTF(configurationRecord.getStreamConfiguration().getStreamName());
            ConfigurationRecord.ScalingPolicyRecord.SERIALIZER.serialize(revisionDataOutput,
                    new ConfigurationRecord.ScalingPolicyRecord(configurationRecord.getStreamConfiguration().getScalingPolicy()));
            ConfigurationRecord.RetentionPolicyRecord.SERIALIZER.serialize(revisionDataOutput,
                    new ConfigurationRecord.RetentionPolicyRecord(configurationRecord.getStreamConfiguration().getRetentionPolicy()));
            revisionDataOutput.writeBoolean(configurationRecord.isUpdating());
        }

        @Override
        protected ConfigurationRecord.ConfigurationRecordBuilder newBuilder() {
            return ConfigurationRecord.builder();
        }
    }
}
