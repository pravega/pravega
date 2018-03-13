/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records.serializers;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;

import java.io.IOException;

import static io.pravega.controller.store.stream.records.StreamConfigurationRecord.RetentionPolicyRecord;
import static io.pravega.controller.store.stream.records.StreamConfigurationRecord.RetentionPolicyRecord.RetentionPolicyRecordBuilder;
import static io.pravega.controller.store.stream.records.StreamConfigurationRecord.ScalingPolicyRecord;
import static io.pravega.controller.store.stream.records.StreamConfigurationRecord.ScalingPolicyRecord.ScalingPolicyRecordBuilder;
import static io.pravega.controller.store.stream.records.StreamConfigurationRecord.StreamConfigurationRecordBuilder;

public class StreamConfigurationRecordSerializer
        extends VersionedSerializer.WithBuilder<StreamConfigurationRecord,
        StreamConfigurationRecordBuilder> {
    @Override
    protected byte writeVersion() {
        return 0;
    }

    @Override
    protected void declareVersions() {
        version(0).revision(0, this::write00, this::read00);
    }

    private void read00(RevisionDataInput revisionDataInput,
                        StreamConfigurationRecordBuilder streamConfigurationRecordBuilder)
            throws IOException {
        // streamConfigurationRecordBuilder.streamConfiguration()
        StreamConfiguration.StreamConfigurationBuilder streamConfigurationBuilder = StreamConfiguration.builder();
        streamConfigurationBuilder.scope(revisionDataInput.readUTF())
                .streamName(revisionDataInput.readUTF())
                .scalingPolicy(ScalingPolicyRecord.SERIALIZER.deserialize(revisionDataInput).getScalingPolicy())
                .retentionPolicy(RetentionPolicyRecord.SERIALIZER.deserialize(revisionDataInput).getRetentionPolicy());
        streamConfigurationRecordBuilder.streamConfiguration(streamConfigurationBuilder.build())
                .updating(revisionDataInput.readBoolean());
    }

    private void write00(StreamConfigurationRecord streamConfigurationRecord, RevisionDataOutput revisionDataOutput)
            throws IOException {
        Preconditions.checkNotNull(streamConfigurationRecord);
        Preconditions.checkNotNull(streamConfigurationRecord.getStreamConfiguration());
        revisionDataOutput.writeUTF(streamConfigurationRecord.getStreamConfiguration().getScope());
        revisionDataOutput.writeUTF(streamConfigurationRecord.getStreamConfiguration().getStreamName());
        ScalingPolicyRecord.SERIALIZER.serialize(revisionDataOutput,
                new ScalingPolicyRecord(streamConfigurationRecord.getStreamConfiguration().getScalingPolicy()));
        RetentionPolicyRecord.SERIALIZER.serialize(revisionDataOutput,
                new RetentionPolicyRecord(streamConfigurationRecord.getStreamConfiguration().getRetentionPolicy()));
        revisionDataOutput.writeBoolean(streamConfigurationRecord.isUpdating());
    }

    @Override
    protected StreamConfigurationRecordBuilder newBuilder() {
        return StreamConfigurationRecord.builder();
    }

    public static class ScalingPolicyRecordSerializer extends
            VersionedSerializer.WithBuilder<ScalingPolicyRecord,
                    ScalingPolicyRecordBuilder> {
        @Override
        protected byte writeVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, ScalingPolicyRecordBuilder scalingPolicyRecordBuilder)
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

        private void write00(ScalingPolicyRecord scalingPolicyRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            if (scalingPolicyRecord == null || scalingPolicyRecord.getScalingPolicy() == null) {
                revisionDataOutput.writeBoolean(false);
                revisionDataOutput.write(new byte[0]);
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
        protected ScalingPolicyRecordBuilder newBuilder() {
            return ScalingPolicyRecord.builder();
        }
    }

    public static class RetentionPolicyRecordSerializer extends
            VersionedSerializer.WithBuilder<RetentionPolicyRecord, RetentionPolicyRecordBuilder> {
        @Override
        protected byte writeVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, RetentionPolicyRecordBuilder retentionPolicyRecordBuilder)
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

        private void write00(RetentionPolicyRecord retentionPolicyRecord, RevisionDataOutput revisionDataOutput)
                throws IOException {
            if (retentionPolicyRecord == null || retentionPolicyRecord.getRetentionPolicy() == null) {
                revisionDataOutput.writeBoolean(false);
                revisionDataOutput.write(new byte[0]);
            } else {
                revisionDataOutput.writeBoolean(true);
                RetentionPolicy retentionPolicy = retentionPolicyRecord.getRetentionPolicy();
                revisionDataOutput.writeCompactInt(retentionPolicy.getRetentionType().ordinal());
                revisionDataOutput.writeLong(retentionPolicy.getRetentionParam());
            }
        }

        @Override
        protected RetentionPolicyRecordBuilder newBuilder() {
            return RetentionPolicyRecord.builder();
        }
    }

}
