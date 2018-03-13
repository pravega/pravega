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

import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.serializers.StreamConfigurationRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@Slf4j
@AllArgsConstructor
public class StreamConfigurationRecord {

    public static final VersionedSerializer.WithBuilder<StreamConfigurationRecord,
            StreamConfigurationRecord.StreamConfigurationRecordBuilder> SERIALIZER = new StreamConfigurationRecordSerializer();

    private final StreamConfiguration streamConfiguration;
    private final boolean updating;

    public static StreamConfigurationRecord update(StreamConfiguration streamConfig) {
        return StreamConfigurationRecord.builder().streamConfiguration(streamConfig)
                .updating(true).build();
    }

    public static StreamConfigurationRecord complete(StreamConfiguration streamConfig) {
        return StreamConfigurationRecord.builder().streamConfiguration(streamConfig)
                .updating(false).build();
    }

    public static class StreamConfigurationRecordBuilder implements ObjectBuilder<StreamConfigurationRecord> {

    }

    public static StreamConfigurationRecord parse(byte[] data) {
        StreamConfigurationRecord streamConfigurationRecord;
        try {
            streamConfigurationRecord = SERIALIZER.deserialize(data);
        } catch (IOException e) {
            log.error("deserialization error for configuration record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return streamConfigurationRecord;
    }

    public byte[] toByteArray() {
        byte[] array;
        try {
            array = SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            log.error("error serializing configuration record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return array;
    }

    @Data
    @Builder
    @Slf4j
    @AllArgsConstructor
    public static class ScalingPolicyRecord {

        public static final VersionedSerializer.WithBuilder<ScalingPolicyRecord, ScalingPolicyRecordBuilder> SERIALIZER
                = new StreamConfigurationRecordSerializer.ScalingPolicyRecordSerializer();

        private final ScalingPolicy scalingPolicy;

        public static class ScalingPolicyRecordBuilder implements ObjectBuilder<ScalingPolicyRecord> {

        }

        public static ScalingPolicyRecord parse(byte[] data) {
            ScalingPolicyRecord scalingPolicyRecord;
            try {
                scalingPolicyRecord = SERIALIZER.deserialize(data);
            } catch (IOException e) {
                log.error("deserialization error for configuration's scaling policy record {}", e);
                throw Lombok.sneakyThrow(e);
            }
            return scalingPolicyRecord;
        }

        public byte[] toByteArray() {
            byte[] array;
            try {
                array = SERIALIZER.serialize(this).array();
            } catch (IOException e) {
                log.error("error serializing configuration's scaling policy record {}", e);
                throw Lombok.sneakyThrow(e);
            }
            return array;
        }
    }

    @Data
    @Builder
    @Slf4j
    @AllArgsConstructor
    public static class RetentionPolicyRecord {

        public static final VersionedSerializer.WithBuilder<RetentionPolicyRecord, RetentionPolicyRecordBuilder> SERIALIZER
                = new StreamConfigurationRecordSerializer.RetentionPolicyRecordSerializer();

        private final RetentionPolicy retentionPolicy;

        public static class RetentionPolicyRecordBuilder implements ObjectBuilder<RetentionPolicyRecord> {

        }

        public static RetentionPolicyRecord parse(byte[] data) {
            RetentionPolicyRecord retentionPolicyRecord;
            try {
                retentionPolicyRecord = SERIALIZER.deserialize(data);
            } catch (IOException e) {
                log.error("deserialization error for configuration's retention policy record {}", e);
                throw Lombok.sneakyThrow(e);
            }
            return retentionPolicyRecord;
        }

        public byte[] toByteArray() {
            byte[] array;
            try {
                array = SERIALIZER.serialize(this).array();
            } catch (IOException e) {
                log.error("error serializing configuration's retention policy record {}", e);
                throw Lombok.sneakyThrow(e);
            }
            return array;
        }
    }

}
