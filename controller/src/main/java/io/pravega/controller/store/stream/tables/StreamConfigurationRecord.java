/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;

import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.tables.serializers.StreamConfigurationRecordSerializer;
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
public class StreamConfigurationRecord {

    public static final StreamConfigurationRecordSerializer SERIALIZER = new StreamConfigurationRecordSerializer();

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

    @SneakyThrows(IOException.class)
    public static StreamConfigurationRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Data
    @Builder
    @Slf4j
    @AllArgsConstructor
    public static class ScalingPolicyRecord {

        public static final StreamConfigurationRecordSerializer.ScalingPolicyRecordSerializer SERIALIZER =
                new StreamConfigurationRecordSerializer.ScalingPolicyRecordSerializer();

        private final ScalingPolicy scalingPolicy;

        public static class ScalingPolicyRecordBuilder implements ObjectBuilder<ScalingPolicyRecord> {

        }

        @SneakyThrows(IOException.class)
        public static ScalingPolicyRecord parse(final byte[] data) {
            return SERIALIZER.deserialize(data);
        }

        @SneakyThrows(IOException.class)
        public byte[] toByteArray() {
            return SERIALIZER.serialize(this).getCopy();
        }
    }

    @Data
    @Builder
    @Slf4j
    @AllArgsConstructor
    public static class RetentionPolicyRecord {

        public static final StreamConfigurationRecordSerializer.RetentionPolicyRecordSerializer SERIALIZER =
                new StreamConfigurationRecordSerializer.RetentionPolicyRecordSerializer();

        private final RetentionPolicy retentionPolicy;

        public static class RetentionPolicyRecordBuilder implements ObjectBuilder<RetentionPolicyRecord> {

        }

        @SneakyThrows(IOException.class)
        public static RetentionPolicyRecord parse(final byte[] data) {
            return SERIALIZER.deserialize(data);
        }

        @SneakyThrows(IOException.class)
        public byte[] toByteArray() {
            return SERIALIZER.serialize(this).getCopy();
        }
    }
}
