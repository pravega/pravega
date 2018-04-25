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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.tables.serializers.StreamCutRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 */
@Data
@Builder
@AllArgsConstructor
@Slf4j
public class StreamCutRecord {
    public static final VersionedSerializer.WithBuilder<StreamCutRecord, StreamCutRecordBuilder> SERIALIZER
            = new StreamCutRecordSerializer();

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
    final Map<Integer, Long> streamCut;

    public static class StreamCutRecordBuilder implements ObjectBuilder<StreamCutRecord> {

    }

    public static StreamCutRecord parse(byte[] data) {
        StreamCutRecord retention;
        try {
            retention = SERIALIZER.deserialize(data);
        } catch (IOException e) {
            log.error("Exception while deserializing streamcut record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return retention;
    }

    public static byte[] toByteArray(StreamCutRecord record) {
        byte[] array;
        try {
            array = SERIALIZER.serialize(record).getCopy();
        } catch (IOException e) {
            log.error("Exception while serializing streamcut record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return array;
    }

}
