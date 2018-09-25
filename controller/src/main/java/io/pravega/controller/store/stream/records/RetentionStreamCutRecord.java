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

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.RetentionStreamCutRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 */
@Data
@Builder
@Slf4j
public class RetentionStreamCutRecord {
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
    final Map<StreamSegmentRecord, Long> streamCut;

    public RetentionStreamCutRecord(long recordingTime, long recordingSize, Map<StreamSegmentRecord, Long> streamCut) {
        this.recordingTime = recordingTime;
        this.recordingSize = recordingSize;
        this.streamCut = ImmutableMap.copyOf(streamCut);
    }

    public RetentionSetRecord getRetentionRecord() {
        return new RetentionSetRecord(recordingTime, recordingSize);
    }

    public static class RetentionStreamCutRecordBuilder implements ObjectBuilder<RetentionStreamCutRecord> {

    }

    @SneakyThrows(IOException.class)
    public static RetentionStreamCutRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}
