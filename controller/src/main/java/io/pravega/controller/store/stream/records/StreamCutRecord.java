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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.serializers.StreamCutRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * This is data class for storing stream cut with time when the cut was computed.
 * And the size of data being cut.
 */
@Data
@Builder
@AllArgsConstructor
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
}
