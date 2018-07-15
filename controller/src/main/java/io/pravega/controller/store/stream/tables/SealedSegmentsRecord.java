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
import io.pravega.controller.store.stream.tables.serializers.SealedSegmentsRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Data class for storing information about stream's truncation point.
 */
@Data
@Builder
@Slf4j
public class SealedSegmentsRecord {
    public static final SealedSegmentsRecordSerializer SERIALIZER = new SealedSegmentsRecordSerializer();

    /**
     * Sealed segments with size at the time of sealing.
     */
    private final Map<Long, Long> sealedSegmentsSizeMap;

    public SealedSegmentsRecord(Map<Long, Long> sealedSegmentsSizeMap) {
        this.sealedSegmentsSizeMap = Collections.unmodifiableMap(new HashMap<>(sealedSegmentsSizeMap));
    }

    public Map<Long, Long> getSealedSegmentsSizeMap() {
        return sealedSegmentsSizeMap;
    }

    public static class SealedSegmentsRecordBuilder implements ObjectBuilder<SealedSegmentsRecord> {

    }

    @SneakyThrows(IOException.class)
    public static SealedSegmentsRecord parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }
}
