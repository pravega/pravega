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
import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.serializers.EpochTransitionRecordSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.AbstractMap;

/**
 * Transient record that is created while epoch transition takes place and captures the transition. This record is deleted
 * once transition completes.
 */
@Data
@Builder
@AllArgsConstructor
@Slf4j
public class EpochTransitionRecord {
    public static final VersionedSerializer.WithBuilder<EpochTransitionRecord, EpochTransitionRecordBuilder> SERIALIZER
            = new EpochTransitionRecordSerializer();

    /**
     * Active epoch at the time of requested transition.
     */
    final int activeEpoch;
    /**
     * New last epoch to being created at the end of the request.
     */
    final int newEpoch;
    /**
     * Time when this epoch creation request was started.
     */
    final long time;
    /**
     * Segments to be sealed.
     */
    final ImmutableSet<Integer> segmentsToSeal;
    /**
     * Key ranges for new segments to be created.
     */
    ImmutableMap<Integer, AbstractMap.SimpleEntry<Double, Double>> newSegmentsWithRange;

    public static class EpochTransitionRecordBuilder implements ObjectBuilder<EpochTransitionRecord> {

    }

    public static EpochTransitionRecord parse(byte[] data) {
        EpochTransitionRecord epochTransitionRecord;
        try {
            epochTransitionRecord = SERIALIZER.deserialize(data);
        } catch (IOException e) {
            log.error("deserialization error for epoch transition record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return epochTransitionRecord;
    }

    public byte[] toByteArray() {
        byte[] array;
        try {
            array = SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            log.error("error serializing epoch transition record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return array;
    }
}
