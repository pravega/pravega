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

import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.EpochRecordSerializer;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Serializable class that captures an epoch record.
 */
public class EpochRecord {
    public static final EpochRecordSerializer SERIALIZER = new EpochRecordSerializer();

    @Getter
    private final int epoch;
    /**
     * The reference epoch is the original epoch that this current epoch duplicates.
     * If referenceEpoch is same as epoch, then this is a clean creation of epoch rather than a duplicate.
     * If we are creating a duplicate of an epoch that was already a duplicate, we set the reference to the parent.
     * This ensures that instead of having a chain of duplicates we have a tree of depth one where all duplicates
     * are children of original epoch as common parent.
     */
    @Getter
    private final int referenceEpoch;
    @Getter
    private final List<StreamSegmentRecord> segments;
    @Getter
    private final long creationTime;

    @Builder
    EpochRecord(int epoch, int referenceEpoch, List<StreamSegmentRecord> segments, long creationTime) {
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segments = ImmutableList.copyOf(segments);
        this.creationTime = creationTime;
    }

    @Builder
    EpochRecord(int epoch, List<StreamSegmentRecord> segments, long creationTime) {
        this(epoch, epoch, segments, creationTime);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public boolean isDuplicate() {
        return epoch != referenceEpoch;
    }

    @SneakyThrows(IOException.class)
    public static EpochRecord parse(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    public static class EpochRecordBuilder implements ObjectBuilder<EpochRecord> {

    }
}
