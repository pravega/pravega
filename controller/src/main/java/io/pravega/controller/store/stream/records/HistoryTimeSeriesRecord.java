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
import io.pravega.controller.store.stream.records.serializers.HistoryTimeSeriesRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Data
public class HistoryTimeSeriesRecord {
    public static final HistoryTimeSeriesRecordSerializer SERIALIZER = new HistoryTimeSeriesRecordSerializer();

    @Getter
    private final int epoch;
    @Getter
    private final int referenceEpoch;
    @Getter
    private final List<StreamSegmentRecord> segmentsSealed;

    @Getter
    private final List<StreamSegmentRecord> segmentsCreated;

    @Getter
    private final long scaleTime;

    @Builder
    HistoryTimeSeriesRecord(int epoch, int referenceEpoch, List<StreamSegmentRecord> segmentsSealed, List<StreamSegmentRecord> segmentsCreated,
                            long creationTime) {
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segmentsSealed = ImmutableList.copyOf(segmentsSealed);
        this.segmentsCreated = ImmutableList.copyOf(segmentsCreated);
        this.scaleTime = creationTime;
    }

    @Builder
    HistoryTimeSeriesRecord(int epoch, List<StreamSegmentRecord> segmentsSealed, List<StreamSegmentRecord> segmentsCreated, long creationTime) {
        this(epoch, epoch, segmentsSealed, segmentsCreated, creationTime);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public boolean isDuplicate() {
        return epoch != referenceEpoch;
    }

    @SneakyThrows(IOException.class)
    public static HistoryTimeSeriesRecord parse(final byte[] record) {
        InputStream inputStream = new ByteArrayInputStream(record, 0, record.length);
        return SERIALIZER.deserialize(inputStream);
    }

    public static class HistoryTimeSeriesRecordBuilder implements ObjectBuilder<HistoryTimeSeriesRecord> {

    }
}
