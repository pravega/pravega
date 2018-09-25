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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.ObjectBuilder;
import io.pravega.controller.store.stream.records.serializers.StreamSegmentRecordSerializer;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Builder;
import lombok.Data;

import java.util.AbstractMap;

@Data
@Builder
public class StreamSegmentRecord {
    public static final StreamSegmentRecordSerializer SERIALIZER = new StreamSegmentRecordSerializer();

    private final int segmentNumber;
    private final int creationEpoch;
    private final long creationTime;
    private final double keyStart;
    private final double keyEnd;

    public static class StreamSegmentRecordBuilder implements ObjectBuilder<StreamSegmentRecord> {

    }

    public long segmentId() {
        return StreamSegmentNameUtils.computeSegmentId(segmentNumber, creationEpoch);
    }

    public boolean overlaps(final StreamSegmentRecord segment) {
        return segment.getKeyStart() > keyStart && segment.getKeyStart() < keyEnd;
    }

    public boolean overlaps(final double keyStart, final double keyEnd) {
        return keyEnd > this.keyStart && keyStart < this.keyEnd;
    }

    public static boolean overlaps(final AbstractMap.SimpleEntry<Double, Double> first,
                                   final AbstractMap.SimpleEntry<Double, Double> second) {
        return second.getValue() > first.getKey() && second.getKey() < first.getValue();
    }

    @VisibleForTesting
    public static StreamSegmentRecord newSegmentRecord(int num, int epoch, long time, double start, double end) {
        return StreamSegmentRecord.builder().segmentNumber(num).creationEpoch(epoch).creationTime(time).keyStart(start).keyEnd(end).build();
    }

}
