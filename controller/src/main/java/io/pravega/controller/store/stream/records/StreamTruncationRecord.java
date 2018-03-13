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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.records.serializers.StreamTruncationRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Data class for storing information about stream's truncation point.
 */
@Data
@Builder
@Slf4j
public class StreamTruncationRecord  {
    public static final VersionedSerializer.WithBuilder<StreamTruncationRecord,
            StreamTruncationRecord.StreamTruncationRecordBuilder> SERIALIZER = new StreamTruncationRecordSerializer();

    public static final StreamTruncationRecord EMPTY = new StreamTruncationRecord(ImmutableMap.of(),
            ImmutableMap.of(), ImmutableSet.of(), ImmutableSet.of(), false);

    /**
     * Stream cut that is applied as part of this truncation.
     */
    private final Map<Integer, Long> streamCut;

    /**
     * If a stream cut spans across multiple epochs then this map captures mapping of segments from the stream cut to
     * epochs they were found in closest to truncation point.
     * This data structure is used to find active segments wrt a stream cut.
     * So for example:
     * epoch 0: 0, 1
     * epoch 1: 0, 2, 3
     * epoch 2: 0, 2, 4, 5
     * epoch 3: 0, 4, 5, 6, 7
     *
     * Following is a valid stream cut {0/offset, 3/offset, 6/offset, 7/offset}
     * This spans from epoch 1 till epoch 3. Any request for segments at epoch 1 or 2 or 3 will need to have this stream cut
     * applied on it to find segments that are available for consumption.
     * Refer to TableHelper.getActiveSegmentsAt
     */
    private final Map<Integer, Integer> cutEpochMap;
    /**
     * All segments that have been deleted for this stream so far.
     */
    private final Set<Integer> deletedSegments;
    /**
     * Segments to delete as part of this truncation.
     * This is non empty while truncation is ongoing.
     * This is reset to empty once truncation completes by calling mergeDeleted method.
     */
    private final Set<Integer> toDelete;

    private final boolean updating;

    public StreamTruncationRecord(Map<Integer, Long> streamCut,
                                  Map<Integer, Integer> cutEpochMap,
                                  Set<Integer> deletedSegments,
                                  Set<Integer> toDelete,
                                  boolean updating) {
        this.streamCut = ImmutableMap.copyOf(streamCut);
        this.cutEpochMap = ImmutableMap.copyOf(cutEpochMap);
        this.deletedSegments = ImmutableSet.copyOf(deletedSegments);
        this.toDelete = ImmutableSet.copyOf(toDelete);
        this.updating = updating;
    }

    int getTruncationEpochLow() {
        return cutEpochMap.values().stream().min(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    int getTruncationEpochHigh() {
        return cutEpochMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    public Map<Integer, Long> getStreamCut() {
        return streamCut;
    }

    public Map<Integer, Integer> getCutEpochMap() {
        return cutEpochMap;
    }

    public Set<Integer> getDeletedSegments() {
        return deletedSegments;
    }

    public Set<Integer> getToDelete() {
        return toDelete;
    }
    
    public static StreamTruncationRecord complete(StreamTruncationRecord toComplete) {
        Preconditions.checkState(toComplete.updating);
        Set<Integer> deleted = new HashSet<>(toComplete.deletedSegments);
        deleted.addAll(toComplete.toDelete);

        return StreamTruncationRecord.builder()
                .updating(false)
                .cutEpochMap(toComplete.cutEpochMap)
                .streamCut(toComplete.streamCut)
                .deletedSegments(deleted)
                .toDelete(ImmutableSet.of())
                .build();
    }

    public static class StreamTruncationRecordBuilder implements ObjectBuilder<StreamTruncationRecord> {

    }

    public static StreamTruncationRecord parse(byte[] data) {
        StreamTruncationRecord streamTruncationRecord;
        try {
            streamTruncationRecord = SERIALIZER.deserialize(data);
        } catch (IOException e) {
            log.error("deserialization error for stream truncation record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return streamTruncationRecord;
    }

    public byte[] toByteArray() {
        byte[] array;
        try {
            array = SERIALIZER.serialize(this).array();
        } catch (IOException e) {
            log.error("error serializing stream truncation record {}", e);
            throw Lombok.sneakyThrow(e);
        }
        return array;
    }

}
