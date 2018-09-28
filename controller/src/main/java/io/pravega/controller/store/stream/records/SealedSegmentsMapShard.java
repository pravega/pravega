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
import io.pravega.controller.store.stream.records.serializers.SealedSegmentsMapShardSerializer;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Data class for storing information about stream's truncation point.
 */
@Builder
@Slf4j
@Data
/**
 * Sealed Segments Map is divided into multiple shards with each shard containing sealed sizes for a group of segments
 * identified by some hashing function.
 * For example, a simple modulo of segment number divided by shard_size can computed to determine the shard where the
 * segmentIds sealed size should be stored. Another alternative is to take epoch from the segmentid and use it to compute the
 * shard number.
 * So each shard can theoretically contain unbounded number of segment ids but if a good hash is chosen, we should not have very large number of
 * entries in it.
 */
public class SealedSegmentsMapShard {
    public static final SealedSegmentsMapShardSerializer SERIALIZER = new SealedSegmentsMapShardSerializer();
    public static final int SHARD_SIZE = 10000;

    private final int shardNumber;
    /**
     * Sealed segments with size at the time of sealing.
     * segmentId -> sealed segment record.
     * Each shard contains segments from a range of `segment epoch`.
     * So each shard size would be say 10k segment numbers. Then the number of records in the map would be 10k
     * * average number of segments per epoch.
     *
     * So to get sealed segment record -> extract segment epoch from segment id. compute the shard by dividing segment number by 10k.
     * Fetch the record from the shard.
     */
    private final Map<Long, Long> sealedSegmentsSizeMap;

    SealedSegmentsMapShard(int shardNumber, Map<Long, Long> sealedSegmentsSizeMap) {
        this.shardNumber = shardNumber;
        this.sealedSegmentsSizeMap = new HashMap<>(sealedSegmentsSizeMap);
    }

    public static class SealedSegmentsMapShardBuilder implements ObjectBuilder<SealedSegmentsMapShard> {

    }

    @SneakyThrows(IOException.class)
    public static SealedSegmentsMapShard parse(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toByteArray() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Synchronized
    public long getSize(long segmentId) {
        return sealedSegmentsSizeMap.get(segmentId);
    }

    @Synchronized
    public void addSealedSegmentSize(long segmentId, long sealedSize) {
        sealedSegmentsSizeMap.putIfAbsent(segmentId, sealedSize);
    }

    @Synchronized
    public Map<Long, Long> getSealedSegmentsSizeMap() {
        return Collections.unmodifiableMap(sealedSegmentsSizeMap);
    }

    public static int getShardNumber(long segmentId, int shardChunkSize) {
        return StreamSegmentNameUtils.getEpoch(segmentId) / shardChunkSize;
    }
}
