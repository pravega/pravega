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
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
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
 * Sealed Segments Map is divided into multiple shards with each shard containing sealed sizes
 * for a group of segments, with shard membership determined by a hash function. For example,
 * we can use a simple modulo of the segment number to determine the shard where the
 * corresponding sizes of the sealed segments are stored. An alternative is to take the epoch
 * from the segmentId to compute the shard number.
 *
 * Each shard can contain unbounded number of segment ids, but if we use a good hash function,
 * then the load is expected to be balanced across shards.
*/
public class SealedSegmentsMapShard {
    public static final SealedSegmentsMapShardSerializer SERIALIZER = new SealedSegmentsMapShardSerializer();
    public static final int SHARD_SIZE = 10000;

    private final int shardNumber;
    /**
     * Sealed segments with size at the time of sealing.
     * segmentId -> sealed segment record.
     * Each shard contains segments from a range of `segment epoch`.
     * So each shard size would be say 10k segment numbers. Then the number of records in the map would be 
     * 10k multiplied by `average number of segments per epoch`.
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
    public static SealedSegmentsMapShard fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
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
    
    private static class SealedSegmentsMapShardSerializer
            extends VersionedSerializer.WithBuilder<SealedSegmentsMapShard, SealedSegmentsMapShard.SealedSegmentsMapShardBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            SealedSegmentsMapShard.SealedSegmentsMapShardBuilder sealedSegmentsRecordBuilder) throws IOException {
            sealedSegmentsRecordBuilder.sealedSegmentsSizeMap(revisionDataInput.readMap(DataInput::readLong, DataInput::readLong));
        }

        private void write00(SealedSegmentsMapShard sealedSegmentsRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeMap(sealedSegmentsRecord.getSealedSegmentsSizeMap(), DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected SealedSegmentsMapShard.SealedSegmentsMapShardBuilder newBuilder() {
            return SealedSegmentsMapShard.builder();
        }
    }
}
