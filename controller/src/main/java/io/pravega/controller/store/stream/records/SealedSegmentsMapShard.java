/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream.records;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.shared.NameUtils;
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
import java.util.stream.Collectors;

/*
 * Data class for storing information about stream's truncation point.
 */

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
@Builder
@Slf4j
@Data
public class SealedSegmentsMapShard {
    public static final SealedSegmentsMapShardSerializer SERIALIZER = new SealedSegmentsMapShardSerializer();
    public static final int SHARD_SIZE = 1000;

    private final int shardNumber;
    /**
     * We maintain a map of Sealed segments with size at the time of sealing. This map can grow very large,
     * O(number of segments), and hence we will store this in multiple shards. Each shard should ideally
     *  contain a limited number of entries, and we should be able to compute the shard just by looking at
     *  segmentId. A segmentId is composed of two parts, creationEpoch and segmentNumber, which gives
     * us two logical choices for sharding.
     *
     * If we were to use segmentNumber to identify shard for a segment, then each shard can contain an
     * arbitrarily large number of entries because with rolling transactions, there can be many duplicate
     * segments. If we use creationEpoch, then we can still have an unbounded number of entries because
     * there is no limit on the number of segments created in an epoch. However, it is expected with auto-scale
     * that this number is small for each epoch.
     *
     * Rolling transactions adds a level of complexity. All segments corresponding to a transaction are recreated
     * in an epoch and number this could be arbitrarily large. We do not foresee transactions in the vast majority
     * of the cases rolling for multiple consecutive epochs, though. As such, assuming a small number of epochs
     * for a rolling transaction and about 1000 segments per epoch on average as an educated guess, we can still
     * support million segments in each shard with a shard size of 1000.
     *
     * Each shard consequently contains segments from a range of epochs in batches of 1000.
     * To get a sealed segment record -&gt; extract segment epoch from segment id, and compute the shard
     * by taking the modulo of the segment number by the SHARD_SIZE. Fetch the size from the shard map.
     **/
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

    @Override
    public String toString() {
        return String.format("%s = %s", "shardNumber", shardNumber) + "\n" +
                String.format("%s = %s", "sealedSegmentsSizeMap", sealedSegmentsSizeMap.keySet().stream()
                .map(key -> key + " : " + sealedSegmentsSizeMap.get(key))
                .collect(Collectors.joining(", ", "{", "}")));
    }

    @Synchronized
    public Long getSize(long segmentId) {
        return sealedSegmentsSizeMap.getOrDefault(segmentId, null);
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
        return NameUtils.getEpoch(segmentId) / shardChunkSize;
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
