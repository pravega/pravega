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
package io.pravega.cli.admin.serializers.controller;

import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class SealedSegmentsMapShardSerializer extends AbstractSerializer {

    public static final String SEALED_SEGMENTS_MAP_SHARD_SHARD_NUMBER = "shardNumber";
    public static final String SEALED_SEGMENTS_MAP_SHARD_SEALED_SEGMENTS_SIZE_MAP = "sealedSegmentsSizeMap";

    private static final Map<String, Function<SealedSegmentsMapShard, String>> SEALED_SEGMENTS_MAP_SHARD_FIELD_MAP =
            ImmutableMap.<String, Function<SealedSegmentsMapShard, String>>builder()
                    .put(SEALED_SEGMENTS_MAP_SHARD_SHARD_NUMBER, r -> String.valueOf(r.getShardNumber()))
                    .put(SEALED_SEGMENTS_MAP_SHARD_SEALED_SEGMENTS_SIZE_MAP, r -> convertMapToString(r.getSealedSegmentsSizeMap(), String::valueOf, String::valueOf))
                    .build();

    @Override
    public String getName() {
        return "SealedSegmentsMapShard";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        Map<Long, Long> sealedSegmentsSizeMap = convertStringToMap(getAndRemoveIfExists(data, SEALED_SEGMENTS_MAP_SHARD_SEALED_SEGMENTS_SIZE_MAP),
                Long::parseLong, Long::parseLong, getName() + "." + SEALED_SEGMENTS_MAP_SHARD_SEALED_SEGMENTS_SIZE_MAP);

        SealedSegmentsMapShard record = SealedSegmentsMapShard.builder()
                .shardNumber(Integer.parseInt(getAndRemoveIfExists(data, SEALED_SEGMENTS_MAP_SHARD_SHARD_NUMBER)))
                .sealedSegmentsSizeMap(ImmutableMap.copyOf(sealedSegmentsSizeMap))
                .build();
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, SealedSegmentsMapShard::fromBytes, SEALED_SEGMENTS_MAP_SHARD_FIELD_MAP);
    }
}
