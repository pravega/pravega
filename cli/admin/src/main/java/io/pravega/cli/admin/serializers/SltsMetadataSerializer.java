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
package io.pravega.cli.admin.serializers;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.Serializer;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ReadIndexBlockMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

import static io.pravega.cli.admin.utils.SerializerUtils.appendField;
import static io.pravega.cli.admin.utils.SerializerUtils.getAndRemoveIfExists;
import static io.pravega.cli.admin.utils.SerializerUtils.parseStringData;

/**
 * An implementation of {@link Serializer} that converts a user-friendly string representing SLTS metadata.
 */
public class SltsMetadataSerializer implements Serializer<String> {
    private static final TransactionData.TransactionDataSerializer SERIALIZER = new TransactionData.TransactionDataSerializer();

    private static final Map<String, Function<ChunkMetadata, Object>> CHUNK_METADATA_FIELD_MAP =
            ImmutableMap.<String, Function<ChunkMetadata, Object>>builder()
                    .put("name", ChunkMetadata::getKey)
                    .put("length", ChunkMetadata::getLength)
                    .put("nextChunk", ChunkMetadata::getNextChunk)
                    .put("status", ChunkMetadata::getStatus)
                    .build();

    private static final Map<String, Function<SegmentMetadata, Object>> SEGMENT_METADATA_FIELD_MAP =
            ImmutableMap.<String, Function<SegmentMetadata, Object>>builder()
                    .put("name", SegmentMetadata::getKey)
                    .put("length", SegmentMetadata::getLength)
                    .put("chunkCount", SegmentMetadata::getChunkCount)
                    .put("startOffset", SegmentMetadata::getStartOffset)
                    .put("status", SegmentMetadata::getStatus)
                    .put("maxRollingLength", SegmentMetadata::getMaxRollinglength)
                    .put("firstChunk", SegmentMetadata::getFirstChunk)
                    .put("lastChunk", SegmentMetadata::getLastChunk)
                    .put("lastModified", SegmentMetadata::getLastModified)
                    .put("firstChunkStartOffset", SegmentMetadata::getFirstChunkStartOffset)
                    .put("lastChunkStartOffset", SegmentMetadata::getLastChunkStartOffset)
                    .put("ownerEpoch", SegmentMetadata::getOwnerEpoch)
                    .build();

    private static final Map<String, Function<ReadIndexBlockMetadata, Object>> READ_INDEX_BLOCK_METADATA_FIELD_MAP =
            ImmutableMap.<String, Function<ReadIndexBlockMetadata, Object>>builder()
                    .put("name", ReadIndexBlockMetadata::getKey)
                    .put("chunkName", ReadIndexBlockMetadata::getChunkName)
                    .put("startOffset", ReadIndexBlockMetadata::getStartOffset)
                    .put("status", ReadIndexBlockMetadata::getStatus)
                    .build();

    @Override
    public ByteBuffer serialize(String value) {
        ByteBuffer buf;
        try {
            // Convert string to map with fields and values.
            Map<String, String> data = parseStringData(value);
            // Use the map to build TransactionData. If the field/key queried does not exist we throw an IllegalArgumentException.
            // The value is handled by checking if a unique field corresponding to any specific implementation of StorageMetadata exists.
            // The correct instance of StorageMetadata is then generated.
            TransactionData transactionData = TransactionData.builder()
                    .key(getAndRemoveIfExists(data, "key"))
                    .value(generateStorageMetadataValue(data))
                    .build();
            buf = SERIALIZER.serialize(transactionData).asByteBuffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf;
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        StringBuilder stringValueBuilder;
        try {
            TransactionData data = SERIALIZER.deserialize(new ByteArrayInputStream(serializedValue.array()));
            stringValueBuilder = new StringBuilder();

            appendField(stringValueBuilder, "key", data.getKey());
            handleStorageMetadataValue(stringValueBuilder, data.getValue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stringValueBuilder.toString();
    }

    /**
     * Convert {@link StorageMetadata} into string of fields and values to be appended it into the given StringBuilder.
     *
     * @param builder  The given StringBuilder.
     * @param metadata The StorageMetadata instance.
     */
    private void handleStorageMetadataValue(StringBuilder builder, StorageMetadata metadata) {
        if (metadata instanceof ChunkMetadata) {
            ChunkMetadata chunkMetadata = (ChunkMetadata) metadata;
            CHUNK_METADATA_FIELD_MAP.forEach((name, f) -> appendField(builder, name, String.valueOf(f.apply(chunkMetadata))));

        } else if (metadata instanceof SegmentMetadata) {
            SegmentMetadata segmentMetadata = (SegmentMetadata) metadata;
            SEGMENT_METADATA_FIELD_MAP.forEach((name, f) -> appendField(builder, name, String.valueOf(f.apply(segmentMetadata))));

        } else if (metadata instanceof ReadIndexBlockMetadata) {
            ReadIndexBlockMetadata readIndexBlockMetadata = (ReadIndexBlockMetadata) metadata;
            READ_INDEX_BLOCK_METADATA_FIELD_MAP.forEach((name, f) -> appendField(builder, name, String.valueOf(f.apply(readIndexBlockMetadata))));
        }
    }

    /**
     * Convert the data map into the required {@link StorageMetadata} instance.
     *
     * @param storageMetadataMap The map containing StorageMetadata in String form.
     * @return The required StorageMetadata instance.
     * @throws IllegalArgumentException if any of the queried fields do not correspond to any valid StorageMetadata implementation.
     */
    private StorageMetadata generateStorageMetadataValue(Map<String, String> storageMetadataMap) {
        if (CHUNK_METADATA_FIELD_MAP.keySet().stream().allMatch(storageMetadataMap::containsKey)) {
            return ChunkMetadata.builder()
                    .name(getAndRemoveIfExists(storageMetadataMap, "name"))
                    .length(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "length")))
                    .nextChunk(getAndRemoveIfExists(storageMetadataMap, "nextChunk"))
                    .status(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, "status")))
                    .build();

        } else if (SEGMENT_METADATA_FIELD_MAP.keySet().stream().allMatch(storageMetadataMap::containsKey)) {
            return SegmentMetadata.builder()
                    .name(getAndRemoveIfExists(storageMetadataMap, "name"))
                    .length(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "length")))
                    .chunkCount(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, "chunkCount")))
                    .startOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "startOffset")))
                    .status(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, "status")))
                    .maxRollinglength(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "maxRollingLength")))
                    .firstChunk(getAndRemoveIfExists(storageMetadataMap, "firstChunk"))
                    .lastChunk(getAndRemoveIfExists(storageMetadataMap, "lastChunk"))
                    .lastModified(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "lastModified")))
                    .firstChunkStartOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "firstChunkStartOffset")))
                    .lastChunkStartOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "lastChunkStartOffset")))
                    .ownerEpoch(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "ownerEpoch")))
                    .build();

        } else if (READ_INDEX_BLOCK_METADATA_FIELD_MAP.keySet().stream().allMatch(storageMetadataMap::containsKey)) {
            return ReadIndexBlockMetadata.builder()
                    .name(getAndRemoveIfExists(storageMetadataMap, "name"))
                    .chunkName(getAndRemoveIfExists(storageMetadataMap, "chunkName"))
                    .startOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, "startOffset")))
                    .status(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, "status")))
                    .build();
        }

        StringBuilder chunkGuide = new StringBuilder("key;");
        CHUNK_METADATA_FIELD_MAP.keySet().forEach(s -> chunkGuide.append(s).append(";"));
        StringBuilder segmentGuide = new StringBuilder("key;");
        SEGMENT_METADATA_FIELD_MAP.keySet().forEach(s -> segmentGuide.append(s).append(";"));
        StringBuilder readIndexBlockGuide = new StringBuilder("key;");
        READ_INDEX_BLOCK_METADATA_FIELD_MAP.keySet().forEach(s -> readIndexBlockGuide.append(s).append(";"));
        throw new IllegalArgumentException("Values provided do not correspond to any valid SLTS metadata.\nThe following are valid metadata keys for each type:\n" +
                "ChunkMetadata: " + chunkGuide.toString() + "\n" +
                "SegmentMetadata: " + segmentGuide.toString() + "\n" +
                "ReadIndexBlockMetadata: " + readIndexBlockGuide.toString() + "\n");
    }
}
