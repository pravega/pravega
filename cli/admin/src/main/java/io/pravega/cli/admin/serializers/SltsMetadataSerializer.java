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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ReadIndexBlockMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

/**
 * An implementation of {@link Serializer} that converts a user-friendly string representing SLTS metadata.
 */
public class SltsMetadataSerializer extends AbstractSerializer {
    
    static final String TRANSACTION_DATA_KEY = "key";
    static final String TRANSACTION_DATA_VERSION = "version";

    static final String CHUNK_METADATA_NAME = "name";
    static final String CHUNK_METADATA_LENGTH = "length";
    static final String CHUNK_METADATA_NEXT_CHUNK = "nextChunk";
    static final String CHUNK_METADATA_STATUS = "status";

    static final String SEGMENT_METADATA_NAME = "name";
    static final String SEGMENT_METADATA_LENGTH = "length";
    static final String SEGMENT_METADATA_CHUNK_COUNT = "chunkCount";
    static final String SEGMENT_METADATA_START_OFFSET = "startOffset";
    static final String SEGMENT_METADATA_STATUS = "status";
    static final String SEGMENT_METADATA_MAX_ROLLING_LENGTH = "maxRollingLength";
    static final String SEGMENT_METADATA_FIRST_CHUNK = "firstChunk";
    static final String SEGMENT_METADATA_LAST_CHUNK = "lastChunk";
    static final String SEGMENT_METADATA_LAST_MODIFIED = "lastModified";
    static final String SEGMENT_METADATA_FIRST_CHUNK_START_OFFSET = "firstChunkStartOffset";
    static final String SEGMENT_METADATA_LAST_CHUNK_START_OFFSET = "lastChunkStartOffset";
    static final String SEGMENT_METADATA_OWNER_EPOCH = "ownerEpoch";

    static final String READ_INDEX_BLOCK_METADATA_NAME = "name";
    static final String READ_INDEX_BLOCK_METADATA_CHUNK_NAME = "chunkName";
    static final String READ_INDEX_BLOCK_METADATA_START_OFFSET = "startOffset";
    static final String READ_INDEX_BLOCK_METADATA_STATUS = "status";

    static final String METADATA_TYPE = "metadataType";
    static final String CHUNK_METADATA = "ChunkMetadata";
    static final String SEGMENT_METADATA = "SegmentMetadata";
    static final String READ_INDEX_BLOCK_METADATA = "ReadIndexBlockMetadata";

    private static final TransactionData.TransactionDataSerializer SERIALIZER = new TransactionData.TransactionDataSerializer();

    private static final Map<String, Function<ChunkMetadata, Object>> CHUNK_METADATA_FIELD_MAP =
            ImmutableMap.<String, Function<ChunkMetadata, Object>>builder()
                    .put(CHUNK_METADATA_NAME, ChunkMetadata::getKey)
                    .put(CHUNK_METADATA_LENGTH, ChunkMetadata::getLength)
                    .put(CHUNK_METADATA_NEXT_CHUNK, ChunkMetadata::getNextChunk)
                    .put(CHUNK_METADATA_STATUS, ChunkMetadata::getStatus)
                    .build();

    private static final Map<String, Function<SegmentMetadata, Object>> SEGMENT_METADATA_FIELD_MAP =
            ImmutableMap.<String, Function<SegmentMetadata, Object>>builder()
                    .put(SEGMENT_METADATA_NAME, SegmentMetadata::getKey)
                    .put(SEGMENT_METADATA_LENGTH, SegmentMetadata::getLength)
                    .put(SEGMENT_METADATA_CHUNK_COUNT, SegmentMetadata::getChunkCount)
                    .put(SEGMENT_METADATA_START_OFFSET, SegmentMetadata::getStartOffset)
                    .put(SEGMENT_METADATA_STATUS, SegmentMetadata::getStatus)
                    .put(SEGMENT_METADATA_MAX_ROLLING_LENGTH, SegmentMetadata::getMaxRollinglength)
                    .put(SEGMENT_METADATA_FIRST_CHUNK, SegmentMetadata::getFirstChunk)
                    .put(SEGMENT_METADATA_LAST_CHUNK, SegmentMetadata::getLastChunk)
                    .put(SEGMENT_METADATA_LAST_MODIFIED, SegmentMetadata::getLastModified)
                    .put(SEGMENT_METADATA_FIRST_CHUNK_START_OFFSET, SegmentMetadata::getFirstChunkStartOffset)
                    .put(SEGMENT_METADATA_LAST_CHUNK_START_OFFSET, SegmentMetadata::getLastChunkStartOffset)
                    .put(SEGMENT_METADATA_OWNER_EPOCH, SegmentMetadata::getOwnerEpoch)
                    .build();

    private static final Map<String, Function<ReadIndexBlockMetadata, Object>> READ_INDEX_BLOCK_METADATA_FIELD_MAP =
            ImmutableMap.<String, Function<ReadIndexBlockMetadata, Object>>builder()
                    .put(READ_INDEX_BLOCK_METADATA_NAME, ReadIndexBlockMetadata::getKey)
                    .put(READ_INDEX_BLOCK_METADATA_CHUNK_NAME, ReadIndexBlockMetadata::getChunkName)
                    .put(READ_INDEX_BLOCK_METADATA_START_OFFSET, ReadIndexBlockMetadata::getStartOffset)
                    .put(READ_INDEX_BLOCK_METADATA_STATUS, ReadIndexBlockMetadata::getStatus)
                    .build();

    @Override
    public String getName() {
        return "SLTS";
    }

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
                    .key(getAndRemoveIfExists(data, TRANSACTION_DATA_KEY))
                    .version(Long.parseLong(getAndRemoveIfExists(data, TRANSACTION_DATA_VERSION)))
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
            TransactionData data = SERIALIZER.deserialize(new ByteArraySegment(serializedValue).getReader());
            stringValueBuilder = new StringBuilder();

            appendField(stringValueBuilder, TRANSACTION_DATA_KEY, data.getKey());
            appendField(stringValueBuilder, TRANSACTION_DATA_VERSION, String.valueOf(data.getVersion()));
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
            appendField(builder, METADATA_TYPE, CHUNK_METADATA);
            ChunkMetadata chunkMetadata = (ChunkMetadata) metadata;
            CHUNK_METADATA_FIELD_MAP.forEach((name, f) -> appendField(builder, name, String.valueOf(f.apply(chunkMetadata))));

        } else if (metadata instanceof SegmentMetadata) {
            appendField(builder, METADATA_TYPE, SEGMENT_METADATA);
            SegmentMetadata segmentMetadata = (SegmentMetadata) metadata;
            SEGMENT_METADATA_FIELD_MAP.forEach((name, f) -> appendField(builder, name, String.valueOf(f.apply(segmentMetadata))));

        } else if (metadata instanceof ReadIndexBlockMetadata) {
            appendField(builder, METADATA_TYPE, READ_INDEX_BLOCK_METADATA);
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
        String metadataType = getAndRemoveIfExists(storageMetadataMap, METADATA_TYPE);
        switch (metadataType) {
            case CHUNK_METADATA:
                return ChunkMetadata.builder()
                        .name(getAndRemoveIfExists(storageMetadataMap, CHUNK_METADATA_NAME))
                        .length(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, CHUNK_METADATA_LENGTH)))
                        .nextChunk(getAndRemoveIfExists(storageMetadataMap, CHUNK_METADATA_NEXT_CHUNK))
                        .status(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, CHUNK_METADATA_STATUS)))
                        .build();

            case SEGMENT_METADATA:
                return SegmentMetadata.builder()
                        .name(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_NAME))
                        .length(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_LENGTH)))
                        .chunkCount(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_CHUNK_COUNT)))
                        .startOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_START_OFFSET)))
                        .status(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_STATUS)))
                        .maxRollinglength(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_MAX_ROLLING_LENGTH)))
                        .firstChunk(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_FIRST_CHUNK))
                        .lastChunk(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_LAST_CHUNK))
                        .lastModified(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_LAST_MODIFIED)))
                        .firstChunkStartOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_FIRST_CHUNK_START_OFFSET)))
                        .lastChunkStartOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_LAST_CHUNK_START_OFFSET)))
                        .ownerEpoch(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, SEGMENT_METADATA_OWNER_EPOCH)))
                        .build();

            case READ_INDEX_BLOCK_METADATA:
                return ReadIndexBlockMetadata.builder()
                        .name(getAndRemoveIfExists(storageMetadataMap, READ_INDEX_BLOCK_METADATA_NAME))
                        .chunkName(getAndRemoveIfExists(storageMetadataMap, READ_INDEX_BLOCK_METADATA_CHUNK_NAME))
                        .startOffset(Long.parseLong(getAndRemoveIfExists(storageMetadataMap, READ_INDEX_BLOCK_METADATA_START_OFFSET)))
                        .status(Integer.parseInt(getAndRemoveIfExists(storageMetadataMap, READ_INDEX_BLOCK_METADATA_STATUS)))
                        .build();

            default:
                throw new IllegalArgumentException("The metadataType value provided does not correspond to any valid SLTS metadata. " +
                        "The following are valid values: " + CHUNK_METADATA + ", " + SEGMENT_METADATA + ", " + READ_INDEX_BLOCK_METADATA);
        }
    }
}
