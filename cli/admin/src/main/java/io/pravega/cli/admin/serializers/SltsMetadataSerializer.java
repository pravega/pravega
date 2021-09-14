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

import static io.pravega.cli.admin.utils.SerializerUtils.addField;

public class SltsMetadataSerializer implements Serializer<String> {
    private static final TransactionData.TransactionDataSerializer SERIALIZER = new TransactionData.TransactionDataSerializer();

    private static final Map<String, Function<TransactionData, Object>> TRANSACTION_DATA_FIELD_MAP =
            ImmutableMap.<String, Function<TransactionData, Object>>builder()
                    .put("key", TransactionData::getKey)
                    .put("created", TransactionData::isCreated)
                    .put("deleted", TransactionData::isDeleted)
                    .put("persisted", TransactionData::isPersisted)
                    .put("pinned", TransactionData::isPinned)
                    .build();

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

    private static final String STORAGE_METADATA_TYPE = "storage-metadata-type";

    @Override
    public ByteBuffer serialize(String value) {
        return null;
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        StringBuilder stringValueBuilder;
        try {
            TransactionData data = SERIALIZER.deserialize(new ByteArrayInputStream(serializedValue.array()));
            stringValueBuilder = new StringBuilder("SLTS metadata info:\n");

            TRANSACTION_DATA_FIELD_MAP.forEach((name, f) -> addField(stringValueBuilder, name, String.valueOf(f.apply(data))));
            handleStorageMetadataValue(stringValueBuilder, data.getValue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stringValueBuilder.toString();
    }

    private void handleStorageMetadataValue(StringBuilder builder, StorageMetadata metadata) {
        if (metadata instanceof ChunkMetadata) {
            addField(builder, STORAGE_METADATA_TYPE, "ChunkMetadata");
            ChunkMetadata chunkMetadata = (ChunkMetadata) metadata;
            CHUNK_METADATA_FIELD_MAP.forEach((name, f) -> addField(builder, name, String.valueOf(f.apply(chunkMetadata))));

        } else if (metadata instanceof SegmentMetadata) {
            addField(builder, STORAGE_METADATA_TYPE, "SegmentMetadata");
            SegmentMetadata segmentMetadata = (SegmentMetadata) metadata;
            SEGMENT_METADATA_FIELD_MAP.forEach((name, f) -> addField(builder, name, String.valueOf(f.apply(segmentMetadata))));

        } else if (metadata instanceof ReadIndexBlockMetadata) {
            addField(builder, STORAGE_METADATA_TYPE, "ReadIndexBlockMetadata");
            ReadIndexBlockMetadata readIndexBlockMetadata = (ReadIndexBlockMetadata) metadata;
            READ_INDEX_BLOCK_METADATA_FIELD_MAP.forEach((name, f) -> addField(builder, name, String.valueOf(f.apply(readIndexBlockMetadata))));
        }
    }
}
