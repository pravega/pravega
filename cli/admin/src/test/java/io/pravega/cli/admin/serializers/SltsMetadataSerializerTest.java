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

import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.CHUNK_METADATA;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.CHUNK_METADATA_LENGTH;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.CHUNK_METADATA_NAME;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.CHUNK_METADATA_NEXT_CHUNK;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.CHUNK_METADATA_STATUS;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.METADATA_TYPE;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.READ_INDEX_BLOCK_METADATA;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.READ_INDEX_BLOCK_METADATA_CHUNK_NAME;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.READ_INDEX_BLOCK_METADATA_NAME;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.READ_INDEX_BLOCK_METADATA_START_OFFSET;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.READ_INDEX_BLOCK_METADATA_STATUS;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_CHUNK_COUNT;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_FIRST_CHUNK;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_FIRST_CHUNK_START_OFFSET;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_LAST_CHUNK;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_LAST_CHUNK_START_OFFSET;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_LAST_MODIFIED;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_LENGTH;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_MAX_ROLLING_LENGTH;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_NAME;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_OWNER_EPOCH;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_START_OFFSET;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.SEGMENT_METADATA_STATUS;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.TRANSACTION_DATA_KEY;
import static io.pravega.cli.admin.serializers.SltsMetadataSerializer.TRANSACTION_DATA_VERSION;
import static org.junit.Assert.assertEquals;

public class SltsMetadataSerializerTest {

    @Test
    public void testSltsChunkMetadataSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_KEY, "k");
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_VERSION, "1062");
        appendField(userGeneratedMetadataBuilder, METADATA_TYPE, CHUNK_METADATA);
        appendField(userGeneratedMetadataBuilder, CHUNK_METADATA_NAME, "chunk0");
        appendField(userGeneratedMetadataBuilder, CHUNK_METADATA_LENGTH, "10");
        appendField(userGeneratedMetadataBuilder, CHUNK_METADATA_NEXT_CHUNK, "chunk1");
        appendField(userGeneratedMetadataBuilder, CHUNK_METADATA_STATUS, "0");

        String userString = userGeneratedMetadataBuilder.toString();
        SltsMetadataSerializer serializer = new SltsMetadataSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test
    public void testSltsSegmentMetadataSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_KEY, "k");
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_VERSION, "1062");
        appendField(userGeneratedMetadataBuilder, METADATA_TYPE, SEGMENT_METADATA);
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_NAME, "segment-name");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_LENGTH, "10");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_CHUNK_COUNT, "5");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_START_OFFSET, "0");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_STATUS, "1");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_MAX_ROLLING_LENGTH, "10");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_FIRST_CHUNK, "chunk0");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_LAST_CHUNK, "chunk4");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_LAST_MODIFIED, "1000");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_FIRST_CHUNK_START_OFFSET, "10");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_LAST_CHUNK_START_OFFSET, "50");
        appendField(userGeneratedMetadataBuilder, SEGMENT_METADATA_OWNER_EPOCH, "12345");

        String userString = userGeneratedMetadataBuilder.toString();
        SltsMetadataSerializer serializer = new SltsMetadataSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test
    public void testSltsReadIndexBlockMetadataSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_KEY, "k");
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_VERSION, "1062");
        appendField(userGeneratedMetadataBuilder, METADATA_TYPE, READ_INDEX_BLOCK_METADATA);
        appendField(userGeneratedMetadataBuilder, READ_INDEX_BLOCK_METADATA_NAME, "r1");
        appendField(userGeneratedMetadataBuilder, READ_INDEX_BLOCK_METADATA_CHUNK_NAME, "chunk0");
        appendField(userGeneratedMetadataBuilder, READ_INDEX_BLOCK_METADATA_START_OFFSET, "10");
        appendField(userGeneratedMetadataBuilder, READ_INDEX_BLOCK_METADATA_STATUS, "0");

        String userString = userGeneratedMetadataBuilder.toString();
        SltsMetadataSerializer serializer = new SltsMetadataSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSltsSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_KEY, "k");
        appendField(userGeneratedMetadataBuilder, TRANSACTION_DATA_VERSION, "1062");
        appendField(userGeneratedMetadataBuilder, METADATA_TYPE, "random_value");
        appendField(userGeneratedMetadataBuilder, CHUNK_METADATA_NAME, "chunk0");
        appendField(userGeneratedMetadataBuilder, CHUNK_METADATA_LENGTH, "10");
        appendField(userGeneratedMetadataBuilder, CHUNK_METADATA_STATUS, "0");

        String userString = userGeneratedMetadataBuilder.toString();
        SltsMetadataSerializer serializer = new SltsMetadataSerializer();
        serializer.serialize(userString);
    }
}
