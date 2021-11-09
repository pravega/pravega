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
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.STREAM_TRUNCATION_RECORD_DELETED_SEGMENTS;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.STREAM_TRUNCATION_RECORD_SIZE_TILL;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.STREAM_TRUNCATION_RECORD_SPAN;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.STREAM_TRUNCATION_RECORD_STREAM_CUT;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.STREAM_TRUNCATION_RECORD_TO_DELETE;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.STREAM_TRUNCATION_RECORD_UPDATING;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.convertCollectionToString;
import static io.pravega.cli.admin.serializers.controller.StreamTruncationRecordSerializer.convertMapToString;
import static io.pravega.cli.admin.utils.TestUtils.generateStreamSegmentRecordString;
import static org.junit.Assert.assertEquals;

public class StreamTruncationRecordSerializerTest {

    @Test
    public void testStreamTruncationRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_STREAM_CUT,
                convertMapToString(ImmutableMap.of(1L, 2L), String::valueOf, String::valueOf));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_SPAN,
                convertMapToString(ImmutableMap.of(
                        generateStreamSegmentRecordString(1, 5, 10L, 2.1, 2.2), 1,
                        generateStreamSegmentRecordString(2, 6, 11L, 3.1, 3.2), 2),
                        s -> s, String::valueOf));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_DELETED_SEGMENTS,
                convertCollectionToString(ImmutableSet.of(1L), String::valueOf));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_TO_DELETE,
                convertCollectionToString(ImmutableSet.of(2L), String::valueOf));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_SIZE_TILL, String.valueOf(10L));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_UPDATING, String.valueOf(false));

        String userString = userGeneratedMetadataBuilder.toString();
        StreamTruncationRecordSerializer serializer = new StreamTruncationRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamTruncationRecordSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_STREAM_CUT,
                convertMapToString(ImmutableMap.of(1L, 2L), String::valueOf, String::valueOf));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_DELETED_SEGMENTS,
                convertCollectionToString(ImmutableSet.of(1L), String::valueOf));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_TO_DELETE,
                convertCollectionToString(ImmutableSet.of(2L), String::valueOf));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_SIZE_TILL, String.valueOf(10L));
        appendField(userGeneratedMetadataBuilder, STREAM_TRUNCATION_RECORD_UPDATING, String.valueOf(false));

        String userString = userGeneratedMetadataBuilder.toString();
        StreamTruncationRecordSerializer serializer = new StreamTruncationRecordSerializer();
        serializer.serialize(userString);
    }
}
