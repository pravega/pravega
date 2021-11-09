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

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertCollectionToString;
import static io.pravega.cli.admin.serializers.controller.EpochRecordSerializer.EPOCH_RECORD_CREATION_TIME;
import static io.pravega.cli.admin.serializers.controller.EpochRecordSerializer.EPOCH_RECORD_EPOCH;
import static io.pravega.cli.admin.serializers.controller.EpochRecordSerializer.EPOCH_RECORD_MERGES;
import static io.pravega.cli.admin.serializers.controller.EpochRecordSerializer.EPOCH_RECORD_REFERENCE_EPOCH;
import static io.pravega.cli.admin.serializers.controller.EpochRecordSerializer.EPOCH_RECORD_SEGMENTS;
import static io.pravega.cli.admin.serializers.controller.EpochRecordSerializer.EPOCH_RECORD_SPLITS;
import static io.pravega.cli.admin.utils.TestUtils.generateStreamSegmentRecordString;
import static org.junit.Assert.assertEquals;

public class EpochRecordSerializerTest {

    @Test
    public void testEpochRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_EPOCH, String.valueOf(10));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_REFERENCE_EPOCH, String.valueOf(5));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_CREATION_TIME, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_SPLITS, String.valueOf(10L));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_MERGES, String.valueOf(20L));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_SEGMENTS,
                convertCollectionToString(ImmutableList.of(
                        generateStreamSegmentRecordString(1, 5, 10L, 2.1, 2.2),
                        generateStreamSegmentRecordString(2, 7, 15L, 3.1, 3.2)),
                        s -> s));

        String userString = userGeneratedMetadataBuilder.toString();
        EpochRecordSerializer serializer = new EpochRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEpochRecordSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_EPOCH, String.valueOf(10));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_REFERENCE_EPOCH, String.valueOf(5));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_SPLITS, String.valueOf(10L));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_MERGES, String.valueOf(20L));
        appendField(userGeneratedMetadataBuilder, EPOCH_RECORD_SEGMENTS,
                convertCollectionToString(ImmutableList.of(
                        generateStreamSegmentRecordString(1, 5, 10L, 2.1, 2.2),
                        generateStreamSegmentRecordString(2, 7, 15L, 3.1, 3.2)),
                        s -> s));

        String userString = userGeneratedMetadataBuilder.toString();
        EpochRecordSerializer serializer = new EpochRecordSerializer();
        serializer.serialize(userString);
    }
}
