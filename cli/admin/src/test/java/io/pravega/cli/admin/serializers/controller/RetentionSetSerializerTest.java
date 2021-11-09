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
import static io.pravega.cli.admin.serializers.AbstractSerializer.appendFieldWithCustomDelimiters;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertCollectionToString;
import static io.pravega.cli.admin.serializers.controller.RetentionSetSerializer.RETENTION_SET_RETENTION_RECORDS;
import static io.pravega.cli.admin.serializers.controller.RetentionSetSerializer.STREAM_CUT_REFERENCE_RECORD_PAIR_DELIMITER;
import static io.pravega.cli.admin.serializers.controller.RetentionSetSerializer.STREAM_CUT_REFERENCE_RECORD_RECORDING_SIZE;
import static io.pravega.cli.admin.serializers.controller.RetentionSetSerializer.STREAM_CUT_REFERENCE_RECORD_RECORDING_TIME;
import static io.pravega.cli.admin.serializers.controller.RetentionSetSerializer.STREAM_CUT_REFERENCE_RECORD_VALUE_DELIMITER;
import static org.junit.Assert.assertEquals;

public class RetentionSetSerializerTest {

    @Test
    public void testRetentionSetSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, RETENTION_SET_RETENTION_RECORDS,
                convertCollectionToString(ImmutableList.of(
                        generateStreamCutReferenceRecordString(100L, 20L),
                        generateStreamCutReferenceRecordString(101L, 21L)), s -> s));

        String userString = userGeneratedMetadataBuilder.toString();
        RetentionSetSerializer serializer = new RetentionSetSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRetentionSetSerializerArgumentFailure() {
        String userString = "";
        RetentionSetSerializer serializer = new RetentionSetSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    private String generateStreamCutReferenceRecordString(long rt, long rz) {
        StringBuilder builder = new StringBuilder();
        appendFieldWithCustomDelimiters(builder, STREAM_CUT_REFERENCE_RECORD_RECORDING_TIME, String.valueOf(rt),
                STREAM_CUT_REFERENCE_RECORD_PAIR_DELIMITER, STREAM_CUT_REFERENCE_RECORD_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(builder, STREAM_CUT_REFERENCE_RECORD_RECORDING_SIZE, String.valueOf(rz),
                STREAM_CUT_REFERENCE_RECORD_PAIR_DELIMITER, STREAM_CUT_REFERENCE_RECORD_VALUE_DELIMITER);
        return builder.toString();
    }
}
