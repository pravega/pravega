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
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.controller.StreamCutRecordSerializer.STREAM_CUT_RECORD_RECORDING_SIZE;
import static io.pravega.cli.admin.serializers.controller.StreamCutRecordSerializer.STREAM_CUT_RECORD_RECORDING_TIME;
import static io.pravega.cli.admin.serializers.controller.StreamCutRecordSerializer.STREAM_CUT_RECORD_STREAM_CUT;
import static io.pravega.cli.admin.serializers.controller.StreamCutRecordSerializer.convertMapToString;
import static org.junit.Assert.assertEquals;

public class StreamCutRecordSerializerTest {

    @Test
    public void testStreamSubscriberSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STREAM_CUT_RECORD_RECORDING_TIME, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, STREAM_CUT_RECORD_RECORDING_SIZE, String.valueOf(10L));
        appendField(userGeneratedMetadataBuilder, STREAM_CUT_RECORD_STREAM_CUT,
                convertMapToString(ImmutableMap.of(1L, 2L), String::valueOf, String::valueOf));

        String userString = userGeneratedMetadataBuilder.toString();
        StreamCutRecordSerializer serializer = new StreamCutRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamSubscriberSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STREAM_CUT_RECORD_RECORDING_TIME, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, STREAM_CUT_RECORD_STREAM_CUT,
                convertMapToString(ImmutableMap.of(1L, 2L), String::valueOf, String::valueOf));

        String userString = userGeneratedMetadataBuilder.toString();
        StreamCutRecordSerializer serializer = new StreamCutRecordSerializer();
        serializer.serialize(userString);
    }
}
