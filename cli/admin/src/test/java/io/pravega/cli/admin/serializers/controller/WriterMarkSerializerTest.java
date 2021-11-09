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
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertMapToString;
import static io.pravega.cli.admin.serializers.controller.WriterMarkSerializer.WRITER_MARK_IS_ALIVE;
import static io.pravega.cli.admin.serializers.controller.WriterMarkSerializer.WRITER_MARK_POSITION;
import static io.pravega.cli.admin.serializers.controller.WriterMarkSerializer.WRITER_MARK_TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class WriterMarkSerializerTest {

    @Test
    public void testWriterMarkSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, WRITER_MARK_TIMESTAMP, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, WRITER_MARK_POSITION,
                convertMapToString(ImmutableMap.of(1L, 2L), String::valueOf, String::valueOf));
        appendField(userGeneratedMetadataBuilder, WRITER_MARK_IS_ALIVE, String.valueOf(true));

        String userString = userGeneratedMetadataBuilder.toString();
        WriterMarkSerializer serializer = new WriterMarkSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriterMarkSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, WRITER_MARK_TIMESTAMP, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, WRITER_MARK_POSITION,
                convertMapToString(ImmutableMap.of(1L, 2L), String::valueOf, String::valueOf));

        String userString = userGeneratedMetadataBuilder.toString();
        WriterMarkSerializer serializer = new WriterMarkSerializer();
        serializer.serialize(userString);
    }
}
