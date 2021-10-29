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
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_ID;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_LENGTH;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_NAME;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_SEALED;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_START_OFFSET;
import static org.junit.Assert.assertEquals;

public class ContainerMetadataSerializerTest {

    @Test
    public void testContainerMetadataSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, SEGMENT_ID, "1");
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_NAME, "segment-name");
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_SEALED, "false");
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_START_OFFSET, "0");
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_LENGTH, "10");
        appendField(userGeneratedMetadataBuilder, "80000000-0000-0000-0000-000000000000", "1632728432718");

        String userString = userGeneratedMetadataBuilder.toString();
        ContainerMetadataSerializer serializer = new ContainerMetadataSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testContainerMetadataSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_NAME, "segment-name");
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_SEALED, "false");
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_START_OFFSET, "0");
        appendField(userGeneratedMetadataBuilder, SEGMENT_PROPERTIES_LENGTH, "10");

        String userString = userGeneratedMetadataBuilder.toString();
        ContainerMetadataSerializer serializer = new ContainerMetadataSerializer();
        serializer.serialize(userString);
    }
}
