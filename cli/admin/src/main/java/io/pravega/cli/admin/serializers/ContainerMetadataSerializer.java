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
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.containers.MetadataStore.SegmentInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

import static io.pravega.cli.admin.utils.SerializerUtils.addField;

public class ContainerMetadataSerializer implements Serializer<String> {
    private static final SegmentInfo.SegmentInfoSerializer SERIALIZER = new SegmentInfo.SegmentInfoSerializer();

    private static final Map<String, Function<SegmentProperties, Object>> SEGMENT_PROPERTIES_FIELD_MAP =
            ImmutableMap.<String, Function<SegmentInfo, Object>>builder()
                    .put("", SegmentInfo::getSegmentId)
                    .build();

    @Override
    public ByteBuffer serialize(String value) {
        return null;
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        StringBuilder stringValueBuilder;
        try {
            SegmentInfo data = SERIALIZER.deserialize(new ByteArrayInputStream(serializedValue.array()));
            stringValueBuilder = new StringBuilder("Container metadata info:\n");

            addField(stringValueBuilder, "segmentId", String.valueOf(data.getSegmentId()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stringValueBuilder.toString();
    }
}
