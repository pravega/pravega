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
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.containers.MetadataStore.SegmentInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static io.pravega.cli.admin.utils.SerializerUtils.addField;
import static io.pravega.cli.admin.utils.SerializerUtils.getAndRemoveIfExists;
import static io.pravega.cli.admin.utils.SerializerUtils.parseStringData;

public class ContainerMetadataSerializer implements Serializer<String> {
    private static final SegmentInfo.SegmentInfoSerializer SERIALIZER = new SegmentInfo.SegmentInfoSerializer();

    private static final Map<String, Function<SegmentProperties, Object>> SEGMENT_PROPERTIES_FIELD_MAP =
            ImmutableMap.<String, Function<SegmentProperties, Object>>builder()
                    .put("name", SegmentProperties::getName)
                    .put("sealed", SegmentProperties::isSealed)
                    .put("deleted", SegmentProperties::isDeleted)
                    .put("lastModified", SegmentProperties::getLastModified)
                    .put("startOffset", SegmentProperties::getStartOffset)
                    .put("length", SegmentProperties::getLength)
                    .build();

    @Override
    public ByteBuffer serialize(String value) {
        ByteBuffer buf;
        try {
            Map<String, String> data = parseStringData(value);
            long segmentId = Long.parseLong(getAndRemoveIfExists(data, "segmentId"));
            StreamSegmentInformation.StreamSegmentInformationBuilder infoBuilder = StreamSegmentInformation.builder();
            populateSegmentInfo(infoBuilder, data);

            SegmentInfo segment = SegmentInfo.builder()
                    .segmentId(segmentId)
                    .properties(infoBuilder.build())
                    .build();
            buf = SERIALIZER.serialize(segment).asByteBuffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf;
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        StringBuilder stringValueBuilder;
        try {
            SegmentInfo data = SERIALIZER.deserialize(new ByteArrayInputStream(serializedValue.array()));
            stringValueBuilder = new StringBuilder("Container metadata info:\n");

            addField(stringValueBuilder, "segmentId", String.valueOf(data.getSegmentId()));
            SegmentProperties sp = data.getProperties();
            SEGMENT_PROPERTIES_FIELD_MAP.forEach((name, f) -> addField(stringValueBuilder, name, String.valueOf(f.apply(sp))));

            stringValueBuilder.append("Segment Attributes: ").append("\n");
            sp.getAttributes().forEach(((attributeId, attributeValue) -> addField(stringValueBuilder, attributeId.toString(), attributeValue.toString())));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stringValueBuilder.toString();
    }
    
    private void populateSegmentInfo(StreamSegmentInformation.StreamSegmentInformationBuilder infoBuilder, Map<String, String> data) {
        infoBuilder
                .name(getAndRemoveIfExists(data, "name"))
                .sealed(Boolean.parseBoolean(getAndRemoveIfExists(data, "sealed")))
                .deleted(Boolean.parseBoolean(getAndRemoveIfExists(data, "deleted")))
                .lastModified(new ImmutableDate(Long.parseLong(getAndRemoveIfExists(data, "lastModified"))))
                .startOffset(Long.parseLong(getAndRemoveIfExists(data, "startOffset")))
                .length(Long.parseLong(getAndRemoveIfExists(data, "length")));
        
        Map<AttributeId, Long> attributes = populateAttributes(data);
        infoBuilder.attributes(attributes);
    }

    private Map<AttributeId, Long> populateAttributes(Map<String, String> data) {
        Map<AttributeId, Long> attributes = new HashMap<>();
        data.forEach((k, v) -> attributes.put(AttributeId.fromUUID(UUID.fromString(k)), Long.parseLong(v)));
        return attributes;
    }
}
