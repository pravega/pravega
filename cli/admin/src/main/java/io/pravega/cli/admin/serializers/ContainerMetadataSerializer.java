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

/**
 * An implementation of {@link Serializer} that converts a user-friendly string representing container metadata.
 */
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
            // Convert string to map with fields and values.
            Map<String, String> data = parseStringData(value);
            long segmentId = Long.parseLong(getAndRemoveIfExists(data, "segmentId"));
            // Use the map to build SegmentProperties. The fields/keys are removed after being queried to ensure attributes
            // can be handled without interference. If the field/key queried does not exist we throw an IllegalArgumentException.
            StreamSegmentInformation properties = StreamSegmentInformation.builder()
                    .name(getAndRemoveIfExists(data, "name"))
                    .sealed(Boolean.parseBoolean(getAndRemoveIfExists(data, "sealed")))
                    .deleted(Boolean.parseBoolean(getAndRemoveIfExists(data, "deleted")))
                    .lastModified(new ImmutableDate(Long.parseLong(getAndRemoveIfExists(data, "lastModified"))))
                    .startOffset(Long.parseLong(getAndRemoveIfExists(data, "startOffset")))
                    .length(Long.parseLong(getAndRemoveIfExists(data, "length")))
                    .attributes(getAttributes(data))
                    .build();

            SegmentInfo segment = SegmentInfo.builder()
                    .segmentId(segmentId)
                    .properties(properties)
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

    /**
     * Reads the remaining data map for attribute Ids and their values.
     * Note: The data map should only contain attribute information.
     *
     * @param segmentMap The map containing segment attributes in String form.
     * @return A map of segment attributes as attributeId-value pairs.
     */
    private Map<AttributeId, Long> getAttributes(Map<String, String> segmentMap) {
        Map<AttributeId, Long> attributes = new HashMap<>();
        segmentMap.forEach((k, v) -> attributes.put(AttributeId.fromUUID(UUID.fromString(k)), Long.parseLong(v)));
        return attributes;
    }
}
