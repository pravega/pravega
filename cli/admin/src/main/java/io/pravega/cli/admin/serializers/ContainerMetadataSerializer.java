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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.containers.MetadataStore.SegmentInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * An implementation of {@link Serializer} that converts a user-friendly string representing container metadata.
 */
public class ContainerMetadataSerializer extends AbstractSerializer {

    public static final String SEGMENT_ID = "segmentId";
    public static final String SEGMENT_PROPERTIES_NAME = "name";
    public static final String SEGMENT_PROPERTIES_SEALED = "sealed";
    public static final String SEGMENT_PROPERTIES_START_OFFSET = "startOffset";
    public static final String SEGMENT_PROPERTIES_LENGTH = "length";

    private static final Map<String, Function<SegmentProperties, Object>> SEGMENT_PROPERTIES_FIELD_MAP =
            ImmutableMap.<String, Function<SegmentProperties, Object>>builder()
                    .put(SEGMENT_PROPERTIES_NAME, SegmentProperties::getName)
                    .put(SEGMENT_PROPERTIES_SEALED, SegmentProperties::isSealed)
                    .put(SEGMENT_PROPERTIES_START_OFFSET, SegmentProperties::getStartOffset)
                    .put(SEGMENT_PROPERTIES_LENGTH, SegmentProperties::getLength)
                    .build();
    
    private static final SegmentInfo.SegmentInfoSerializer SERIALIZER = new SegmentInfo.SegmentInfoSerializer();

    @Override
    public String getName() {
        return "container";
    }

    @Override
    public ByteBuffer serialize(String value) {
        ByteBuffer buf;
        try {
            // Convert string to map with fields and values.
            Map<String, String> data = parseStringData(value);
            long segmentId = Long.parseLong(getAndRemoveIfExists(data, SEGMENT_ID));
            // Use the map to build SegmentProperties. The fields/keys are removed after being queried to ensure attributes
            // can be handled without interference. If the field/key queried does not exist we throw an IllegalArgumentException.
            StreamSegmentInformation properties = StreamSegmentInformation.builder()
                    .name(getAndRemoveIfExists(data, SEGMENT_PROPERTIES_NAME))
                    .sealed(Boolean.parseBoolean(getAndRemoveIfExists(data, SEGMENT_PROPERTIES_SEALED)))
                    .startOffset(Long.parseLong(getAndRemoveIfExists(data, SEGMENT_PROPERTIES_START_OFFSET)))
                    .length(Long.parseLong(getAndRemoveIfExists(data, SEGMENT_PROPERTIES_LENGTH)))
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
            SegmentInfo data = SERIALIZER.deserialize(new ByteArraySegment(serializedValue).getReader());
            stringValueBuilder = new StringBuilder();

            appendField(stringValueBuilder, SEGMENT_ID, String.valueOf(data.getSegmentId()));
            SegmentProperties sp = data.getProperties();
            SEGMENT_PROPERTIES_FIELD_MAP.forEach((name, f) -> appendField(stringValueBuilder, name, String.valueOf(f.apply(sp))));

            sp.getAttributes().forEach((attributeId, attributeValue) -> appendField(stringValueBuilder, attributeId.toString(), attributeValue.toString()));
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
