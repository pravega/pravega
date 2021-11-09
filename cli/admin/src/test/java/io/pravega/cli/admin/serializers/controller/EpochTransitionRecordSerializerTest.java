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
import java.util.Map;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertCollectionToString;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertMapToString;
import static io.pravega.cli.admin.serializers.controller.EpochTransitionRecordSerializer.EPOCH_TRANSITION_RECORD_ACTIVE_EPOCH;
import static io.pravega.cli.admin.serializers.controller.EpochTransitionRecordSerializer.EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE;
import static io.pravega.cli.admin.serializers.controller.EpochTransitionRecordSerializer.EPOCH_TRANSITION_RECORD_SEGMENTS_TO_SEAL;
import static io.pravega.cli.admin.serializers.controller.EpochTransitionRecordSerializer.EPOCH_TRANSITION_RECORD_TIME;
import static org.junit.Assert.assertEquals;

public class EpochTransitionRecordSerializerTest {

    @Test
    public void testEpochTransitionRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, EPOCH_TRANSITION_RECORD_ACTIVE_EPOCH, String.valueOf(1));
        appendField(userGeneratedMetadataBuilder, EPOCH_TRANSITION_RECORD_TIME, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, EPOCH_TRANSITION_RECORD_SEGMENTS_TO_SEAL,
                convertCollectionToString(ImmutableSet.of(1L, 2L), String::valueOf));
        appendField(userGeneratedMetadataBuilder, EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE,
                convertMapToString(ImmutableMap.of(3L, Map.entry(0.0, 0.2), 4L, Map.entry(0.3, 0.9)),
                        String::valueOf, v -> v.getKey() + "-" + v.getValue()));

        String userString = userGeneratedMetadataBuilder.toString();
        EpochTransitionRecordSerializer serializer = new EpochTransitionRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEpochTransitionRecordSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, EPOCH_TRANSITION_RECORD_ACTIVE_EPOCH, String.valueOf(1));
        appendField(userGeneratedMetadataBuilder, EPOCH_TRANSITION_RECORD_TIME, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE,
                convertMapToString(ImmutableMap.of(3L, Map.entry(0.0, 0.2), 4L, Map.entry(0.3, 0.9)),
                        String::valueOf, v -> v.getKey() + "-" + v.getValue()));

        String userString = userGeneratedMetadataBuilder.toString();
        EpochTransitionRecordSerializer serializer = new EpochTransitionRecordSerializer();
        serializer.serialize(userString);
    }
}