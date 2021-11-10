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

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.POLICY_PAIR_DELIMITER;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.POLICY_VALUE_DELIMITER;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.RETENTION_POLICY;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.RETENTION_POLICY_RETENTION_MAX;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.RETENTION_POLICY_RETENTION_PARAM;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.RETENTION_POLICY_RETENTION_TYPE;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.SCALING_POLICY;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.SCALING_POLICY_MIN_NUM_SEGMENTS;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.SCALING_POLICY_SCALE_FACTOR;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.SCALING_POLICY_SCALE_TYPE;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.SCALING_POLICY_TARGET_RATE;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_RECORD_REMOVE_TAGS;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_RECORD_SCOPE;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_RECORD_STREAM_NAME;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_RECORD_TAG_ONLY_UPDATE;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_RECORD_UPDATING;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_ROLLOVER_SIZE_BYTES;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_TAGS;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.STREAM_CONFIGURATION_TIMESTAMP_AGGREGATION_TIMEOUT;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.appendFieldWithCustomDelimiters;
import static io.pravega.cli.admin.serializers.controller.StreamConfigurationRecordSerializer.convertCollectionToString;
import static org.junit.Assert.assertEquals;

public class StreamConfigurationRecordSerializerTest {

    @Test
    public void testStreamConfigurationRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_SCOPE, "testScope");
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_STREAM_NAME, "testStream");
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_UPDATING, String.valueOf(false));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_TAG_ONLY_UPDATE, String.valueOf(false));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_REMOVE_TAGS, convertCollectionToString(ImmutableSet.of("test"), s -> s));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_TAGS, convertCollectionToString(ImmutableSet.of("test"), s -> s));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_ROLLOVER_SIZE_BYTES, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_TIMESTAMP_AGGREGATION_TIMEOUT, String.valueOf(10L));

        appendField(userGeneratedMetadataBuilder, SCALING_POLICY, generateScalingPolicyString("FIXED_NUM_SEGMENTS", 1, 2, 5));
        appendField(userGeneratedMetadataBuilder, RETENTION_POLICY, generateRetentionPolicyString("TIME", 1L, 2L));

        String userString = userGeneratedMetadataBuilder.toString();
        StreamConfigurationRecordSerializer serializer = new StreamConfigurationRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test
    public void testStreamConfigurationRecordSerializerWithNoPolicy() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_SCOPE, "testScope");
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_STREAM_NAME, "testStream");
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_UPDATING, String.valueOf(false));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_TAG_ONLY_UPDATE, String.valueOf(false));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_REMOVE_TAGS, convertCollectionToString(ImmutableSet.of("test"), s -> s));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_TAGS, convertCollectionToString(ImmutableSet.of("test"), s -> s));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_ROLLOVER_SIZE_BYTES, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_TIMESTAMP_AGGREGATION_TIMEOUT, String.valueOf(10L));

        appendField(userGeneratedMetadataBuilder, SCALING_POLICY, "NO_POLICY");
        appendField(userGeneratedMetadataBuilder, RETENTION_POLICY, "NO_POLICY");

        String userString = userGeneratedMetadataBuilder.toString();
        StreamConfigurationRecordSerializer serializer = new StreamConfigurationRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStreamConfigurationRecordSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_SCOPE, "testScope");
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_STREAM_NAME, "testStream");
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_UPDATING, String.valueOf(false));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_RECORD_REMOVE_TAGS, convertCollectionToString(ImmutableSet.of("test"), s -> s));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_TAGS, convertCollectionToString(ImmutableSet.of("test"), s -> s));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_ROLLOVER_SIZE_BYTES, String.valueOf(100L));
        appendField(userGeneratedMetadataBuilder, STREAM_CONFIGURATION_TIMESTAMP_AGGREGATION_TIMEOUT, String.valueOf(10L));

        appendField(userGeneratedMetadataBuilder, SCALING_POLICY, generateScalingPolicyString("RANDOM", 2, 3, 10));
        appendField(userGeneratedMetadataBuilder, RETENTION_POLICY, generateRetentionPolicyString("SIZE", 3L, 4L));

        String userString = userGeneratedMetadataBuilder.toString();
        StreamConfigurationRecordSerializer serializer = new StreamConfigurationRecordSerializer();
        serializer.serialize(userString);
    }

    private String generateScalingPolicyString(String scaleType, int scaleFactor, int minNumSegments, int targetRate) {
        StringBuilder policyBuilder = new StringBuilder();
        appendFieldWithCustomDelimiters(policyBuilder, SCALING_POLICY_SCALE_TYPE, scaleType, POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(policyBuilder, SCALING_POLICY_SCALE_FACTOR, String.valueOf(scaleFactor), POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(policyBuilder, SCALING_POLICY_MIN_NUM_SEGMENTS, String.valueOf(minNumSegments), POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(policyBuilder, SCALING_POLICY_TARGET_RATE, String.valueOf(targetRate), POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER);
        return policyBuilder.toString();
    }

    private String generateRetentionPolicyString(String retentionType, long retentionParam, long retentionMax) {
        StringBuilder policyBuilder = new StringBuilder();
        appendFieldWithCustomDelimiters(policyBuilder, RETENTION_POLICY_RETENTION_TYPE, retentionType, POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(policyBuilder, RETENTION_POLICY_RETENTION_PARAM, String.valueOf(retentionParam), POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(policyBuilder, RETENTION_POLICY_RETENTION_MAX, String.valueOf(retentionMax), POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER);
        return policyBuilder.toString();
    }
}
