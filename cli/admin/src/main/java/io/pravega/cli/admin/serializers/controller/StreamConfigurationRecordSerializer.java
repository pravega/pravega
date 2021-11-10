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
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class StreamConfigurationRecordSerializer extends AbstractSerializer {

    static final String STREAM_CONFIGURATION_RECORD_SCOPE = "scope";
    static final String STREAM_CONFIGURATION_RECORD_STREAM_NAME = "streamName";
    static final String STREAM_CONFIGURATION_RECORD_UPDATING = "updating";
    static final String STREAM_CONFIGURATION_RECORD_TAG_ONLY_UPDATE = "tagOnlyUpdate";
    static final String STREAM_CONFIGURATION_RECORD_REMOVE_TAGS = "removeTags";

    static final String STREAM_CONFIGURATION_ROLLOVER_SIZE_BYTES = "rolloverSizeBytes";
    static final String STREAM_CONFIGURATION_TAGS = "tags";
    static final String STREAM_CONFIGURATION_TIMESTAMP_AGGREGATION_TIMEOUT = "timestampAggregationTimeout";

    static final String SCALING_POLICY = "scalingPolicy";
    static final String SCALING_POLICY_SCALE_TYPE = "scaleType";
    static final String SCALING_POLICY_SCALE_FACTOR = "scaleFactor";
    static final String SCALING_POLICY_MIN_NUM_SEGMENTS = "minNumSegments";
    static final String SCALING_POLICY_TARGET_RATE = "targetRate";

    static final String RETENTION_POLICY = "retentionPolicy";
    static final String RETENTION_POLICY_RETENTION_TYPE = "retentionType";
    static final String RETENTION_POLICY_RETENTION_PARAM = "retentionParam";
    static final String RETENTION_POLICY_RETENTION_MAX = "retentionMax";

    static final String POLICY_PAIR_DELIMITER = "|";
    static final String POLICY_VALUE_DELIMITER = ":";

    private static final String POLICY_PAIR_DELIMITER_REGEX = "\\|";
    private static final String NO_POLICY = "NO_POLICY";

    private static final Map<String, Function<ScalingPolicy, String>> SCALING_POLICY_FIELD_MAP =
            ImmutableMap.<String, Function<ScalingPolicy, String>>builder()
                    .put(SCALING_POLICY_SCALE_TYPE, sp -> sp.getScaleType().toString())
                    .put(SCALING_POLICY_SCALE_FACTOR, sp -> String.valueOf(sp.getScaleFactor()))
                    .put(SCALING_POLICY_MIN_NUM_SEGMENTS, sp -> String.valueOf(sp.getMinNumSegments()))
                    .put(SCALING_POLICY_TARGET_RATE, sp -> String.valueOf(sp.getTargetRate()))
                    .build();

    private static final Map<String, Function<RetentionPolicy, String>> RETENTION_POLICY_FIELD_MAP =
            ImmutableMap.<String, Function<RetentionPolicy, String>>builder()
                    .put(RETENTION_POLICY_RETENTION_TYPE, rp -> rp.getRetentionType().toString())
                    .put(RETENTION_POLICY_RETENTION_PARAM, rp -> String.valueOf(rp.getRetentionParam()))
                    .put(RETENTION_POLICY_RETENTION_MAX, rp -> String.valueOf(rp.getRetentionMax()))
                    .build();

    private static final Map<String, Function<StreamConfigurationRecord, String>> STREAM_CONFIGURATION_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<StreamConfigurationRecord, String>>builder()
                    .put(STREAM_CONFIGURATION_RECORD_SCOPE, StreamConfigurationRecord::getScope)
                    .put(STREAM_CONFIGURATION_RECORD_STREAM_NAME, StreamConfigurationRecord::getStreamName)
                    .put(STREAM_CONFIGURATION_RECORD_UPDATING, r -> String.valueOf(r.isUpdating()))
                    .put(STREAM_CONFIGURATION_RECORD_TAG_ONLY_UPDATE, r -> String.valueOf(r.isTagOnlyUpdate()))
                    .put(STREAM_CONFIGURATION_RECORD_REMOVE_TAGS, r -> convertCollectionToString(r.getRemoveTags(), s -> s))
                    .put(STREAM_CONFIGURATION_TAGS, r -> convertCollectionToString(r.getStreamConfiguration().getTags(), s -> s))
                    .put(STREAM_CONFIGURATION_ROLLOVER_SIZE_BYTES, r -> String.valueOf(r.getStreamConfiguration().getRolloverSizeBytes()))
                    .put(STREAM_CONFIGURATION_TIMESTAMP_AGGREGATION_TIMEOUT, r -> String.valueOf(r.getStreamConfiguration().getTimestampAggregationTimeout()))
                    .put(SCALING_POLICY, r -> convertPolicyToString(r.getStreamConfiguration().getScalingPolicy(), SCALING_POLICY_FIELD_MAP))
                    .put(RETENTION_POLICY, r -> convertPolicyToString(r.getStreamConfiguration().getRetentionPolicy(), RETENTION_POLICY_FIELD_MAP))
                    .build();

    @Override
    public String getName() {
        return "StreamConfigurationRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        StreamConfigurationRecord record = StreamConfigurationRecord.builder()
                .scope(getAndRemoveIfExists(data, STREAM_CONFIGURATION_RECORD_SCOPE))
                .streamName(getAndRemoveIfExists(data, STREAM_CONFIGURATION_RECORD_STREAM_NAME))
                .updating(Boolean.parseBoolean(getAndRemoveIfExists(data, STREAM_CONFIGURATION_RECORD_UPDATING)))
                .tagOnlyUpdate(Boolean.parseBoolean(getAndRemoveIfExists(data, STREAM_CONFIGURATION_RECORD_TAG_ONLY_UPDATE)))
                .removeTags(convertStringToCollection(getAndRemoveIfExists(data, STREAM_CONFIGURATION_RECORD_REMOVE_TAGS), s -> s))
                .streamConfiguration(getStreamConfigurationFromData(data))
                .build();
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, StreamConfigurationRecord::fromBytes, STREAM_CONFIGURATION_RECORD_FIELD_MAP);
    }

    private static <T> String convertPolicyToString(T policy, Map<String, Function<T, String>> fieldMap) {
        if (policy != null) {
            StringBuilder policyBuilder = new StringBuilder();
            fieldMap.forEach((name, f) -> appendFieldWithCustomDelimiters(policyBuilder, name, f.apply(policy),
                    POLICY_PAIR_DELIMITER, POLICY_VALUE_DELIMITER));
            return policyBuilder.toString();
        }
        return NO_POLICY;
    }

    private static StreamConfiguration getStreamConfigurationFromData(Map<String, String> data) {
        return StreamConfiguration.builder()
                .rolloverSizeBytes(Long.parseLong(getAndRemoveIfExists(data, STREAM_CONFIGURATION_ROLLOVER_SIZE_BYTES)))
                .timestampAggregationTimeout(Long.parseLong(getAndRemoveIfExists(data, STREAM_CONFIGURATION_TIMESTAMP_AGGREGATION_TIMEOUT)))
                .tags(convertStringToCollection(getAndRemoveIfExists(data, STREAM_CONFIGURATION_TAGS), s -> s))
                .scalingPolicy(getScalingPolicyFromData(getAndRemoveIfExists(data, SCALING_POLICY)))
                .retentionPolicy(getRetentionPolicyFromData(getAndRemoveIfExists(data, RETENTION_POLICY)))
                .build();
    }

    private static ScalingPolicy getScalingPolicyFromData(String scalingPolicyData) {
        if (scalingPolicyData.equalsIgnoreCase(NO_POLICY)) {
            return null;
        }
        Map<String, String> scalingPolicyDataMap = parseStringDataWithCustomDelimiters(scalingPolicyData, POLICY_PAIR_DELIMITER_REGEX, POLICY_VALUE_DELIMITER);
        return ScalingPolicy.builder()
                .scaleType(ScalingPolicy.ScaleType.valueOf(getAndRemoveIfExists(scalingPolicyDataMap, SCALING_POLICY_SCALE_TYPE).toUpperCase()))
                .scaleFactor(Integer.parseInt(getAndRemoveIfExists(scalingPolicyDataMap, SCALING_POLICY_SCALE_FACTOR)))
                .targetRate(Integer.parseInt(getAndRemoveIfExists(scalingPolicyDataMap, SCALING_POLICY_TARGET_RATE)))
                .minNumSegments(Integer.parseInt(getAndRemoveIfExists(scalingPolicyDataMap, SCALING_POLICY_MIN_NUM_SEGMENTS)))
                .build();
    }

    private static RetentionPolicy getRetentionPolicyFromData(String retentionPolicyData) {
        if (retentionPolicyData.equalsIgnoreCase(NO_POLICY)) {
            return null;
        }
        Map<String, String> retentionPolicyDataMap = parseStringDataWithCustomDelimiters(retentionPolicyData, POLICY_PAIR_DELIMITER_REGEX, POLICY_VALUE_DELIMITER);
        return RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.valueOf(getAndRemoveIfExists(retentionPolicyDataMap, RETENTION_POLICY_RETENTION_TYPE).toUpperCase()))
                .retentionParam(Long.parseLong(getAndRemoveIfExists(retentionPolicyDataMap, RETENTION_POLICY_RETENTION_PARAM)))
                .retentionMax(Long.parseLong(getAndRemoveIfExists(retentionPolicyDataMap, RETENTION_POLICY_RETENTION_MAX)))
                .build();
    }
}
