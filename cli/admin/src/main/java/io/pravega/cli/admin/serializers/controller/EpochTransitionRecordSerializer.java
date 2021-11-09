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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class EpochTransitionRecordSerializer extends AbstractSerializer {

    static final String EPOCH_TRANSITION_RECORD_ACTIVE_EPOCH = "activeEpoch";
    static final String EPOCH_TRANSITION_RECORD_TIME = "time";
    static final String EPOCH_TRANSITION_RECORD_SEGMENTS_TO_SEAL = "segmentsToSeal";
    static final String EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE = "newSegmentsWithRange";

    private static final Map<String, Function<EpochTransitionRecord, String>> EPOCH_TRANSITION_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<EpochTransitionRecord, String>>builder()
                    .put(EPOCH_TRANSITION_RECORD_ACTIVE_EPOCH, r -> String.valueOf(r.getActiveEpoch()))
                    .put(EPOCH_TRANSITION_RECORD_TIME, r -> String.valueOf(r.getTime()))
                    .put(EPOCH_TRANSITION_RECORD_SEGMENTS_TO_SEAL, r -> convertCollectionToString(r.getSegmentsToSeal(), String::valueOf))
                    .put(EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE, r ->
                            convertMapToString(r.getNewSegmentsWithRange(), String::valueOf, v -> v.getKey() + "-" + v.getValue()))
                    .build();

    @Override
    public String getName() {
        return "EpochTransitionRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);

        Set<Long> segmentsToSeal = new HashSet<>(convertStringToCollection(getAndRemoveIfExists(data, EPOCH_TRANSITION_RECORD_SEGMENTS_TO_SEAL), Long::parseLong));
        Map<Long, Map.Entry<Double, Double>> newSegmentsWithRange =
                convertStringToMap(getAndRemoveIfExists(data, EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE), Long::parseLong, s -> {
                    List<String> pair = Arrays.asList(s.split("-"));
                    Preconditions.checkArgument(pair.size() == 2,
                            String.format("Incomplete key-value pair provided in the %s.%s map", getName(), EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE));
                    return Map.entry(Double.parseDouble(pair.get(0)), Double.parseDouble(pair.get(1)));
                    }, getName() + "." + EPOCH_TRANSITION_RECORD_NEW_SEGMENTS_WITH_RANGE);

        EpochTransitionRecord record = new EpochTransitionRecord(
                Integer.parseInt(getAndRemoveIfExists(data, EPOCH_TRANSITION_RECORD_ACTIVE_EPOCH)),
                Long.parseLong(getAndRemoveIfExists(data, EPOCH_TRANSITION_RECORD_TIME)),
                ImmutableSet.copyOf(segmentsToSeal),
                ImmutableMap.copyOf(newSegmentsWithRange));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, EpochTransitionRecord::fromBytes, EPOCH_TRANSITION_RECORD_FIELD_MAP);
    }
}
