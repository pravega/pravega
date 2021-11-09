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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class EpochRecordSerializer extends AbstractSerializer {

    static final String EPOCH_RECORD_EPOCH = "epoch";
    static final String EPOCH_RECORD_REFERENCE_EPOCH = "referenceEpoch";
    static final String EPOCH_RECORD_SEGMENTS = "segments";
    static final String EPOCH_RECORD_CREATION_TIME = "creationTime";
    static final String EPOCH_RECORD_SPLITS = "splits";
    static final String EPOCH_RECORD_MERGES = "merges";

    private static final Map<String, Function<EpochRecord, String>> EPOCH_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<EpochRecord, String>>builder()
                    .put(EPOCH_RECORD_EPOCH, r -> String.valueOf(r.getEpoch()))
                    .put(EPOCH_RECORD_REFERENCE_EPOCH, r -> String.valueOf(r.getReferenceEpoch()))
                    .put(EPOCH_RECORD_CREATION_TIME, r -> String.valueOf(r.getCreationTime()))
                    .put(EPOCH_RECORD_SPLITS, r -> String.valueOf(r.getSplits()))
                    .put(EPOCH_RECORD_MERGES, r -> String.valueOf(r.getMerges()))
                    .put(EPOCH_RECORD_SEGMENTS, r -> convertCollectionToString(r.getSegments(), AbstractSerializer::convertStreamSegmentRecordToString))
                    .build();

    @Override
    public String getName() {
        return "EpochRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        List<StreamSegmentRecord> segments = new ArrayList<>(convertStringToCollection(getAndRemoveIfExists(data, EPOCH_RECORD_SEGMENTS),
                AbstractSerializer::convertStringToStreamSegmentRecord));

        EpochRecord record = new EpochRecord(
                Integer.parseInt(getAndRemoveIfExists(data, EPOCH_RECORD_EPOCH)), Integer.parseInt(getAndRemoveIfExists(data, EPOCH_RECORD_REFERENCE_EPOCH)),
                ImmutableList.copyOf(segments),
                Long.parseLong(getAndRemoveIfExists(data, EPOCH_RECORD_CREATION_TIME)),
                Long.parseLong(getAndRemoveIfExists(data, EPOCH_RECORD_SPLITS)),
                Long.parseLong(getAndRemoveIfExists(data, EPOCH_RECORD_MERGES)));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, EpochRecord::fromBytes, EPOCH_RECORD_FIELD_MAP);
    }
}
