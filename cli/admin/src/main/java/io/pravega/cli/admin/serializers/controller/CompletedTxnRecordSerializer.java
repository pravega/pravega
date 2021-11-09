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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class CompletedTxnRecordSerializer extends AbstractSerializer {

    static final String COMPLETED_TXN_RECORD_COMPLETE_TIME = "completeTime";
    static final String COMPLETED_TXN_RECORD_COMPLETION_STATUS = "completionStatus";

    private static final Map<String, Function<CompletedTxnRecord, String>> COMPLETED_TXN_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<CompletedTxnRecord, String>>builder()
                    .put(COMPLETED_TXN_RECORD_COMPLETE_TIME, r -> String.valueOf(r.getCompleteTime()))
                    .put(COMPLETED_TXN_RECORD_COMPLETION_STATUS, r -> r.getCompletionStatus().toString())
                    .build();

    @Override
    public String getName() {
        return "CompletedTxnRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);

        CompletedTxnRecord record =  CompletedTxnRecord.builder()
                .completeTime(Long.parseLong(getAndRemoveIfExists(data, COMPLETED_TXN_RECORD_COMPLETE_TIME)))
                .completionStatus(TxnStatus.valueOf(getAndRemoveIfExists(data, COMPLETED_TXN_RECORD_COMPLETION_STATUS).toUpperCase()))
                .build();
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, CompletedTxnRecord::fromBytes, COMPLETED_TXN_RECORD_FIELD_MAP);
    }
}
