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
import io.pravega.controller.store.stream.records.ActiveTxnRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;

public class ActiveTxnRecordSerializer extends AbstractSerializer {

    public static final String ACTIVE_TXN_RECORD_TX_CREATION_TIMESTAMP = "txCreationTimestamp";
    public static final String ACTIVE_TXN_RECORD_LEASE_EXPIRY_TIME = "leaseExpiryTime";
    public static final String ACTIVE_TXN_RECORD_MAX_EXECUTION_EXPIRY_TIME = "maxExecutionExpiryTime";
    public static final String ACTIVE_TXN_RECORD_TXN_STATUS = "txnStatus";
    public static final String ACTIVE_TXN_RECORD_WRITER_ID = "writerId";
    public static final String ACTIVE_TXN_RECORD_COMMIT_TIME = "commitTime";
    public static final String ACTIVE_TXN_RECORD_COMMIT_ORDER = "commitOrder";
    public static final String ACTIVE_TXN_RECORD_COMMIT_OFFSETS = "commitOffsets";

    private static final Map<String, Function<ActiveTxnRecord, String>> ACTIVE_TXN_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<ActiveTxnRecord, String>>builder()
                    .put(ACTIVE_TXN_RECORD_TX_CREATION_TIMESTAMP, r -> String.valueOf(r.getTxCreationTimestamp()))
                    .put(ACTIVE_TXN_RECORD_LEASE_EXPIRY_TIME, r -> String.valueOf(r.getLeaseExpiryTime()))
                    .put(ACTIVE_TXN_RECORD_MAX_EXECUTION_EXPIRY_TIME, r -> String.valueOf(r.getMaxExecutionExpiryTime()))
                    .put(ACTIVE_TXN_RECORD_TXN_STATUS, r -> r.getTxnStatus().toString())
                    .put(ACTIVE_TXN_RECORD_WRITER_ID, ActiveTxnRecord::getWriterId)
                    .put(ACTIVE_TXN_RECORD_COMMIT_TIME, r -> String.valueOf(r.getCommitTime()))
                    .put(ACTIVE_TXN_RECORD_COMMIT_ORDER, r -> String.valueOf(r.getCommitOrder()))
                    .put(ACTIVE_TXN_RECORD_COMMIT_OFFSETS, r -> convertMapToString(r.getCommitOffsets(), String::valueOf, String::valueOf))
                    .build();

    @Override
    public String getName() {
        return "ActiveTxnRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        Map<Long, Long> commitOffset = convertStringToMap(getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_COMMIT_OFFSETS), Long::parseLong, Long::parseLong,
                getName() + "." + ACTIVE_TXN_RECORD_COMMIT_OFFSETS);

        ActiveTxnRecord record = new ActiveTxnRecord(Long.parseLong(getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_TX_CREATION_TIMESTAMP)),
                Long.parseLong(getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_LEASE_EXPIRY_TIME)),
                Long.parseLong(getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_MAX_EXECUTION_EXPIRY_TIME)),
                TxnStatus.valueOf(getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_TXN_STATUS).toUpperCase()),
                getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_WRITER_ID),
                Long.parseLong(getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_COMMIT_TIME)),
                Long.parseLong(getAndRemoveIfExists(data, ACTIVE_TXN_RECORD_COMMIT_ORDER)),
                ImmutableMap.copyOf(commitOffset));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return applyDeserializer(serializedValue, ActiveTxnRecord::fromBytes, ACTIVE_TXN_RECORD_FIELD_MAP);
    }
}
