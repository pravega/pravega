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
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class CommittingTransactionsRecordSerializer extends AbstractSerializer {

    public static final String COMMITTING_TRANSACTIONS_RECORD_EPOCH = "epoch";
    public static final String COMMITTING_TRANSACTIONS_RECORD_TRANSACTIONS_TO_COMMIT = "transactionsToCommit";
    public static final String COMMITTING_TRANSACTIONS_RECORD_ACTIVE_EPOCH = "activeEpoch";

    private static final Map<String, Function<CommittingTransactionsRecord, String>> COMMITTING_TRANSACTIONS_RECORD_FIELD_MAP =
            ImmutableMap.<String, Function<CommittingTransactionsRecord, String>>builder()
                    .put(COMMITTING_TRANSACTIONS_RECORD_EPOCH, r -> String.valueOf(r.getEpoch()))
                    .put(COMMITTING_TRANSACTIONS_RECORD_TRANSACTIONS_TO_COMMIT, r -> convertCollectionToString(r.getTransactionsToCommit(), UUID::toString))
                    .put(COMMITTING_TRANSACTIONS_RECORD_ACTIVE_EPOCH, r -> String.valueOf(r.getCurrentEpoch()))
                    .build();

    @Override
    public String getName() {
        return "CommittingTransactionsRecord";
    }

    @Override
    public ByteBuffer serialize(String value) {
        Map<String, String> data = parseStringData(value);
        List<UUID> transactionsToCommit = new ArrayList<>(convertStringToCollection(getAndRemoveIfExists(data, COMMITTING_TRANSACTIONS_RECORD_TRANSACTIONS_TO_COMMIT),
                UUID::fromString));

        CommittingTransactionsRecord record = new CommittingTransactionsRecord(Integer.parseInt(getAndRemoveIfExists(data, COMMITTING_TRANSACTIONS_RECORD_EPOCH)),
                ImmutableList.copyOf(transactionsToCommit), Integer.parseInt(getAndRemoveIfExists(data, COMMITTING_TRANSACTIONS_RECORD_ACTIVE_EPOCH)));
        return new ByteArraySegment(record.toBytes()).asByteBuffer();
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        StringBuilder stringValueBuilder;
        CommittingTransactionsRecord data = CommittingTransactionsRecord.fromBytes(new ByteArraySegment(serializedValue).getCopy());
        stringValueBuilder = new StringBuilder();
        COMMITTING_TRANSACTIONS_RECORD_FIELD_MAP.forEach((name, f) -> appendField(stringValueBuilder, name, f.apply(data)));
        return stringValueBuilder.toString();
    }
}
