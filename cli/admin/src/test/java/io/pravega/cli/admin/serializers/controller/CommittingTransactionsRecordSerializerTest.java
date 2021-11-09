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
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.controller.CommittingTransactionsRecordSerializer.COMMITTING_TRANSACTIONS_RECORD_ACTIVE_EPOCH;
import static io.pravega.cli.admin.serializers.controller.CommittingTransactionsRecordSerializer.COMMITTING_TRANSACTIONS_RECORD_EPOCH;
import static io.pravega.cli.admin.serializers.controller.CommittingTransactionsRecordSerializer.COMMITTING_TRANSACTIONS_RECORD_TRANSACTIONS_TO_COMMIT;
import static io.pravega.cli.admin.serializers.controller.CommittingTransactionsRecordSerializer.convertCollectionToString;
import static org.junit.Assert.assertEquals;

public class CommittingTransactionsRecordSerializerTest {

    @Test
    public void testCommittingTransactionsRecordSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, COMMITTING_TRANSACTIONS_RECORD_EPOCH, String.valueOf(10));
        appendField(userGeneratedMetadataBuilder, COMMITTING_TRANSACTIONS_RECORD_TRANSACTIONS_TO_COMMIT,
                convertCollectionToString(ImmutableList.of(UUID.randomUUID(), UUID.randomUUID()), UUID::toString));
        appendField(userGeneratedMetadataBuilder, COMMITTING_TRANSACTIONS_RECORD_ACTIVE_EPOCH, String.valueOf(12));

        String userString = userGeneratedMetadataBuilder.toString();
        CommittingTransactionsRecordSerializer serializer = new CommittingTransactionsRecordSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCommittingTransactionsRecordSerializerArgumentFailure() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, COMMITTING_TRANSACTIONS_RECORD_EPOCH, String.valueOf(10));
        appendField(userGeneratedMetadataBuilder, COMMITTING_TRANSACTIONS_RECORD_ACTIVE_EPOCH, String.valueOf(12));

        String userString = userGeneratedMetadataBuilder.toString();
        CommittingTransactionsRecordSerializer serializer = new CommittingTransactionsRecordSerializer();
        serializer.serialize(userString);
    }
}
