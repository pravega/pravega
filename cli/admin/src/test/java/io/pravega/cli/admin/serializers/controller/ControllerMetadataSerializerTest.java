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

import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.segmentstore.contracts.AttributeId;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isCompletedTransactionsBatchTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isCompletedTransactionsBatchesTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isDeletedStreamsTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isEpochsWithTransactionsTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isStreamMetadataTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isTransactionsInEpochTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isWriterPositionsTableName;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCHES_TABLE;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.CREATION_TIME_KEY;
import static io.pravega.shared.NameUtils.DELETED_STREAMS_TABLE;
import static io.pravega.shared.NameUtils.EPOCHS_WITH_TRANSACTIONS_TABLE;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.METADATA_TABLE;
import static io.pravega.shared.NameUtils.STATE_KEY;
import static io.pravega.shared.NameUtils.TRANSACTIONS_IN_EPOCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.WRITERS_POSITIONS_TABLE;
import static io.pravega.shared.NameUtils.getQualifiedTableName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ControllerMetadataSerializerTest {

    private static final String TEST_SCOPE = "testScope";
    private static final String TEST_STREAM = "testStream";

    @Test
    public void testControllerMetadataSerializer() {
        StateRecord record = new StateRecord(State.ACTIVE);
        ControllerMetadataSerializer serializer = new ControllerMetadataSerializer(
                getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM, String.format(METADATA_TABLE, AttributeId.UUID.randomUUID())),
                STATE_KEY);
        ByteBuffer buf = serializer.serialize(record);
        assertEquals("StateRecord", serializer.getMetadataType());
        assertEquals(record.getState(), ((StateRecord) serializer.deserialize(buf)).getState());
    }

    @Test
    public void testControllerMetadataIntegerSerializer() {
        int record = 1;
        ControllerMetadataSerializer serializer = new ControllerMetadataSerializer(
                DELETED_STREAMS_TABLE, "");
        ByteBuffer buf = serializer.serialize(record);
        assertEquals("Integer", serializer.getMetadataType());
        assertEquals(record, (int) serializer.deserialize(buf));
    }

    @Test
    public void testControllerMetadataLongSerializer() {
        long record = 100L;
        ControllerMetadataSerializer serializer = new ControllerMetadataSerializer(
                getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM, String.format(METADATA_TABLE, AttributeId.UUID.randomUUID())),
                CREATION_TIME_KEY);
        ByteBuffer buf = serializer.serialize(record);
        assertEquals("Long", serializer.getMetadataType());
        assertEquals(record, (long) serializer.deserialize(buf));
    }

    @Test
    public void testControllerMetadataEmptySerializer() {
        String test = "whatever";
        ControllerMetadataSerializer serializer = new ControllerMetadataSerializer(
                COMPLETED_TRANSACTIONS_BATCHES_TABLE, "test_key");
        ByteBuffer buf = serializer.serialize(test);
        assertEquals("Empty", serializer.getMetadataType());
        assertEquals(0, serializer.deserialize(buf));
    }

    @Test
    public void testTableChecks() {
        assertTrue(isStreamMetadataTableName(getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM,
                String.format(METADATA_TABLE, AttributeId.UUID.randomUUID()))));
        assertTrue(isEpochsWithTransactionsTableName(getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM,
                String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, UUID.randomUUID()))));
        assertTrue(isWriterPositionsTableName(getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM,
                String.format(WRITERS_POSITIONS_TABLE, UUID.randomUUID()))));
        assertTrue(isTransactionsInEpochTableName(getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM,
                String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, 1, UUID.randomUUID()))));
        assertTrue(isCompletedTransactionsBatchesTableName(COMPLETED_TRANSACTIONS_BATCHES_TABLE));
        assertTrue(isCompletedTransactionsBatchTableName(getQualifiedTableName(INTERNAL_SCOPE_NAME, String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, 2))));
        assertTrue(isDeletedStreamsTableName(DELETED_STREAMS_TABLE));
    }
}
