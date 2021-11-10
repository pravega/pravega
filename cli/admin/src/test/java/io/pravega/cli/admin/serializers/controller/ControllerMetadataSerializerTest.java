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

import io.pravega.segmentstore.contracts.AttributeId;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.IntSerializer;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.LongSerializer;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.StringSerializer;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isCompletedTransactionsBatchTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isCompletedTransactionsBatchesTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isDeletedStreamsTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isEpochsWithTransactionsTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isStreamMetadataTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isTransactionsInEpochTableName;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.isWriterPositionsTableName;
import static io.pravega.cli.admin.serializers.controller.StateRecordSerializer.STATE_RECORD_STATE;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCHES_TABLE;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
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
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, STATE_RECORD_STATE, "ACTIVE");

        String userString = userGeneratedMetadataBuilder.toString();
        ControllerMetadataSerializer serializer = new ControllerMetadataSerializer(
                getQualifiedTableName(INTERNAL_SCOPE_NAME, TEST_SCOPE, TEST_STREAM, String.format(METADATA_TABLE, AttributeId.UUID.randomUUID())),
                STATE_KEY);
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
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

    @Test
    public void testPrimitiveSerializers() {
        StringSerializer stringSerializer = new StringSerializer();
        StringBuilder userGeneratedStringBuilder = new StringBuilder();
        appendField(userGeneratedStringBuilder, stringSerializer.getName(), "test_message");
        String userString = userGeneratedStringBuilder.toString();
        ByteBuffer buf = stringSerializer.serialize(userString);
        assertEquals(userString, stringSerializer.deserialize(buf));

        IntSerializer intSerializer = new IntSerializer();
        StringBuilder userGeneratedIntBuilder = new StringBuilder();
        appendField(userGeneratedIntBuilder, intSerializer.getName(), String.valueOf(2));
        userString = userGeneratedIntBuilder.toString();
        buf = intSerializer.serialize(userString);
        assertEquals(userString, intSerializer.deserialize(buf));

        LongSerializer longSerializer = new LongSerializer();
        StringBuilder userGeneratedLongBuilder = new StringBuilder();
        appendField(userGeneratedLongBuilder, longSerializer.getName(), String.valueOf(2000L));
        userString = userGeneratedLongBuilder.toString();
        buf = longSerializer.serialize(userString);
        assertEquals(userString, longSerializer.deserialize(buf));
    }
}
