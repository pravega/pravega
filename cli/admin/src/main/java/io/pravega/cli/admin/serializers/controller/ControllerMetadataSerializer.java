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
import io.pravega.common.util.BitConverter;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.COMMITTING_TRANSACTIONS_RECORD_KEY;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCHES_TABLE;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.CONFIGURATION_KEY;
import static io.pravega.shared.NameUtils.CREATION_TIME_KEY;
import static io.pravega.shared.NameUtils.CURRENT_EPOCH_KEY;
import static io.pravega.shared.NameUtils.DELETED_STREAMS_TABLE;
import static io.pravega.shared.NameUtils.EPOCHS_WITH_TRANSACTIONS_TABLE;
import static io.pravega.shared.NameUtils.EPOCH_RECORD_KEY_FORMAT;
import static io.pravega.shared.NameUtils.EPOCH_TRANSITION_KEY;
import static io.pravega.shared.NameUtils.HISTORY_TIMESERIES_CHUNK_FORMAT;
import static io.pravega.shared.NameUtils.METADATA_TABLE;
import static io.pravega.shared.NameUtils.RETENTION_SET_KEY;
import static io.pravega.shared.NameUtils.RETENTION_STREAM_CUT_RECORD_KEY_FORMAT;
import static io.pravega.shared.NameUtils.SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT;
import static io.pravega.shared.NameUtils.SEGMENT_MARKER_PATH_FORMAT;
import static io.pravega.shared.NameUtils.SEGMENT_SEALED_EPOCH_KEY_FORMAT;
import static io.pravega.shared.NameUtils.STATE_KEY;
import static io.pravega.shared.NameUtils.SUBSCRIBER_KEY_PREFIX;
import static io.pravega.shared.NameUtils.SUBSCRIBER_SET_KEY;
import static io.pravega.shared.NameUtils.TRANSACTIONS_IN_EPOCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.TRUNCATION_KEY;
import static io.pravega.shared.NameUtils.WAITING_REQUEST_PROCESSOR_PATH;
import static io.pravega.shared.NameUtils.WRITERS_POSITIONS_TABLE;

public class ControllerMetadataSerializer extends AbstractSerializer {

    private static final String UUID_REGEX = "\\b[0-9a-f]{8}\\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\\b[0-9a-f]{12}\\b";
    private static final String NUMBER_REGEX = "[0-9]+";

    private static final Map<String, AbstractSerializer> STREAM_METADATA_TABLE_SERIALIZERS =
            ImmutableMap.<String, AbstractSerializer>builder()
                    .put(CREATION_TIME_KEY, new LongSerializer())
                    .put(CONFIGURATION_KEY, new StreamConfigurationRecordSerializer())
                    .put(TRUNCATION_KEY, new StreamTruncationRecordSerializer())
                    .put(STATE_KEY, new StateRecordSerializer())
                    .put(EPOCH_TRANSITION_KEY, new EpochTransitionRecordSerializer())
                    .put(RETENTION_SET_KEY, new RetentionSetSerializer())
                    .put(String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, NUMBER_REGEX), new StreamCutRecordSerializer())
                    .put(CURRENT_EPOCH_KEY, new IntSerializer())
                    .put(String.format(EPOCH_RECORD_KEY_FORMAT, NUMBER_REGEX), new EpochRecordSerializer())
                    .put(String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, NUMBER_REGEX), new HistoryTimeSeriesSerializer())
                    .put(String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, NUMBER_REGEX), new SealedSegmentsMapShardSerializer())
                    .put(String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, NUMBER_REGEX), new LongSerializer())
                    .put(COMMITTING_TRANSACTIONS_RECORD_KEY, new CommittingTransactionsRecordSerializer())
                    .put(SEGMENT_MARKER_PATH_FORMAT, new LongSerializer())
                    .put(WAITING_REQUEST_PROCESSOR_PATH, new StringSerializer())
                    .put(SUBSCRIBER_KEY_PREFIX, new StreamSubscriberSerializer())
                    .put(SUBSCRIBER_SET_KEY, new SubscribersSerializer())
                    .build();

    /**
     * Map containing key-value pairs as described below:
     *
     * 1) KEY: A function to check if the tableName is of a particular table type or not.
     * For example, function that returns boolean true if the table is a stream metadata table and false otherwise.
     *
     * 2) VALUE: A function to return the required serializer based on the key provided for the given table.
     * In tables where the entries are homogenous, this will simply return the required serializer irrespective
     * of the key, however, if the table contains different entries for different keys then this function should provide
     * the required one based on the provided key
     */
    private static final Map<Function<String, Boolean>, Function<String, AbstractSerializer>> SERIALIZERS =
            ImmutableMap.<Function<String, Boolean>, Function<String, AbstractSerializer>>builder()
                    .put(ControllerMetadataSerializer::isStreamMetadataTableName,
                            key -> STREAM_METADATA_TABLE_SERIALIZERS.entrySet().stream()
                                    .filter(mapEntry -> checkIfPatternExists(key, mapEntry.getKey()))
                                    .map(Map.Entry::getValue)
                                    .collect(Collectors.toList()).stream()
                                    .findFirst()
                                    .orElseThrow(() -> new IllegalArgumentException(String.format("%s is not a valid metadata key.", key))))
                    .put(ControllerMetadataSerializer::isEpochsWithTransactionsTableName, s -> new IntSerializer())
                    .put(ControllerMetadataSerializer::isWriterPositionsTableName, s -> new WriterMarkSerializer())
                    .put(ControllerMetadataSerializer::isTransactionsInEpochTableName, s -> new ActiveTxnRecordSerializer())
                    .put(ControllerMetadataSerializer::isCompletedTransactionsBatchesTableName, s -> new IntSerializer())
                    .put(ControllerMetadataSerializer::isCompletedTransactionsBatchTableName, s -> new CompletedTxnRecordSerializer())
                    .put(ControllerMetadataSerializer::isDeletedStreamsTableName, s -> new IntSerializer())
                    .build();

    private final AbstractSerializer serializer;

    public ControllerMetadataSerializer(String tableName, String key) {
        serializer = SERIALIZERS.entrySet().stream()
                // Get the method to return the serializer for the key, corresponding to given tableName.
                .filter(mapEntry -> mapEntry.getKey().apply(tableName))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList()).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("%s is not a valid controller table.", tableName)))
                .apply(key);
    }

    @Override
    public String getName() {
        return serializer.getName();
    }

    @Override
    public ByteBuffer serialize(String value) {
        return serializer.serialize(value);
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return serializer.deserialize(serializedValue);
    }

    /**
     * Checks whether the given name is a Stream Metadata Table.
     *
     * @param tableName The name of the table.
     * @return true if the name is Stream Metadata Table. False otherwise
     */
    public static boolean isStreamMetadataTableName(String tableName) {
        return checkIfPatternExists(tableName, String.format(METADATA_TABLE, UUID_REGEX));
    }

    /**
     * Checks whether the given name is an EpochsWithTransactions Table.
     *
     * @param tableName The name of the table.
     * @return true if the name is EpochsWithTransactions Table. False otherwise
     */
    public static boolean isEpochsWithTransactionsTableName(String tableName) {
        return checkIfPatternExists(tableName, String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, UUID_REGEX));
    }

    /**
     * Checks whether the given name is an TransactionsInEpoch Table.
     *
     * @param tableName The name of the table.
     * @return true if the name is TransactionsInEpoch Table. False otherwise
     */
    public static boolean isTransactionsInEpochTableName(String tableName) {
        return checkIfPatternExists(tableName, String.format(TRANSACTIONS_IN_EPOCH_TABLE_FORMAT, NUMBER_REGEX, UUID_REGEX));
    }

    /**
     * Checks whether the given name is a WriterPositions Table.
     *
     * @param tableName The name of the table.
     * @return true if the name is WriterPositions Table. False otherwise
     */
    public static boolean isWriterPositionsTableName(String tableName) {
        return checkIfPatternExists(tableName, String.format(WRITERS_POSITIONS_TABLE, UUID_REGEX));
    }

    /**
     * Checks whether the given name is a CompletedTransactionsBatches Table.
     *
     * @param tableName The name of the table.
     * @return true if the name is CompletedTransactionsBatches Table. False otherwise.
     */
    public static boolean isCompletedTransactionsBatchesTableName(String tableName) {
        return checkIfPatternExists(tableName, COMPLETED_TRANSACTIONS_BATCHES_TABLE);
    }

    /**
     * Checks whether the given name is a CompletedTransactionsBatch Table.
     *
     * @param tableName The name of the table.
     * @return true if the name is CompletedTransactionsBatch Table. False otherwise.
     */
    public static boolean isCompletedTransactionsBatchTableName(String tableName) {
        return checkIfPatternExists(tableName, String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, NUMBER_REGEX));
    }

    /**
     * Check whether the given name is a DeletedStreams Table.
     *
     * @param tableName The name of the table.
     * @return true if the name is DeletedStreams Table. False otherwise.
     */
    public static boolean isDeletedStreamsTableName(String tableName) {
        return checkIfPatternExists(tableName, DELETED_STREAMS_TABLE);
    }

    private static boolean checkIfPatternExists(String line, String pattern) {
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(line);
        return m.find();
    }

    private static class StringSerializer extends AbstractSerializer {
        @Override
        public String getName() {
            return "ControllerString";
        }

        @Override
        public ByteBuffer serialize(String value) {
            Map<String, String> data = parseStringData(value);
            return ByteBuffer.wrap(getAndRemoveIfExists(data, getName()).getBytes(Charsets.UTF_8));
        }

        @Override
        public String deserialize(ByteBuffer serializedValue) {
            Map<String, Function<String, String>> fieldMap = new HashMap<>();
            fieldMap.put(getName(), s -> s);
            return applyDeserializer(serializedValue, bytes -> new String(bytes, StandardCharsets.UTF_8), fieldMap);
        }
    }

    private static class IntSerializer extends AbstractSerializer {
        @Override
        public String getName() {
            return "ControllerInt";
        }

        @Override
        public ByteBuffer serialize(String value) {
            Map<String, String> data = parseStringData(value);
            ByteBuffer b = ByteBuffer.allocate(4);
            b.putInt(Integer.parseInt(getAndRemoveIfExists(data, getName())));
            return b;
        }

        @Override
        public String deserialize(ByteBuffer serializedValue) {
            Map<String, Function<Integer, String>> fieldMap = new HashMap<>();
            fieldMap.put(getName(), String::valueOf);
            return applyDeserializer(serializedValue, bytes -> BitConverter.readInt(bytes, 0), fieldMap);
        }
    }

    private static class LongSerializer extends AbstractSerializer {
        @Override
        public String getName() {
            return "ControllerLong";
        }

        @Override
        public ByteBuffer serialize(String value) {
            Map<String, String> data = parseStringData(value);
            ByteBuffer b = ByteBuffer.allocate(8);
            b.putLong(Long.parseLong(getAndRemoveIfExists(data, getName())));
            return b;
        }

        @Override
        public String deserialize(ByteBuffer serializedValue) {
            Map<String, Function<Long, String>> fieldMap = new HashMap<>();
            fieldMap.put(getName(), String::valueOf);
            return applyDeserializer(serializedValue, bytes -> BitConverter.readLong(bytes, 0), fieldMap);
        }
    }
}
