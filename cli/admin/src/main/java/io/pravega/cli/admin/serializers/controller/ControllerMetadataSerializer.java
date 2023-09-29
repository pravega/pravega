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
import io.pravega.client.stream.Serializer;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.CompletedTxnRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamSubscriber;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.Subscribers;
import io.pravega.controller.store.stream.records.WriterMark;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.val;
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

public class ControllerMetadataSerializer implements Serializer<Object> {

    public static final String STRING = "String";
    public static final String INTEGER = "Integer";
    public static final String LONG = "Long";
    public static final String EMPTY = "Empty";

    private static final String UUID_REGEX = "\\b[0-9a-f]{8}\\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\\b[0-9a-f]{12}\\b";
    private static final String NUMBER_REGEX = "[0-9]+";

    private static final String STREAM_CONFIGURATION_RECORD = "StreamConfigurationRecord";
    private static final String STREAM_TRUNCATION_RECORD = "StreamTruncationRecord";
    private static final String STATE_RECORD = "StateRecord";
    private static final String EPOCH_TRANSITION_RECORD = "EpochTransitionRecord";
    private static final String RETENTION_SET = "RetentionSet";
    private static final String STREAM_CUT_RECORD = "StreamCutRecord";
    private static final String EPOCH_RECORD = "EpochRecord";
    private static final String HISTORY_TIME_SERIES = "HistoryTimeSeries";
    private static final String SEALED_SEGMENTS_MAP_SHARD = "SealedSegmentsMapShard";
    private static final String COMMITTING_TRANSACTIONS_RECORD = "CommittingTransactionsRecord";
    private static final String STREAM_SUBSCRIBER = "StreamSubscriber";
    private static final String SUBSCRIBERS = "Subscribers";

    private static final String WRITER_MARK = "WriterMark";
    private static final String ACTIVE_TXN_RECORD = "ActiveTxnRecord";
    private static final String COMPLETED_TXN_RECORD = "CompletedTxnRecord";

    private static final Map<String, String> STREAM_METADATA_TABLE_TYPES =
            ImmutableMap.<String, String>builder()
                    .put(CREATION_TIME_KEY, LONG)
                    .put(CONFIGURATION_KEY, STREAM_CONFIGURATION_RECORD)
                    .put(TRUNCATION_KEY, STREAM_TRUNCATION_RECORD)
                    .put(STATE_KEY, STATE_RECORD)
                    .put(EPOCH_TRANSITION_KEY, EPOCH_TRANSITION_RECORD)
                    .put(RETENTION_SET_KEY, RETENTION_SET)
                    .put(String.format(RETENTION_STREAM_CUT_RECORD_KEY_FORMAT, NUMBER_REGEX), STREAM_CUT_RECORD)
                    .put(CURRENT_EPOCH_KEY, INTEGER)
                    .put(String.format(EPOCH_RECORD_KEY_FORMAT, NUMBER_REGEX), EPOCH_RECORD)
                    .put(String.format(HISTORY_TIMESERIES_CHUNK_FORMAT, NUMBER_REGEX), HISTORY_TIME_SERIES)
                    .put(String.format(SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT, NUMBER_REGEX), SEALED_SEGMENTS_MAP_SHARD)
                    .put(String.format(SEGMENT_SEALED_EPOCH_KEY_FORMAT, NUMBER_REGEX), INTEGER)
                    .put(COMMITTING_TRANSACTIONS_RECORD_KEY, COMMITTING_TRANSACTIONS_RECORD)
                    .put(SEGMENT_MARKER_PATH_FORMAT, LONG)
                    .put(WAITING_REQUEST_PROCESSOR_PATH, STRING)
                    .put(SUBSCRIBER_KEY_PREFIX, STREAM_SUBSCRIBER)
                    .put(SUBSCRIBER_SET_KEY, SUBSCRIBERS)
                    .build();

    private static final Map<Function<String, Boolean>, Function<String, String>> METADATA_TYPES =
            ImmutableMap.<Function<String, Boolean>, Function<String, String>>builder()
                    .put(ControllerMetadataSerializer::isStreamMetadataTableName,
                            key -> STREAM_METADATA_TABLE_TYPES.entrySet().stream()
                                    .filter(mapEntry -> checkIfPatternExists(key, mapEntry.getKey()))
                                    .map(Map.Entry::getValue)
                                    .findFirst()
                                    .orElseThrow(() -> new IllegalArgumentException(String.format("%s is not a valid metadata key.", key))))
                    .put(ControllerMetadataSerializer::isEpochsWithTransactionsTableName, s -> EMPTY)
                    .put(ControllerMetadataSerializer::isWriterPositionsTableName, s -> WRITER_MARK)
                    .put(ControllerMetadataSerializer::isTransactionsInEpochTableName, s -> ACTIVE_TXN_RECORD)
                    .put(ControllerMetadataSerializer::isCompletedTransactionsBatchesTableName, s -> EMPTY)
                    .put(ControllerMetadataSerializer::isCompletedTransactionsBatchTableName, s -> COMPLETED_TXN_RECORD)
                    .put(ControllerMetadataSerializer::isDeletedStreamsTableName, s -> INTEGER)
                    .build();

    private static final Map<String, Map.Entry<Function<byte[], Object>, Function<Object, byte[]>>> BYTE_CONVERTERS =
            ImmutableMap.<String, Map.Entry<Function<byte[], Object>, Function<Object, byte[]>>>builder()
                    .put(STRING, Map.entry(bytes -> new String(bytes, StandardCharsets.UTF_8), x -> String.valueOf(x).getBytes(StandardCharsets.UTF_8)))
                    .put(INTEGER, Map.entry(bytes -> BitConverter.readInt(bytes, 0), x -> {
                        byte[] bytes = new byte[Integer.BYTES];
                        BitConverter.writeInt(bytes, 0, (int) x);
                        return bytes;
                    }))
                    .put(LONG, Map.entry(bytes -> BitConverter.readLong(bytes, 0), x -> {
                        byte[] bytes = new byte[Long.BYTES];
                        BitConverter.writeLong(bytes, 0, (long) x);
                        return bytes;
                    }))
                    .put(EMPTY, Map.entry(bytes -> 0, x -> new byte[0]))
                    .put(STREAM_CONFIGURATION_RECORD, Map.entry(StreamConfigurationRecord::fromBytes, x -> ((StreamConfigurationRecord) x).toBytes()))
                    .put(STREAM_TRUNCATION_RECORD, Map.entry(StreamTruncationRecord::fromBytes, x -> ((StreamTruncationRecord) x).toBytes()))
                    .put(STATE_RECORD, Map.entry(StateRecord::fromBytes, x -> ((StateRecord) x).toBytes()))
                    .put(EPOCH_TRANSITION_RECORD, Map.entry(EpochTransitionRecord::fromBytes, x -> ((EpochTransitionRecord) x).toBytes()))
                    .put(RETENTION_SET, Map.entry(RetentionSet::fromBytes, x -> ((RetentionSet) x).toBytes()))
                    .put(STREAM_CUT_RECORD, Map.entry(StreamCutRecord::fromBytes, x -> ((StreamCutRecord) x).toBytes()))
                    .put(EPOCH_RECORD, Map.entry(EpochRecord::fromBytes, x -> ((EpochRecord) x).toBytes()))
                    .put(HISTORY_TIME_SERIES, Map.entry(HistoryTimeSeries::fromBytes, x -> ((HistoryTimeSeries) x).toBytes()))
                    .put(SEALED_SEGMENTS_MAP_SHARD, Map.entry(SealedSegmentsMapShard::fromBytes, x -> ((SealedSegmentsMapShard) x).toBytes()))
                    .put(COMMITTING_TRANSACTIONS_RECORD, Map.entry(CommittingTransactionsRecord::fromBytes, x -> ((CommittingTransactionsRecord) x).toBytes()))
                    .put(STREAM_SUBSCRIBER, Map.entry(StreamSubscriber::fromBytes, x -> ((StreamSubscriber) x).toBytes()))
                    .put(SUBSCRIBERS, Map.entry(Subscribers::fromBytes, x -> ((Subscribers) x).toBytes()))
                    .put(WRITER_MARK, Map.entry(WriterMark::fromBytes, x -> ((WriterMark) x).toBytes()))
                    .put(ACTIVE_TXN_RECORD, Map.entry(ActiveTxnRecord::fromBytes, x -> ((ActiveTxnRecord) x).toBytes()))
                    .put(COMPLETED_TXN_RECORD, Map.entry(CompletedTxnRecord::fromBytes, x -> ((CompletedTxnRecord) x).toBytes()))
                    .build();

    private static final Map<String, Class<?>> METADATA_CLASSES =
            ImmutableMap.<String, Class<?>>builder()
                    .put(STRING, String.class)
                    .put(INTEGER, Integer.class)
                    .put(LONG, Long.class)
                    // The value returned in the EMPTY case is integer 0.
                    .put(EMPTY, Integer.class)
                    .put(STREAM_CONFIGURATION_RECORD, StreamConfigurationRecord.class)
                    .put(STREAM_TRUNCATION_RECORD, StreamTruncationRecord.class)
                    .put(STATE_RECORD, StateRecord.class)
                    .put(EPOCH_TRANSITION_RECORD, EpochTransitionRecord.class)
                    .put(RETENTION_SET, RetentionSet.class)
                    .put(STREAM_CUT_RECORD, StreamCutRecord.class)
                    .put(EPOCH_RECORD, EpochRecord.class)
                    .put(HISTORY_TIME_SERIES, HistoryTimeSeries.class)
                    .put(SEALED_SEGMENTS_MAP_SHARD, SealedSegmentsMapShard.class)
                    .put(COMMITTING_TRANSACTIONS_RECORD, CommittingTransactionsRecord.class)
                    .put(STREAM_SUBSCRIBER, StreamSubscriber.class)
                    .put(SUBSCRIBERS, Subscribers.class)
                    .put(WRITER_MARK, WriterMark.class)
                    .put(ACTIVE_TXN_RECORD, ActiveTxnRecord.class)
                    .put(COMPLETED_TXN_RECORD, CompletedTxnRecord.class)
                    .build();

    @Getter
    private final String metadataType;

    public ControllerMetadataSerializer(String tableName, String key) {
        metadataType = METADATA_TYPES.entrySet().stream()
                // Get the method to return the metadataType for the key, corresponding to given tableName.
                .filter(mapEntry -> mapEntry.getKey().apply(tableName))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("%s is not a valid controller table.", tableName)))
                .apply(key);
    }

    @Override
    public ByteBuffer serialize(Object value) {
        val serializer = BYTE_CONVERTERS.get(metadataType);
        if ( serializer == null ) {
            throw new IllegalStateException("No Serializer found");
        }
        return ByteBuffer.wrap(serializer.getValue().apply(value));
    }

    @Override
    public Object deserialize(ByteBuffer serializedValue) {
        val serializer = BYTE_CONVERTERS.get(metadataType);
        if ( serializer == null ) {
            throw new IllegalStateException("No Serializer found");
        }
        return serializer.getKey().apply(new ByteArraySegment(serializedValue).getCopy());
    }

    /**
     * Method to get the {@link Class} of the metadata associated to the serializer instance.
     *
     * @return The class of the metadata.
     */
    public Class<?> getMetadataClass() {
        return METADATA_CLASSES.get(metadataType);
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
}
