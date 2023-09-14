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
package io.pravega.shared;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Utility methods for StreamSegment Names.
 */
public final class NameUtils {
    //region Members

    // The prefix which will be used to name all internal streams.
    public static final String INTERNAL_NAME_PREFIX = "_";

    // The separator in controller metadata tables.
    public static final String SEPARATOR = ".#.";

    // The scope name which has to be used when creating internally used pravega streams.
    public static final String INTERNAL_SCOPE_NAME = "_system";

    // The Prefix which is used when creating internally used pravega streams.
    public static final String INTERNAL_SCOPE_PREFIX = INTERNAL_SCOPE_NAME + "/";

    // The prefix used for internal container segments.
    public static final String INTERNAL_CONTAINER_PREFIX = "_system/containers/";

    // The prefix which has to be appended to streams created internally for readerGroups.
    public static final String READER_GROUP_STREAM_PREFIX = INTERNAL_NAME_PREFIX + "RG";

    /**
     * Formatting for stream metadata tables.
     */
    public static final String METADATA_TABLE = "metadata" + SEPARATOR + "%s";

    /**
     * Formatting for EpochsWithTransactions metadata tables.
     */
    public static final String EPOCHS_WITH_TRANSACTIONS_TABLE = "epochsWithTransactions" + SEPARATOR + "%s";

    /**
     * Formatting for TransactionsInEpoch metadata tables.
     */
    public static final String TRANSACTIONS_IN_EPOCH_TABLE_FORMAT = "transactionsInEpoch-%s" + SEPARATOR + "%s";

    /**
     * Formatting for WriterPositions metadata tables.
     */
    public static final String WRITERS_POSITIONS_TABLE = "writersPositions" + SEPARATOR + "%s";

    /**
     * The table name for CompletedTransactionsBatches table.
     */
    public static final String COMPLETED_TRANSACTIONS_BATCHES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME,
            "completedTransactionsBatches");

    /**
     * The table name for CompletedTransactionsBatch tables.
     */
    public static final String COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT = "completedTransactionsBatch-%s";

    /**
     * The table name for the DeletedStreams table.
     */
    public static final String DELETED_STREAMS_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "deletedStreams");

    /**
     * Size of the prefix or suffix included with the user stream name.
     */
    public static final int MAX_PREFIX_OR_SUFFIX_SIZE = 5;

    /**
     * Size of the overall name as permitted by the host.
     */
    public static final int MAX_NAME_SIZE = 255;

    /**
     * Max event size for index segment appends in StreamSegments.
     */
    public static final int INDEX_APPEND_EVENT_SIZE = 24;

    /**
     * Size of the name that can be specified by user.
     */
    public static final int MAX_GIVEN_NAME_SIZE = MAX_NAME_SIZE - MAX_PREFIX_OR_SUFFIX_SIZE;

    /**
     * Controller Metadata keys.
     */
    public static final String CREATION_TIME_KEY = "creationTime";
    public static final String CONFIGURATION_KEY = "configuration";
    public static final String TRUNCATION_KEY = "truncation";
    public static final String STATE_KEY = "state";
    public static final String EPOCH_TRANSITION_KEY = "epochTransition";
    public static final String RETENTION_SET_KEY = "retention";
    public static final String RETENTION_STREAM_CUT_RECORD_KEY_FORMAT = "retentionCuts-%s"; // stream cut reference
    public static final String CURRENT_EPOCH_KEY = "currentEpochRecord";
    public static final String EPOCH_RECORD_KEY_FORMAT = "epochRecord-%s";
    public static final String HISTORY_TIMESERIES_CHUNK_FORMAT = "historyTimeSeriesChunk-%s";
    public static final String SEGMENTS_SEALED_SIZE_MAP_SHARD_FORMAT = "segmentsSealedSizeMapShard-%s";
    public static final String SEGMENT_SEALED_EPOCH_KEY_FORMAT = "segmentSealedEpochPath-%s"; // segment id
    public static final String COMMITTING_TRANSACTIONS_RECORD_KEY = "committingTxns";
    public static final String SEGMENT_MARKER_PATH_FORMAT = "markers-%d";
    public static final String WAITING_REQUEST_PROCESSOR_PATH = "waitingRequestProcessor";
    public static final String SUBSCRIBER_KEY_PREFIX = "subscriber_";
    public static final String SUBSCRIBER_SET_KEY = "subscriberset";

    /**
     * This is used for composing metric tags.
     */
    static final String TAG_SCOPE = "scope";
    static final String TAG_STREAM = "stream";
    static final String TAG_SEGMENT = "segment";
    static final String TAG_EPOCH = "epoch";
    static final String TAG_DEFAULT = "default";
    static final String TAG_WRITER = "writer";

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it stores its extended attributes.
     */
    private static final String ATTRIBUTE_SUFFIX = "$attributes.index";

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it stores its Rolling Storage Header.
     */
    private static final String HEADER_SUFFIX = "$header";

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it represents a SegmentChunk.
     */
    private static final String OFFSET_SUFFIX = "$offset.";

    /**
     * This is appended to the end of the Parent Segment Name, then we append a unique identifier.
     */
    private static final String TRANSACTION_DELIMITER = "#transaction.";

    /**
     * This is appended to the end of the Parent Segment Name, then we append a unique identifier.
     */
    private static final String TRANSIENT_DELIMITER = "#transient.";

    /**
     * This is appended to the end of the Primary Segment Name, followed by epoch.
     */
    private static final String EPOCH_DELIMITER = ".#epoch.";

    /**
     * Format for chunk name with segment name , epoch and offset.
     */
    private static final String CHUNK_NAME_FORMAT_WITH_EPOCH_OFFSET = "%s.E-%d-O-%d.%s";

    /**
     * Format for name of read index block index entry.
     */
    private static final String BLOCK_INDEX_NAME_FORMAT_WITH_OFFSET = "%s.B-%d";

    /**
     * Prefix for Container Metadata Segment name.
     */
    private static final String METADATA_SEGMENT_NAME_PREFIX = INTERNAL_CONTAINER_PREFIX + "metadata_";

    /**
     * Format for Container Metadata Segment name.
     */
    private static final String METADATA_SEGMENT_NAME_FORMAT = METADATA_SEGMENT_NAME_PREFIX + "%d";

    /**
     * Prefix for Storage Metadata Segment name.
     */
    private static final String STORAGE_METADATA_SEGMENT_NAME_PREFIX = INTERNAL_CONTAINER_PREFIX + "storage_metadata_";

    /**
     * Format for Storage Metadata Segment name.
     */
    private static final String STORAGE_METADATA_SEGMENT_NAME_FORMAT = STORAGE_METADATA_SEGMENT_NAME_PREFIX + "%d";

    /**
     * Format for Container System Journal file name.
     */
    private static final String SYSJOURNAL_NAME_FORMAT = INTERNAL_CONTAINER_PREFIX + "_sysjournal.epoch%d.container%d.file%d";

    /**
     * Format for Container System snapshot file name.
     */
    private static final String SYSJOURNAL_SNAPSHOT_NAME_FORMAT = INTERNAL_CONTAINER_PREFIX + "_sysjournal.epoch%d.container%d.snapshot%d";

    /**
     * Format for Container System snapshot file name.
     */
    private static final String SYSJOURNAL_SNAPSHOT_INFO_NAME_FORMAT = INTERNAL_CONTAINER_PREFIX + "_sysjournal.container%d.snapshot_info";

    /**
     * The Transaction unique identifier is made of two parts, each having a length of 16 bytes (64 bits in Hex).
     */
    private static final int TRANSACTION_PART_LENGTH = Long.BYTES * 8 / 4;

    /**
     * The Transient unique identifier is made of two parts, ecah having a length of 16 bytes (64 bits in Hex).
     */
    private static final int TRANSIENT_PART_LENGTH = TRANSACTION_PART_LENGTH;

    /**
     * The length of the Transaction unique identifier, in bytes (it is made of two parts).
     */
    private static final int TRANSACTION_ID_LENGTH = 2 * TRANSACTION_PART_LENGTH;

    /**
     * The length of the Transient Segments unique identifier, in bytes (it is made of two parts).
     */
    private static final int TRANSIENT_ID_LENGTH = 2 * TRANSIENT_PART_LENGTH;

    /**
     * Custom String format that converts a 64 bit integer into a hex number, with leading zeroes.
     */
    private static final String FULL_HEX_FORMAT = "%0" + TRANSACTION_PART_LENGTH + "x";

    /**
     * This is used in composing table names as `scope`/_tables
     */
    private static final String TABLES = "_tables";

    /**
     * This is used in composing segment name for a table segment used for a KeyValueTable
     */
    private static final String KVTABLE_SUFFIX = "_kvtable";

    /**
     * Prefix for identifying system created mark segments for storing watermarks.
     */
    @Getter(AccessLevel.PUBLIC)
    private static final String MARK_PREFIX = INTERNAL_NAME_PREFIX + "MARK";

    /**
     * Formatting for internal Segments used for ContainerEventProcessor.
     */
    private static final String CONTAINER_EVENT_PROCESSOR_SEGMENT_NAME = INTERNAL_CONTAINER_PREFIX + "event_processor_%s_%d";

    private static final String CONTAINER_EPOCH_INFO = INTERNAL_CONTAINER_PREFIX + "container_%d_epoch";

    /**
     * This is appended at the end of the Segment name to indicate it stores its index metadata.
     */
    private static final String INDEX_SEGMENT_SUFFIX = "#index";

    //endregion

    /**
     * Returns the transaction name for a TransactionStreamSegment based on the name of the current Parent StreamSegment, and the transactionId.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment for this transaction.
     * @param transactionId           The unique Id for the transaction.
     * @return The name of the Transaction StreamSegmentId.
     */
    public static String getTransactionNameFromId(String parentStreamSegmentName, UUID transactionId) {
        StringBuilder result = new StringBuilder();
        result.append(parentStreamSegmentName);
        result.append(TRANSACTION_DELIMITER);
        result.append(String.format(FULL_HEX_FORMAT, transactionId.getMostSignificantBits()));
        result.append(String.format(FULL_HEX_FORMAT, transactionId.getLeastSignificantBits()));
        return result.toString();
    }

    /**
     * Returns the transient name for a TransientSegment based on the name of the current Parent StreamSegment, and the transientId.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment for this transient segment.
     * @param writerId The Writer Id used to create the transient segment.
     * @return The name of the Transient StreamSegmentId.
     */
    public static String getTransientNameFromId(String parentStreamSegmentName, UUID writerId) {
        UUID random = UUID.randomUUID();
        StringBuilder result = new StringBuilder();
        result.append(parentStreamSegmentName);
        result.append(TRANSIENT_DELIMITER);
        result.append(String.format(FULL_HEX_FORMAT, writerId.getMostSignificantBits()));
        result.append(String.format(FULL_HEX_FORMAT, writerId.getLeastSignificantBits()));
        result.append('.');
        result.append(String.format(FULL_HEX_FORMAT, random.getMostSignificantBits()));
        result.append(String.format(FULL_HEX_FORMAT, random.getLeastSignificantBits()));
        return result.toString();
    }

    /**
     * Finds the position of a delimiter within a string and validates said string is of expected format.
     * @param streamSegmentName The name of the stream segment to validate.
     * @param delimiter The delimiter to check for.
     * @param idLength The length of the id.
     *
     * @return The start position of the delimiter contained within streamSegmentName.
     */
    private static int getDelimiterPosition(String streamSegmentName, String delimiter, int idLength) {
        int endOfStreamNamePos = streamSegmentName.lastIndexOf(delimiter);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + delimiter.length() + idLength > streamSegmentName.length()) {
            return -1;
        }
        return endOfStreamNamePos;
    }

    /**
     * Attempts to extract the name of the Parent StreamSegment for the given Transaction/Transient StreamSegment. This method returns a
     * valid value only if the Transaction/Transient StreamSegmentName was generated using the generateTransactionStreamSegmentName method.
     *
     * @param segmentName The name of the Transaction StreamSegment or Transient Segment to extract the name of the Parent StreamSegment.
     * @return The name of the Parent StreamSegment, or null if not a valid StreamSegment.
     */
    public static String getParentStreamSegmentName(String segmentName) {
        // Check to see if it is a valid Transaction.
        int endOfTransactionStream = getDelimiterPosition(segmentName, TRANSACTION_DELIMITER, TRANSACTION_ID_LENGTH);
        if (endOfTransactionStream >= 0) {
            return segmentName.substring(0, endOfTransactionStream);
        }
        // Check to see if it is a valid Transient Segment.
        int endOfTransientStream = getDelimiterPosition(segmentName, TRANSIENT_DELIMITER, TRANSIENT_ID_LENGTH);
        if (endOfTransientStream >= 0) {
            return segmentName.substring(0, endOfTransientStream);
        }
        return null;
    }

    /**
     * Checks if the given stream segment name is formatted for a Transaction Segment or regular segment.
     *
     * @param streamSegmentName The name of the StreamSegment to check for transaction delimiter.
     * @return true if stream segment name contains transaction delimiter, false otherwise.
     */
    public static boolean isTransactionSegment(String streamSegmentName) {
        // Check to see if the given name is a properly formatted Transaction.
        return getDelimiterPosition(streamSegmentName, TRANSACTION_DELIMITER, TRANSACTION_ID_LENGTH) >= 0;
    }

    /**
     * Checks if the given stream segment name is formatted for a Transient Segment or not.
     * @param streamSegmentName The name of the StreamSegment to check for the transient delimiter.
     * @return true if stream segment name contains transient delimiter, false otherwise.
     */
    public static boolean isTransientSegment(String streamSegmentName) {
        return getDelimiterPosition(streamSegmentName, TRANSIENT_DELIMITER, TRANSIENT_ID_LENGTH) >= 0;
    }

    /**
     * Attempts to extract the primary part of stream segment name before the epoch delimiter. This method returns a
     * valid value only if the StreamSegmentName was generated using the getQualifiedStreamSegmentName method.
     *
     * @param streamSegmentName The name of the StreamSegment to extract the name of the Primary StreamSegment name.
     * @return The primary part of StreamSegment.
     */
    public static String extractPrimaryStreamSegmentName(String streamSegmentName) {
        if (isTransactionSegment(streamSegmentName) || isTransientSegment(streamSegmentName)) {
            return extractPrimaryStreamSegmentName(getParentStreamSegmentName(streamSegmentName));
        }
        int endOfStreamNamePos = streamSegmentName.lastIndexOf(EPOCH_DELIMITER);
        if (endOfStreamNamePos < 0) {
            // epoch delimiter not present in the name, return the full name
            return streamSegmentName;
        }
        return streamSegmentName.substring(0, endOfStreamNamePos);
    }

    /**
     * Checks whether the given name is an Attribute Segment or not.
     *
     * @param segmentName   The name of the segment.
     * @return              True if the segment is an attribute Segment, false otherwise.
     */
    public static boolean isAttributeSegment(String segmentName) {
        return segmentName.endsWith(ATTRIBUTE_SUFFIX);
    }

    /**
     * Gets the name of the meta-Segment mapped to the given Segment Name that is responsible with storing extended attributes.
     *
     * @param segmentName The name of the Segment to get the Attribute segment name for.
     * @return The result.
     */
    public static String getAttributeSegmentName(String segmentName) {
        Preconditions.checkArgument(!isAttributeSegment(segmentName), "segmentName is already an attribute segment name");
        return segmentName + ATTRIBUTE_SUFFIX;
    }

    /**
     * Gets the name of the meta-Segment mapped to the given Segment Name that is responsible with storing its Rollover
     * information.
     * Existence of this file should also indicate that a Segment with this file has a rollover policy in place.
     *
     * @param segmentName The name of the Segment to get the Header segment name for.
     * @return The result.
     */
    public static String getHeaderSegmentName(String segmentName) {
        Preconditions.checkArgument(!segmentName.endsWith(HEADER_SUFFIX), "segmentName is already a segment header name");
        return segmentName + HEADER_SUFFIX;
    }

    /**
     * Checks whether given name is a Header Segment.
     *
     * @param segmentName The name of the segment.
     * @return true if the name is Header Segment. False otherwise
     */
    public static boolean isHeaderSegment(String segmentName) {
        return segmentName.endsWith(HEADER_SUFFIX);
    }

    /**
     * Gets the name of the Segment name from its Header Segment Name.
     *
     * @param headerSegmentName The name of the Header Segment.
     * @return The Segment Name.
     */
    public static String getSegmentNameFromHeader(String headerSegmentName) {
        Preconditions.checkArgument(headerSegmentName.endsWith(HEADER_SUFFIX));
        return headerSegmentName.substring(0, headerSegmentName.length() - HEADER_SUFFIX.length());
    }

    /**
     * Gets the name of the SegmentChunk for the given Segment and Offset.
     *
     * @param segmentName The name of the Segment to get the SegmentChunk name for.
     * @param offset      The starting offset of the SegmentChunk.
     * @return The SegmentChunk name.
     */
    public static String getSegmentChunkName(String segmentName, long offset) {
        Preconditions.checkArgument(!segmentName.contains(OFFSET_SUFFIX), "segmentName is already a SegmentChunk name");
        return segmentName + OFFSET_SUFFIX + offset;
    }

    /**
     * Gets the name of the SegmentChunk for the given segment, epoch and offset.
     *
     * @param segmentName The name of the Segment to get the SegmentChunk name for.
     * @param epoch       The epoch of the container.
     * @param offset      The starting offset of the SegmentChunk.
     * @return formatted chunk name.
     */
    public static String getSegmentChunkName(String segmentName, long epoch, long offset) {
        return String.format(CHUNK_NAME_FORMAT_WITH_EPOCH_OFFSET, segmentName, epoch, offset, UUID.randomUUID());
    }


    /**
     * Gets the name of the read index block entry for the given segment and offset.
     *
     * @param segmentName The name of the Segment.
     * @param offset      The starting offset of the block.
     * @return formatted read index block entry name.
     */
    public static String getSegmentReadIndexBlockName(String segmentName, long offset) {
        return String.format(BLOCK_INDEX_NAME_FORMAT_WITH_OFFSET, segmentName, offset);
    }

    /**
     * Checks whether given name is a Container Metadata Segment.
     *
     * @param segmentName The name of the segment.
     * @return true if the name is Container Metadata Segment. False otherwise
     */
    public static boolean isMetadataSegmentName(String segmentName) {
        return segmentName.startsWith(METADATA_SEGMENT_NAME_PREFIX);
    }

    /**
     * Gets the name of the Segment that is used to store the Container's Segment Metadata. There is one such Segment
     * per container.
     *
     * @param containerId The Id of the Container.
     * @return The Metadata Segment name.
     */
    public static String getMetadataSegmentName(int containerId) {
        Preconditions.checkArgument(containerId >= 0, "containerId must be a non-negative number.");
        return String.format(METADATA_SEGMENT_NAME_FORMAT, containerId);
    }

    /**
     * Checks whether given name is a Storage Metadata Segment.
     *
     * @param segmentName The name of the segment.
     * @return true if the name is Storage Metadata Segment. False otherwise
     */
    public static boolean isStorageMetadataSegmentName(String segmentName) {
        return segmentName.startsWith(STORAGE_METADATA_SEGMENT_NAME_PREFIX);
    }

    /**
     * Gets the name of the Segment that is used to store the Container's Segment Metadata. There is one such Segment
     * per container.
     *
     * @param containerId The Id of the Container.
     * @return The Metadata Segment name.
     */
    public static String getStorageMetadataSegmentName(int containerId) {
        Preconditions.checkArgument(containerId >= 0, "containerId must be a non-negative number.");
        return String.format(STORAGE_METADATA_SEGMENT_NAME_FORMAT, containerId);
    }

    /**
     * Gets file name of SystemJournal for given container instance.
     * @param containerId The Id of the Container.
     * @param epoch Epoch of the container instance.
     * @param currentFileIndex Current index for journal file.
     * @return File name of SystemJournal for given container instance
     */
    public static String getSystemJournalFileName(int containerId, long epoch, long currentFileIndex) {
        return String.format(SYSJOURNAL_NAME_FORMAT, epoch, containerId, currentFileIndex);
    }

    /**
     * Gets file name of SystemJournal snapshot for given container instance.
     * @param containerId The Id of the Container.
     * @param epoch Epoch of the container instance.
     * @param currentSnapshotIndex Current index for journal file.
     * @return File name of SystemJournal for given container instance
     */
    public static String getSystemJournalSnapshotFileName(int containerId, long epoch, long currentSnapshotIndex) {
        return String.format(SYSJOURNAL_SNAPSHOT_NAME_FORMAT, epoch, containerId, currentSnapshotIndex);
    }

    /**
     * Gets file name of ContainerEpochInfo for the given container instance.
     * @param containerId The Id of the Container.
     * @return File name of ContainerEpochInfo for given container instance
     */
    public static String getContainerEpochFileName(int containerId) {
        return String.format(CONTAINER_EPOCH_INFO, containerId);
    }
  
    /**
     * Gets file name of SystemJournal snapshot info file for given container instance.
     * @param containerId The Id of the Container.
     * @return File name of SystemJournal for given container instance
     */
    public static String getSystemJournalSnapshotInfoFileName(int containerId) {
        return String.format(SYSJOURNAL_SNAPSHOT_INFO_NAME_FORMAT, containerId);
    }

    /**
     * Method to compute 64 bit segment id which takes segment number and epoch and composes it as
     * `msb = epoch` `lsb = segmentNumber`.
     * Primary id identifies the segment container mapping and primary + secondary uniquely identifies a segment
     * within a stream.
     *
     * @param segmentNumber segment number.
     * @param epoch epoch in which segment was created.
     * @return segment id which is composed using segment number and epoch.
     */
    public static long computeSegmentId(int segmentNumber, int epoch) {
        Preconditions.checkArgument(segmentNumber >= 0);
        Preconditions.checkArgument(epoch >= 0);
        return (long) epoch << 32 | (segmentNumber & 0xFFFFFFFFL);
    }

    /**
     * Method to extract segmentNumber from given segment id. Segment number is encoded in 32 msb of segment id
     *
     * @param segmentId segment id.
     * @return segment number by extracting it from segment id.
     */
    public static int getSegmentNumber(long segmentId) {
        return (int) segmentId;
    }

    /**
     * Method to extract epoch from given segment id. Epoch is encoded in 32 lsb of the segment id.
     *
     * @param segmentId segment id.
     * @return epoch by extracting it from segment id.
     */
    public static int getEpoch(long segmentId) {
        return (int) (segmentId >> 32);
    }

    /**
     * Method to extract epoch from given transaction id.
     *
     * @param txnId Unique id of the transaction.
     * @return epoch by extracting it the transaction id.
     */
    public static int getEpoch(UUID txnId) {
        return (int) (txnId.getMostSignificantBits() >> 32);
    }

    /**
     * Compose and return scoped stream name.
     *
     * @param scope scope to be used in ScopedStream name.
     * @param streamName stream name to be used in ScopedStream name.
     * @return scoped stream name.
     */
    public static String getScopedStreamName(String scope, String streamName) {
        return getScopedStreamNameInternal(scope, streamName).toString();
    }

    /**
     * Compose and return scoped Key-Value Table name.
     *
     * @param scope scope to be used in scoped Key-Value Table name.
     * @param streamName Key-Value Table name to be used in Scoped Key-Value Table name.
     * @return scoped Key-Value Table name.
     */
    public static String getScopedKeyValueTableName(String scope, String streamName) {
        return getScopedStreamNameInternal(scope, streamName).toString();
    }

    /**
     * Compose and return scoped ReaderGroup name.
     *
     * @param scope scope to be used in ScopedReaderGroup name.
     * @param rgName ReaderGroup name to be used in ScopedReaderGroup name.
     * @return scoped stream name.
     */
    public static String getScopedReaderGroupName(String scope, String rgName) {
        return getScopedStreamNameInternal(scope, rgName).toString();
    }

    /**
     * Method to generate Fully Qualified TableSegmentName using scope, stream and segment id.
     *
     * @param scope scope to be used in the ScopedTableSegment name
     * @param kvTableName kvTable name to be used in ScopedTableSegment name.
     * @param segmentId segment id to be used in ScopedStreamSegment name.
     * @return fully qualified TableSegmentName for a TableSegment that is part of the KeyValueTable.
     */
    public static String getQualifiedTableSegmentName(String scope, String kvTableName, long segmentId) {
        int segmentNumber = getSegmentNumber(segmentId);
        int epoch = getEpoch(segmentId);
        StringBuilder sb = getScopedStreamNameInternal(scope, kvTableName + KVTABLE_SUFFIX);
        sb.append('/');
        sb.append(segmentNumber);
        sb.append(EPOCH_DELIMITER);
        sb.append(epoch);
        return sb.toString();
    }

    /**
     * Returns a list representing the components of a scoped Stream/Key-Value Table name.
     *
     * @param scopedName The scoped name.
     * @return A list containing the components. If the scoped name was properly formatted, this list should have
     * two elements: element index 0 has the scope name and element index 1 has the stream name.
     * @throws IllegalStateException If 'scopedName' is not in the form 'scope/name`.
     */
    public static List<String> extractScopedNameTokens(String scopedName) {
        String[] tokens = scopedName.split("/");
        Preconditions.checkArgument(tokens.length == 2, "Unexpected format for '%s'. Expected format '<scope-name>/<name>'.", scopedName);
        return Arrays.asList(tokens);
    }

    /**
     * Method to generate Fully Qualified StreamSegmentName using scope, stream and segment id.
     *
     * @param scope scope to be used in the ScopedStreamSegment name
     * @param streamName stream name to be used in ScopedStreamSegment name.
     * @param segmentId segment id to be used in ScopedStreamSegment name.
     * @return fully qualified StreamSegmentName.
     */
    public static String getQualifiedStreamSegmentName(String scope, String streamName, long segmentId) {
        int segmentNumber = getSegmentNumber(segmentId);
        int epoch = getEpoch(segmentId);
        StringBuilder sb = getScopedStreamNameInternal(scope, streamName);
        sb.append('/');
        sb.append(segmentNumber);
        sb.append(EPOCH_DELIMITER);
        sb.append(epoch);
        return sb.toString();
    }

    /**
     * Method to extract different parts of stream segment name.
     * The tokens extracted are in following order scope, stream name and segment id.
     * If its a transational segment, the transaction id is ignored.
     * This function works even when scope is not set.
     *
     * @param qualifiedName StreamSegment's qualified name.
     * @return tokens capturing different components of stream segment name. Note: segmentId is extracted and sent back
     * as a String
     */
    public static List<String> extractSegmentTokens(String qualifiedName) {
        Preconditions.checkNotNull(qualifiedName);
        String originalSegmentName = isTransactionSegment(qualifiedName) ? getParentStreamSegmentName(qualifiedName) : qualifiedName;

        List<String> retVal = new LinkedList<>();
        String[] tokens = originalSegmentName.split("/");
        int segmentIdIndex = tokens.length == 2 ? 1 : 2;
        long segmentId;
        if (tokens[segmentIdIndex].contains(EPOCH_DELIMITER)) {
            String[] segmentIdTokens = tokens[segmentIdIndex].split(EPOCH_DELIMITER);
            segmentId = computeSegmentId(Integer.parseInt(segmentIdTokens[0]), Integer.parseInt(segmentIdTokens[1]));
        } else {
            // no secondary delimiter, set the secondary id to 0 for segment id computation
            segmentId = computeSegmentId(Integer.parseInt(tokens[segmentIdIndex]), 0);
        }
        retVal.add(tokens[0]);
        if (tokens.length == 3) {
            retVal.add(tokens[1]);
        }
        retVal.add(Long.toString(segmentId));

        return retVal;
    }

    private static StringBuilder getScopedStreamNameInternal(String scope, String streamName) {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(scope)) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        return sb;
    }

    // region table names

    /**
     * Method to generate Fully Qualified table name using scope, and other tokens to be used to compose the table name.
     * The composed name has following format: {@literal <scope>/_tables/<tokens[0]>/<tokens[1]>...}
     *
     * @param scope scope in which table segment to create
     * @param tokens tokens used for composing table segment name
     * @return Fully qualified table segment name composed of supplied tokens.
     */
    public static String getQualifiedTableName(String scope, String... tokens) {
        Preconditions.checkArgument(tokens != null && tokens.length > 0);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s/%s", scope, TABLES));
        for (String token : tokens) {
            sb.append('/');
            sb.append(token);
        }
        return sb.toString();
    }

    /**
     * Method to extract tokens that were used to compose fully qualified table segment name using method getQualifiedTableName.
     *
     * The first token in the returned list corresponds to scope. Remainder tokens correspond to tokens used to compose tableName.
     *
     * @param qualifiedName fully qualified table name
     * @return tokens capturing different components of table segment name. First element in the list represents scope
     */
    public static List<String> extractTableSegmentTokens(String qualifiedName) {
        Preconditions.checkNotNull(qualifiedName);
        List<String> retVal = new LinkedList<>();
        String[] tokens = qualifiedName.split("/");
        Preconditions.checkArgument(tokens.length > 2);
        Preconditions.checkArgument(tokens[1].equals(TABLES));
        // add scope
        retVal.add(tokens[0]);
        for (int i = 2; i < tokens.length; i++) {
            retVal.add(tokens[i]);
        }

        return retVal;
    }

    /**
     * Method to check if given segment name is a table name generated using getQualifiedTableName.
     * @param qualifiedName qualified table name
     * @return true if the name is generated using getQualifiedTableName. False otherwise
     */
    public static boolean isTableSegment(String qualifiedName) {
        Preconditions.checkNotNull(qualifiedName);
        String[] tokens = qualifiedName.split("/");
        Preconditions.checkArgument(tokens.length > 2);

        return tokens[1].equals(TABLES);
    }
    // endregion

    // region metrics
    /**
     * Generate segment tags (string array) on the input fully qualified segment name to be associated with a metric.
     *
     * @param qualifiedSegmentName Fully qualified segment name.
     * @return String array as segment tag of metric.
     */
    public static String[] segmentTags(String qualifiedSegmentName) {
        Preconditions.checkNotNull(qualifiedSegmentName);
        String[] tags = {TAG_SCOPE, null, TAG_STREAM, null, TAG_SEGMENT, null, TAG_EPOCH, null};

        return updateSegmentTags(qualifiedSegmentName, tags);
    }

    /**
     * Generate segment tags (string array) on the input fully qualified segment name and writer id to be associated with a metric.
     * @param qualifiedSegmentName Fully qualified segment name.
     * @param writerId The writer id.
     * @return String arrays as a segment tag of metric.
     */
    public static String[] segmentTags(String qualifiedSegmentName, String writerId) {
        Preconditions.checkNotNull(qualifiedSegmentName);
        Exceptions.checkNotNullOrEmpty(writerId, "writerId");
        String[] tags = {TAG_SCOPE, null, TAG_STREAM, null, TAG_SEGMENT, null, TAG_EPOCH, null, TAG_WRITER, null};

        updateSegmentTags(qualifiedSegmentName, tags);
        tags[9] = writerId; // update the writer id tag.
        return tags;
    }

    private static String[] updateSegmentTags(String qualifiedSegmentName, String[] tags) {
        String segmentBaseName = getSegmentBaseName(qualifiedSegmentName);
        String[] tokens = segmentBaseName.split("/");

        int segmentIdIndex = (tokens.length == 1) ? 0 : (tokens.length) == 2 ? 1 : 2;
        if (tokens[segmentIdIndex].contains(EPOCH_DELIMITER)) {
            String[] segmentIdTokens = tokens[segmentIdIndex].split(EPOCH_DELIMITER);
            tags[5] = segmentIdTokens[0];
            tags[7] = segmentIdTokens[1];
        } else {
            tags[5] = tokens[segmentIdIndex];
            tags[7] = "0";
        }
        if (tokens.length == 3) {
            tags[1] = tokens[0];
            tags[3] = tokens[1];
        } else if (tokens.length == 1) {
            tags[1] = TAG_DEFAULT;
            tags[3] = TAG_DEFAULT;
        } else {
            tags[1] = TAG_DEFAULT;
            tags[3] = tokens[0];
        }
        return tags;
    }

    /**
     * Generate writer tags (string array) based on the writerId.
     *
     * @param writerId Writer id.
     * @return String array as writer tag of metric.
     */
    public static String[] writerTags(String writerId) {
        Exceptions.checkNotNullOrEmpty(writerId, "writerId");
        return new String[]{TAG_WRITER, writerId};
    }

    /**
     * Get base name of segment with the potential transaction delimiter removed.
     *
     * @param segmentQualifiedName fully qualified segment name.
     * @return the base name of segment.
     */
    private static String getSegmentBaseName(String segmentQualifiedName) {
        String segmentBaseName = NameUtils.getParentStreamSegmentName(segmentQualifiedName);
        return (segmentBaseName == null) ? segmentQualifiedName : segmentBaseName;
    }

    /**
     * Get the name for this EventProcessor internal Segment.
     *
     * @param containerId    Id of the container where this Segment lives.
     * @param processorName  Name of the EventProcessor.
     * @return The name for the internal Segment used by an EventProcessor.
     */
    public static String getEventProcessorSegmentName(int containerId, String processorName) {
        return String.format(CONTAINER_EVENT_PROCESSOR_SEGMENT_NAME, processorName, containerId);
    }

    // endregion

    /**
     * Construct an internal representation of stream name. This is required to distinguish between user created
     * and Pravega internally created streams.
     *
     * @param streamName    The stream name for which we need to construct an internal name.
     * @return              The stream name which has to be used internally in the Pravega system.
     */
    public static String getInternalNameForStream(String streamName) {
        return INTERNAL_NAME_PREFIX + streamName;
    }

    /**
     * Construct a stream name which will internally be used by the readergroup implementation.
     *
     * @param groupNameName The readergroup name for which we need to construct an internal stream name.
     * @return              The stream name which has to be used internally by the reader group implementation.
     */
    public static String getStreamForReaderGroup(String groupNameName) {
        return READER_GROUP_STREAM_PREFIX + groupNameName;
    }

    /**
     * Validates a user created stream name.
     *
     * @param name User supplied stream name to validate.
     * @return The name in the case is valid.
     */
    public static String validateUserStreamName(String name) {
        Preconditions.checkNotNull(name);
        Preconditions.checkArgument(name.length() <= MAX_GIVEN_NAME_SIZE, "Name cannot exceed %s characters", MAX_GIVEN_NAME_SIZE);
        Preconditions.checkArgument(name.matches("[\\p{Alnum}\\.\\-]+"), "Name must be a-z, 0-9, ., -.");
        return name;
    }

    /**
     * Validates a user created stream segment.
     *
     * @param streamSegmentName User supplied stream segment name to validate.
     * @return True, if its a user created stream segment.
     */
    public static boolean isUserStreamSegment(String streamSegmentName) {
        try {
            if (isTableSegment(streamSegmentName) || isTransactionSegment(streamSegmentName) || isTransientSegment(streamSegmentName)
                    || isIndexSegment(streamSegmentName)) {
                return false;
            }
            validateUserStreamName(getStreamName(streamSegmentName));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String getStreamName(String streamSegment) {
        return streamSegment.split("/")[1];
    }

    /**
     * Validates a user-created Key-Value Table name.
     * @param name User supplied Key-Value Table name to validate.
     * @return The name, if valid.
     */
    public static String validateUserKeyValueTableName(String name) {
        return validateUserStreamName(name); // Currently, the same rules apply as for Streams.
    }

    /**
     * Validates an internal stream name.
     *
     * @param name Stream name to validate.
     * @return The name in the case is valid.
     */
    public static String validateStreamName(String name) {
        Preconditions.checkNotNull(name);

        // In addition to user stream names, pravega internally created stream have a special prefix.
        final String matcher = "[" + INTERNAL_NAME_PREFIX + "]?[\\p{Alnum}\\.\\-]+";
        Preconditions.checkArgument(name.length() <= MAX_NAME_SIZE, "Name cannot exceed %s characters", MAX_NAME_SIZE);
        Preconditions.checkArgument(name.matches(matcher), "Name must be " + matcher);
        return name;
    }

    /**
     * Validates a user created scope name.
     *
     * @param name Scope name to validate.
     * @return The name in the case is valid.
     */
    public static String validateUserScopeName(String name) {
        Preconditions.checkNotNull(name);
        Preconditions.checkArgument(name.length() <= MAX_NAME_SIZE, "Name cannot exceed %s characters", MAX_NAME_SIZE);
        Preconditions.checkArgument(name.matches("[\\p{Alnum}\\.\\-]+"), "Name must be a-z, 0-9, ., -.");
        return name;
    }

    /**
     * Validates a scope name.
     *
     * @param name Scope name to validate.
     * @return The name in the case is valid.
     */
    public static String validateScopeName(String name) {
        return validateStreamName(name);
    }

    /**
     * Validates a reader group name.
     *
     * @param name Reader group name to validate.
     * @return The name in the case is valid.
     */
    public static String validateReaderGroupName(String name) {
        return validateUserStreamName(name);
    }

    /**
     * Validates a readerId.
     *
     * @param readerId ReaderId to validate.
     * @return The name in the case is valid.
     */
    public static String validateReaderId(String readerId) {
        return validateUserStreamName(readerId);
    }

    /**
     * Validates a writerId.
     *
     * @param writerId ReaderId to validate.
     * @return The name in the case is valid.
     */
    public static String validateWriterId(String writerId) {
        return validateUserStreamName(writerId);
    }

    // region watermark
    public static String getMarkStreamForStream(String stream) {
        StringBuilder sb = new StringBuilder();
        sb.append(MARK_PREFIX);
        sb.append(stream);
        return sb.toString();
    }

    /**
     * Parse the connection name to fetch the connection details.
     *
     * @param connection String with connection information.
     * @return String array with endpoint and port details.
     */
    public static String[] getConnectionDetails(String connection) {
        Preconditions.checkNotNull(connection);
        return connection.split(":");
    }

    /**
     * Checks whether the given name is an Index Segment or not.
     *
     * @param segmentName   The name of the segment.
     * @return              True if the segment is an index Segment, false otherwise.
     */
    public static boolean isIndexSegment(String segmentName) {
        return segmentName.endsWith(INDEX_SEGMENT_SUFFIX);
    }

    /**
     * Gets the name of the index-Segment mapped to the given Segment Name that is responsible with storing index metadata.
     *
     * @param segmentName The name of the Segment to get the index segment name for.
     * @return The result.
     */
    public static String getIndexSegmentName(String segmentName) {
        Preconditions.checkArgument(!isIndexSegment(segmentName), "segmentName is already an index segment name");
        return segmentName + INDEX_SEGMENT_SUFFIX;
    }
}
