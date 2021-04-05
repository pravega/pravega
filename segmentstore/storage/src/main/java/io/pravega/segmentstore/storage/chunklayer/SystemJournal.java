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
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.shared.NameUtils;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.concurrent.GuardedBy;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;

/**
 * This class implements system journaling functionality for critical storage system segments which is useful for bootstrap after failover.
 * It records any layout changes to storage system segments.
 * Storage system segments are the segments that the storage subsystem uses to store all metadata.
 * This creates a circular dependency while reading or writing the data about these segments from the metadata segments.
 * System journal is a mechanism to break this circular dependency by having independent log of all layout changes to system segments.
 * During bootstrap all the system journal files are read and processed to re-create the state of the storage system segments.
 * Currently only two actions are considered viz. Addition of new chunks {@link SystemJournal.ChunkAddedRecord} and truncation of segments
 * {@link SystemJournal.TruncationRecord}.
 * In addition to these two records, log also contains system snapshot records {@link SystemJournal.SystemSnapshotRecord} which contains the state
 * of each storage system segments ({@link SystemJournal.SegmentSnapshotRecord}) after replaying all available logs at the time of snapshots.
 * These snapshot records help avoid replaying entire log evey time. Each container instance records snapshot immediately after bootstrap.
 * To avoid data corruption, each instance writes to its own distinct log file/object.
 * The bootstrap algorithm also correctly ignores invalid log entries written by running instance which is no longer owner of the given container.
 * To prevent applying partial changes resulting from unexpected crash, the log records are written as {@link SystemJournal.SystemJournalRecordBatch}.
 * In such cases either a full batch is read and applied completely or no records in the batch are applied.
 */
@Slf4j
public class SystemJournal {
    /**
     * Serializer for {@link SystemJournal.SystemJournalRecordBatch}.
     */
    private static final SystemJournalRecordBatch.SystemJournalRecordBatchSerializer BATCH_SERIALIZER = new SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();

    /**
     * Serializer for {@link SystemJournal.SystemSnapshotRecord}.
     */
    private static final SystemSnapshotRecord.Serializer SYSTEM_SNAPSHOT_SERIALIZER = new SystemSnapshotRecord.Serializer();

    private final Object lock = new Object();

    @Getter
    private final ChunkStorage chunkStorage;

    @Getter
    private final ChunkMetadataStore metadataStore;

    /**
     * Epoch of the current instance.
     */
    @Getter
    private volatile long epoch;

    /**
     * Container id of the owner container.
     */
    @Getter
    private final int containerId;

    /**
     * Index of current journal file.
     */
    @Getter
    @GuardedBy("lock")
    private int currentFileIndex;

    @GuardedBy("lock")
    private boolean newChunkRequired;

    /**
     * String prefix for all system segments.
     */
    @Getter
    private final String systemSegmentsPrefix;

    /**
     * System segments to track.
     */
    @Getter
    private final String[] systemSegments;

    /**
     * Offset at which next log will be written.
     */
    @GuardedBy("lock")
    private volatile long systemJournalOffset;

    /**
     * Handle to current journal file.
     */
    @GuardedBy("lock")
    private volatile ChunkHandle currentHandle;

    /**
     * Configuration {@link ChunkedSegmentStorageConfig} for the {@link ChunkedSegmentStorage}.
     */
    @Getter
    private final ChunkedSegmentStorageConfig config;

    private final AtomicBoolean reentryGuard = new AtomicBoolean();

    /**
     * Constructs an instance of {@link SystemJournal}.
     *
     * @param containerId   Container id of the owner container.
     * @param chunkStorage  ChunkStorage instance to use for writing all logs.
     * @param metadataStore ChunkMetadataStore for owner container.
     * @param config        Configuration options for this ChunkedSegmentStorage instance.
     */
    public SystemJournal(int containerId, ChunkStorage chunkStorage, ChunkMetadataStore metadataStore, ChunkedSegmentStorageConfig config) {
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.config = Preconditions.checkNotNull(config, "config");
        this.containerId = containerId;
        this.systemSegments = getChunkStorageSystemSegments(containerId);
        this.systemSegmentsPrefix = NameUtils.INTERNAL_SCOPE_NAME;
    }

    /**
     * Initializes this instance.
     *
     * @throws Exception Exception if any.
     */
    public void initialize() throws Exception {
        if (chunkStorage.supportsAppend()) {
            chunkStorage.create(getSystemJournalChunkName()).get();
        }
    }

    /**
     * Bootstrap the metadata about storage metadata segments by reading and processing the journal.
     *
     * @param epoch      Epoch of the current container instance.
     * @throws Exception Exception in case of any error.
     */
    public CompletableFuture<Void> bootstrap(long epoch) throws Exception {
        this.epoch = epoch;
        Preconditions.checkState(!reentryGuard.getAndSet(true), "bootstrap called multiple times.");
        try (val txn = metadataStore.beginTransaction(false, getSystemSegments())) {
            // Keep track of offsets at which chunks were added to the system segments.
            val chunkStartOffsets = new HashMap<String, Long>();

            // Keep track of offsets at which system segments were truncated.
            // We don't need to apply each truncate operation, only need to apply the final truncate offset.
            val finalTruncateOffsets = new HashMap<String, Long>();
            val finalFirstChunkStartsAtOffsets = new HashMap<String, Long>();

            // Step 1: Create metadata records for system segments from latest snapshot.
            val epochToStart = applyLatestSnapshot(txn, chunkStartOffsets);

            // Step 2: For each epoch, find the corresponding system journal files, process them and apply operations recorded.
            applySystemLogOperations(txn, epochToStart, chunkStartOffsets, finalTruncateOffsets, finalFirstChunkStartsAtOffsets);

            // Step 3: Adjust the length of the last chunk.
            adjustLastChunkLengths(txn);

            // Step 4: Apply the truncate offsets.
            applyFinalTruncateOffsets(txn, finalTruncateOffsets, finalFirstChunkStartsAtOffsets);

            // Step 5: Validate and save a snapshot.
            validateAndSaveSnapshot(txn);

            // Step 5: Finally commit all data.
            return txn.commit(true, true);
        }
    }

    /**
     * Commits a given system log record to the underlying log chunk.
     *
     * @param record Record to persist.
     * @throws ChunkStorageException Exception if any.
     * @throws ExecutionException Exception in case of any error.
     * @throws InterruptedException Exception in case of any error.
     */
    public void commitRecord(SystemJournalRecord record) throws ChunkStorageException, ExecutionException, InterruptedException {
        Preconditions.checkArgument(null != record, "record must not be null");
        commitRecords(Collections.singletonList(record));
    }

    /**
     * Commits a given list of system log records to the underlying log chunk.
     *
     * @param records List of records to log to.
     * @throws ChunkStorageException Exception in case of any error.
     * @throws ExecutionException Exception in case of any error.
     * @throws InterruptedException Exception in case of any error.
     */
    public void commitRecords(Collection<SystemJournalRecord> records) throws ChunkStorageException, ExecutionException, InterruptedException {
        Preconditions.checkArgument(null != records, "records must not be null");
        Preconditions.checkArgument(records.size() > 0, "records must not be empty");

        SystemJournalRecordBatch batch = SystemJournalRecordBatch.builder().systemJournalRecords(records).build();
        ByteArraySegment bytes;
        try {
            bytes = BATCH_SERIALIZER.serialize(batch);
        } catch (IOException e) {
            throw new ChunkStorageException(getSystemJournalChunkName(), "Unable to serialize", e);
        }
        // Persist
        synchronized (lock) {
            boolean done = false;
            while (!done) {
                try {
                    writeToJournal(bytes);
                    done = true;
                } catch (ExecutionException e) {
                    val ex = Exceptions.unwrap(e);
                    // In case of partial write during previous failure, this time we'll get InvalidOffsetException.
                    // In that case we start a new journal file and retry.
                    if (!(ex instanceof InvalidOffsetException)) {
                        throw e;
                    }
                }
                // Add a new log file if required.
                if (!chunkStorage.supportsAppend() || !config.isAppendEnabled() || !done) {
                    newChunkRequired = true;
                }
            }
        }
        log.debug("SystemJournal[{}] Logging system log records - file={}, batch={}.", containerId, currentHandle.getChunkName(), batch);
    }

    /**
     * Find and apply latest snapshot.
     */
    private long applyLatestSnapshot(MetadataTransaction txn, HashMap<String, Long> chunkStartOffsets) throws Exception {
        long epochToCheck;
        String snapshotFile;
        boolean found = false;

        // Find latest epoch with snapshot.
        for (epochToCheck = epoch - 1; epochToCheck >= 0; epochToCheck--) {
            snapshotFile = getSystemJournalChunkName(containerId, epochToCheck, 0);
            if (chunkStorage.exists(snapshotFile).get()) {
                SystemSnapshotRecord systemSnapshot = null;
                try {
                    // Read contents.
                    byte[] contents = getContents(snapshotFile);
                    systemSnapshot = SYSTEM_SNAPSHOT_SERIALIZER.deserialize(contents);
                } catch (EOFException e) {
                    log.warn("SystemJournal[{}] Incomplete snapshot found, skipping {}.", containerId, snapshotFile);
                }
                if (null != systemSnapshot) {
                    log.debug("SystemJournal[{}] Processing system log snapshot {}.", containerId, systemSnapshot);
                    // Initialize the segments and their chunks.
                    for (SegmentSnapshotRecord segmentSnapshot : systemSnapshot.segmentSnapshotRecords) {
                        // Update segment data.
                        segmentSnapshot.segmentMetadata.setActive(true)
                                .setOwnershipChanged(true)
                                .setStorageSystemSegment(true);
                        segmentSnapshot.segmentMetadata.setOwnerEpoch(epoch);

                        // Add segment data.
                        txn.create(segmentSnapshot.segmentMetadata);

                        // make sure that the record is marked pinned.
                        txn.markPinned(segmentSnapshot.segmentMetadata);

                        // Add chunk metadata and keep track of start offsets for each chunk.
                        long offset = segmentSnapshot.segmentMetadata.getFirstChunkStartOffset();
                        for (ChunkMetadata metadata : segmentSnapshot.chunkMetadataCollection) {
                            txn.create(metadata);

                            // make sure that the record is marked pinned.
                            txn.markPinned(metadata);

                            chunkStartOffsets.put(metadata.getName(), offset);
                            offset += metadata.getLength();
                        }
                        found = true;
                    }
                    break;
                }
            }
        }
        if (!found) {
            for (String systemSegment : systemSegments) {
                SegmentMetadata segmentMetadata = SegmentMetadata.builder()
                        .name(systemSegment)
                        .ownerEpoch(epoch)
                        .maxRollinglength(config.getDefaultRollingPolicy().getMaxLength())
                        .build();
                segmentMetadata.setActive(true)
                        .setOwnershipChanged(true)
                        .setStorageSystemSegment(true);
                segmentMetadata.checkInvariants();
                txn.create(segmentMetadata);
                txn.markPinned(segmentMetadata);
            }
        }
        return epochToCheck;
    }

    /**
     * Read contents from file.
     */
    private byte[] getContents(String snapshotFile) throws ExecutionException, InterruptedException {
        val info = chunkStorage.getInfo(snapshotFile).get();
        val h = ChunkHandle.readHandle(snapshotFile);
        byte[] contents = new byte[Math.toIntExact(info.getLength())];
        long fromOffset = 0;
        int remaining = contents.length;
        while (remaining > 0) {
            int bytesRead = chunkStorage.read(h, fromOffset, remaining, contents, Math.toIntExact(fromOffset)).get();
            remaining -= bytesRead;
            fromOffset += bytesRead;
        }
        return contents;
    }

    /**
     * Process all systemLog entries to recreate the state of metadata storage system segments.
     */
    private void applySystemLogOperations(MetadataTransaction txn,
                                          long epochToStartScanning,
                                          HashMap<String, Long> chunkStartOffsets,
                                          HashMap<String, Long> finalTruncateOffsets,
                                          HashMap<String, Long> finalFirstChunkStartsAtOffsets) throws Exception {
        for (long epochToRecover = epochToStartScanning; epochToRecover < epoch; epochToRecover++) {
            // Start scan with file index 1.
            int fileIndexToRecover = 1;
            while (chunkStorage.exists(getSystemJournalChunkName(containerId, epochToRecover, fileIndexToRecover)).get()) {
                // Read contents.
                val systemLogName = getSystemJournalChunkName(containerId, epochToRecover, fileIndexToRecover);
                byte[] contents = getContents(systemLogName);
                val input = new ByteArrayInputStream(contents);

                // Apply record batches from the file.
                // Loop is exited with eventual EOFException.
                while (true) {
                    try {
                        val batch = BATCH_SERIALIZER.deserialize(input);
                        if (null != batch.getSystemJournalRecords()) {
                            for (val record : batch.getSystemJournalRecords()) {
                                log.debug("SystemJournal[{}] Processing system log record ={}.", epoch, record);
                                // ChunkAddedRecord.
                                if (record instanceof ChunkAddedRecord) {
                                    val chunkAddedRecord = (ChunkAddedRecord) record;
                                    applyChunkAddition(txn, chunkStartOffsets,
                                            chunkAddedRecord.getSegmentName(),
                                            nullToEmpty(chunkAddedRecord.getOldChunkName()),
                                            chunkAddedRecord.getNewChunkName(),
                                            chunkAddedRecord.getOffset());
                                }

                                // TruncationRecord.
                                if (record instanceof TruncationRecord) {
                                    val truncationRecord = (TruncationRecord) record;
                                    finalTruncateOffsets.put(truncationRecord.getSegmentName(), truncationRecord.getOffset());
                                    finalFirstChunkStartsAtOffsets.put(truncationRecord.getSegmentName(), truncationRecord.getStartOffset());
                                }
                            }
                        }
                    } catch (EOFException e) {
                        log.debug("SystemJournal[{}] Done processing file {}.", containerId, systemLogName);
                        break;
                    }
                }
                // Move to next file.
                fileIndexToRecover++;
            }
        }
    }

    /**
     * Adjusts the lengths of last chunks for each segment.
     */
    private void adjustLastChunkLengths(MetadataTransaction txn) throws Exception {
        for (String systemSegment : systemSegments) {
            SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(systemSegment).get();
            segmentMetadata.checkInvariants();
            // Update length of last chunk in metadata to what we actually find on LTS.
            if (null != segmentMetadata.getLastChunk()) {
                val chunkInfo = chunkStorage.getInfo(segmentMetadata.getLastChunk()).get();
                long length = chunkInfo.getLength();

                ChunkMetadata lastChunk = (ChunkMetadata) txn.get(segmentMetadata.getLastChunk()).get();
                Preconditions.checkState(null != lastChunk, "lastChunk must not be null. Segment=%s", segmentMetadata);
                lastChunk.setLength(length);
                txn.update(lastChunk);
                segmentMetadata.setLength(segmentMetadata.getLastChunkStartOffset() + length);
            }
            Preconditions.checkState(segmentMetadata.isOwnershipChanged(), "ownershipChanged must be true. Segment=%s", segmentMetadata);
            segmentMetadata.checkInvariants();
            txn.update(segmentMetadata);
        }
    }

    /**
     * Apply last effective truncate offsets.
     */
    private void applyFinalTruncateOffsets(MetadataTransaction txn, HashMap<String, Long> finalTruncateOffsets, HashMap<String, Long> finalFirstChunkStartsAtOffsets) throws Exception {
        for (String systemSegment : systemSegments) {
            if (finalTruncateOffsets.containsKey(systemSegment)) {
                val truncateAt = finalTruncateOffsets.get(systemSegment);
                val firstChunkStartsAt = finalFirstChunkStartsAtOffsets.get(systemSegment);
                applyTruncate(txn, systemSegment, truncateAt, firstChunkStartsAt);
            }
        }
    }

    /**
     * Apply chunk addition.
     */
    private void applyChunkAddition(MetadataTransaction txn, HashMap<String, Long> chunkStartOffsets, String segmentName, String oldChunkName, String newChunkName, long offset) throws Exception {
        Preconditions.checkState(null != oldChunkName, "oldChunkName must not be null");
        Preconditions.checkState(null != newChunkName && !newChunkName.isEmpty(), "newChunkName must not be null or empty");

        SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(segmentName).get();
        segmentMetadata.checkInvariants();

        // set length.
        segmentMetadata.setLength(offset);

        val newChunkMetadata = ChunkMetadata.builder()
                .name(newChunkName)
                .build();
        newChunkMetadata.setActive(true);
        txn.create(newChunkMetadata);
        txn.markPinned(newChunkMetadata);

        chunkStartOffsets.put(newChunkName, offset);
        // Set first and last pointers.
        if (!oldChunkName.isEmpty()) {
            ChunkMetadata oldChunk = (ChunkMetadata) txn.get(oldChunkName).get();
            Preconditions.checkState(null != oldChunk, "oldChunk must not be null. oldChunkName=%s", oldChunkName);

            // In case the old segment store was still writing some zombie chunks when ownership changed
            // then new offset may invalidate tail part of chunk list.
            // Note that chunk with oldChunkName is still valid, it is the chunks after this that become invalid.
            String toDelete = oldChunk.getNextChunk();
            while (toDelete != null) {
                ChunkMetadata chunkToDelete = (ChunkMetadata) txn.get(toDelete).get();
                txn.delete(toDelete);
                toDelete = chunkToDelete.getNextChunk();
                segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() - 1);
            }

            // Set next chunk
            oldChunk.setNextChunk(newChunkName);

            // Set length
            long oldLength = chunkStartOffsets.get(oldChunkName);
            oldChunk.setLength(offset - oldLength);

            txn.update(oldChunk);
        } else {
            segmentMetadata.setFirstChunk(newChunkName);
            segmentMetadata.setStartOffset(offset);
        }
        segmentMetadata.setLastChunk(newChunkName);
        segmentMetadata.setLastChunkStartOffset(offset);
        segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() + 1);
        segmentMetadata.checkInvariants();
        // Save the segment metadata.
        txn.update(segmentMetadata);
    }

    private String getSystemJournalChunkName() {
        return getSystemJournalChunkName(containerId, epoch, currentFileIndex);
    }

    private String getSystemJournalChunkName(int containerId, long epoch, long currentFileIndex) {
        return NameUtils.getSystemJournalFileName(containerId, epoch, currentFileIndex);
    }

    /**
     * Apply truncate action to the segment metadata.
     */
    private void applyTruncate(MetadataTransaction txn, String segmentName, long truncateAt, long firstChunkStartsAt) throws Exception {
        SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(segmentName).get();
        segmentMetadata.checkInvariants();
        String currentChunkName = segmentMetadata.getFirstChunk();
        ChunkMetadata currentMetadata;
        long startOffset = segmentMetadata.getFirstChunkStartOffset();
        while (null != currentChunkName) {
            currentMetadata = (ChunkMetadata) txn.get(currentChunkName).get();
            // If for given chunk start <= truncateAt < end  then we have found the chunk that will be the first chunk.
            if ((startOffset <= truncateAt) && (startOffset + currentMetadata.getLength() > truncateAt)) {
                break;
            }

            startOffset += currentMetadata.getLength();
            // move to next chunk
            currentChunkName = currentMetadata.getNextChunk();
            txn.delete(currentMetadata.getName());
            segmentMetadata.setChunkCount(segmentMetadata.getChunkCount() - 1);
        }
        Preconditions.checkState(firstChunkStartsAt == startOffset, "firstChunkStartsAt (%s) must be equal to startOffset (%s)", firstChunkStartsAt, startOffset);
        segmentMetadata.setFirstChunk(currentChunkName);
        if (null == currentChunkName) {
            segmentMetadata.setLastChunk(null);
            segmentMetadata.setLastChunkStartOffset(firstChunkStartsAt);
        }
        segmentMetadata.setStartOffset(truncateAt);
        segmentMetadata.setFirstChunkStartOffset(firstChunkStartsAt);
        segmentMetadata.checkInvariants();

    }

    public void validateAndSaveSnapshot(MetadataTransaction txn) throws Exception {
        SystemSnapshotRecord systemSnapshot = SystemSnapshotRecord.builder()
                .epoch(epoch)
                .segmentSnapshotRecords(new ArrayList<>())
                .build();

        for (String systemSegment : systemSegments) {
            // Find segment metadata.
            SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(systemSegment).get();
            segmentMetadata.checkInvariants();

            SegmentSnapshotRecord segmentSnapshot = SegmentSnapshotRecord.builder()
                    .segmentMetadata(segmentMetadata)
                    .chunkMetadataCollection(new ArrayList<>())
                    .build();

            // Enumerate all chunks.
            String currentChunkName = segmentMetadata.getFirstChunk();
            ChunkMetadata currentMetadata;
            long dataSize = 0;
            long chunkCount = 0;
            while (null != currentChunkName) {
                currentMetadata = (ChunkMetadata) txn.get(currentChunkName).get();

                val chunkInfo = chunkStorage.getInfo(currentChunkName).get();
                dataSize += currentMetadata.getLength();
                chunkCount++;
                Preconditions.checkState(chunkInfo.getLength() >= currentMetadata.getLength(),
                        "Wrong chunk length chunkInfo=%d, currentMetadata=%d.", chunkInfo.getLength(), currentMetadata.getLength());

                segmentSnapshot.chunkMetadataCollection.add(currentMetadata);
                // move to next chunk
                currentChunkName = currentMetadata.getNextChunk();
            }

            // Validate
            Preconditions.checkState(chunkCount == segmentMetadata.getChunkCount(), "Wrong chunk count. Segment=%s", segmentMetadata);
            Preconditions.checkState(dataSize == segmentMetadata.getLength() - segmentMetadata.getFirstChunkStartOffset(),
                    "Data size does not match dataSize (%s). Segment=%s", dataSize, segmentMetadata);

            // Add to the system snapshot.
            systemSnapshot.segmentSnapshotRecords.add(segmentSnapshot);
        }

        // Write snapshot
        val snapshotFile = getSystemJournalChunkName(containerId, epoch, 0);

        ByteArraySegment bytes;
        try {
            bytes = SYSTEM_SNAPSHOT_SERIALIZER.serialize(systemSnapshot);
        } catch (IOException e) {
            throw new ChunkStorageException(getSystemJournalChunkName(), "Unable to serialize", e);
        }
        synchronized (lock) {
            currentHandle = chunkStorage.createWithContent(snapshotFile,
                        bytes.getLength(),
                        new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.getLength())).get();
            // Start new journal.
            newChunkRequired = true;
        }

    }

    /**
     * Writes given ByteArraySegment to journal.
     * @param bytes Bytes to write.
     */
    private void writeToJournal(ByteArraySegment bytes) throws ExecutionException, InterruptedException {

        if (newChunkRequired) {
            currentFileIndex++;
            systemJournalOffset = 0;
            currentHandle = chunkStorage.createWithContent(getSystemJournalChunkName(containerId, epoch, currentFileIndex), bytes.getLength(),
                    new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.getLength())).get();
            systemJournalOffset += bytes.getLength();
            newChunkRequired = false;
        } else {
            Preconditions.checkState(chunkStorage.supportsAppend() && config.isAppendEnabled(), "Append mode not enabled or chunk storage does not support appends.");
            val bytesWritten = chunkStorage.write(currentHandle, systemJournalOffset, bytes.getLength(),
                    new ByteArrayInputStream(bytes.array(), bytes.arrayOffset(), bytes.getLength())).get();
            Preconditions.checkState(bytesWritten == bytes.getLength(),
                    "Bytes written do not match expected length. Actual=%d, expected=%d", bytesWritten, bytes.getLength());
            systemJournalOffset += bytesWritten;
        }
    }

    /**
     * Indicates whether given segment is a system segment.
     *
     * @param segmentName Name of the segment to check.
     * @return True if given segment is a system segment.
     */
    public boolean isStorageSystemSegment(String segmentName) {
        if (segmentName.startsWith(systemSegmentsPrefix)) {
            for (String systemSegment : systemSegments) {
                if (segmentName.equals(systemSegment)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Gets the names of the critical storage segments.
     *
     * @param containerId Container if of the owner container.
     * @return Array of names of the critical storage segments.
     */
    public static String[] getChunkStorageSystemSegments(int containerId) {
        return new String[]{
                NameUtils.getStorageMetadataSegmentName(containerId),
                NameUtils.getAttributeSegmentName(NameUtils.getStorageMetadataSegmentName(containerId)),
                NameUtils.getMetadataSegmentName(containerId),
                NameUtils.getAttributeSegmentName(NameUtils.getMetadataSegmentName(containerId))
        };
    }

    /**
     * Represents a system journal record.
     */
    @Data
    public static class SystemJournalRecord {
        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class SystemJournalRecordSerializer extends VersionedSerializer.MultiType<SystemJournalRecord> {
            /**
             * Declare all supported serializers of subtypes.
             *
             * @param builder A MultiType.Builder that can be used to declare serializers.
             */
            @Override
            protected void declareSerializers(Builder builder) {
                // Unused values (Do not repurpose!):
                // - 0: Unsupported Serializer.
                builder.serializer(ChunkAddedRecord.class, 1, new ChunkAddedRecord.Serializer())
                        .serializer(TruncationRecord.class, 2, new TruncationRecord.Serializer())
                        .serializer(SystemSnapshotRecord.class, 3, new SystemSnapshotRecord.Serializer())
                        .serializer(SegmentSnapshotRecord.class, 4, new SegmentSnapshotRecord.Serializer());
            }
        }
    }

    /**
     * Represents a system journal record.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode
    static class SystemJournalRecordBatch {
        @NonNull
        private final Collection<SystemJournalRecord> systemJournalRecords;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class SystemJournalRecordBatchBuilder implements ObjectBuilder<SystemJournalRecordBatch> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class SystemJournalRecordBatchSerializer
                extends VersionedSerializer.WithBuilder<SystemJournalRecordBatch, SystemJournalRecordBatchBuilder> {
            private static final SystemJournalRecord.SystemJournalRecordSerializer SERIALIZER = new SystemJournalRecord.SystemJournalRecordSerializer();
            private static final RevisionDataOutput.ElementSerializer<SystemJournalRecord> ELEMENT_SERIALIZER = (dataOutput, element) -> SERIALIZER.serialize(dataOutput, element);
            private static final RevisionDataInput.ElementDeserializer<SystemJournalRecord> ELEMENT_DESERIALIZER = dataInput -> SERIALIZER.deserialize(dataInput.getBaseStream());

            @Override
            protected SystemJournalRecordBatchBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput input, SystemJournalRecordBatchBuilder b) throws IOException {
                b.systemJournalRecords(input.readCollection(ELEMENT_DESERIALIZER));
            }

            private void write00(SystemJournalRecordBatch object, RevisionDataOutput output) throws IOException {
                output.writeCollection(object.systemJournalRecords, ELEMENT_SERIALIZER);
            }
        }
    }

    /**
     * Journal record for chunk addition.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class ChunkAddedRecord extends SystemJournalRecord {
        /**
         * Name of the segment.
         */
        @NonNull
        private final String segmentName;

        /**
         * Offset at which first byte in chunk starts.
         */
        private final long offset;

        /**
         * Name of the old last chunk.
         */
        private final String oldChunkName;

        /**
         * Name of the new chunk.
         */
        @NonNull
        private final String newChunkName;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class ChunkAddedRecordBuilder implements ObjectBuilder<ChunkAddedRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<ChunkAddedRecord, ChunkAddedRecordBuilder> {
            @Override
            protected ChunkAddedRecordBuilder newBuilder() {
                return ChunkAddedRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(ChunkAddedRecord object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.segmentName);
                output.writeUTF(nullToEmpty(object.newChunkName));
                output.writeUTF(nullToEmpty(object.oldChunkName));
                output.writeCompactLong(object.offset);
            }

            private void read00(RevisionDataInput input, ChunkAddedRecordBuilder b) throws IOException {
                b.segmentName(input.readUTF());
                b.newChunkName(emptyToNull(input.readUTF()));
                b.oldChunkName(emptyToNull(input.readUTF()));
                b.offset(input.readCompactLong());
            }
        }
    }

    /**
     * Journal record for segment truncation.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class TruncationRecord extends SystemJournalRecord {
        /**
         * Name of the segment.
         */
        @NonNull
        private final String segmentName;

        /**
         * Offset at which chunk is truncated.
         */
        private final long offset;

        /**
         * Name of the new first chunk.
         */
        @NonNull
        private final String firstChunkName;

        /**
         * Offset inside the first chunk where data starts.
         */
        private final long startOffset;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class TruncationRecordBuilder implements ObjectBuilder<TruncationRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<TruncationRecord, TruncationRecord.TruncationRecordBuilder> {
            @Override
            protected TruncationRecord.TruncationRecordBuilder newBuilder() {
                return TruncationRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(TruncationRecord object, RevisionDataOutput output) throws IOException {
                output.writeUTF(object.segmentName);
                output.writeCompactLong(object.offset);
                output.writeUTF(object.firstChunkName);
                output.writeCompactLong(object.startOffset);
            }

            private void read00(RevisionDataInput input, TruncationRecord.TruncationRecordBuilder b) throws IOException {
                b.segmentName(input.readUTF());
                b.offset(input.readCompactLong());
                b.firstChunkName(input.readUTF());
                b.startOffset(input.readCompactLong());
            }
        }
    }

    /**
     * Journal record for segment snapshot.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class SegmentSnapshotRecord extends SystemJournalRecord {
        /**
         * Data about the segment.
         */
        @NonNull
        private final SegmentMetadata segmentMetadata;

        @NonNull
        private final Collection<ChunkMetadata> chunkMetadataCollection;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class SegmentSnapshotRecordBuilder implements ObjectBuilder<SegmentSnapshotRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<SegmentSnapshotRecord, SegmentSnapshotRecord.SegmentSnapshotRecordBuilder> {

            private static final SegmentMetadata.StorageMetadataSerializer SEGMENT_METADATA_SERIALIZER = new SegmentMetadata.StorageMetadataSerializer();
            private static final ChunkMetadata.StorageMetadataSerializer CHUNK_METADATA_SERIALIZER = new ChunkMetadata.StorageMetadataSerializer();
            private static final RevisionDataOutput.ElementSerializer<ChunkMetadata> ELEMENT_SERIALIZER = (dataOutput, element) -> CHUNK_METADATA_SERIALIZER.serialize(dataOutput, element);
            private static final RevisionDataInput.ElementDeserializer<ChunkMetadata> ELEMENT_DESERIALIZER = dataInput -> (ChunkMetadata) CHUNK_METADATA_SERIALIZER.deserialize(dataInput.getBaseStream());

            @Override
            protected SegmentSnapshotRecord.SegmentSnapshotRecordBuilder newBuilder() {
                return SegmentSnapshotRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SegmentSnapshotRecord object, RevisionDataOutput output) throws IOException {
                SEGMENT_METADATA_SERIALIZER.serialize(output, object.segmentMetadata);
                output.writeCollection(object.chunkMetadataCollection, ELEMENT_SERIALIZER);
            }

            private void read00(RevisionDataInput input, SegmentSnapshotRecord.SegmentSnapshotRecordBuilder b) throws IOException {
                b.segmentMetadata((SegmentMetadata) SEGMENT_METADATA_SERIALIZER.deserialize(input.getBaseStream()));
                b.chunkMetadataCollection(input.readCollection(ELEMENT_DESERIALIZER));
            }
        }
    }

    /**
     * Journal record for segment snapshot.
     */
    @Builder(toBuilder = true)
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class SystemSnapshotRecord extends SystemJournalRecord {
        /**
         * Epoch of the snapshot
         */
        private final long epoch;

        /**
         * Snapshot of the individual segments.
         */
        @NonNull
        private final Collection<SegmentSnapshotRecord> segmentSnapshotRecords;

        /**
         * Builder that implements {@link ObjectBuilder}.
         */
        public static class SystemSnapshotRecordBuilder implements ObjectBuilder<SystemSnapshotRecord> {
        }

        /**
         * Serializer that implements {@link VersionedSerializer}.
         */
        public static class Serializer extends VersionedSerializer.WithBuilder<SystemSnapshotRecord, SystemSnapshotRecord.SystemSnapshotRecordBuilder> {
            private static final SegmentSnapshotRecord.Serializer CHUNK_METADATA_SERIALIZER = new SegmentSnapshotRecord.Serializer();
            private static final RevisionDataOutput.ElementSerializer<SegmentSnapshotRecord> ELEMENT_SERIALIZER = (dataOutput, element) -> CHUNK_METADATA_SERIALIZER.serialize(dataOutput, element);
            private static final RevisionDataInput.ElementDeserializer<SegmentSnapshotRecord> ELEMENT_DESERIALIZER = dataInput -> (SegmentSnapshotRecord) CHUNK_METADATA_SERIALIZER.deserialize(dataInput.getBaseStream());

            @Override
            protected SystemSnapshotRecord.SystemSnapshotRecordBuilder newBuilder() {
                return SystemSnapshotRecord.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SystemSnapshotRecord object, RevisionDataOutput output) throws IOException {
                output.writeCompactLong(object.epoch);
                output.writeCollection(object.segmentSnapshotRecords, ELEMENT_SERIALIZER);
            }

            private void read00(RevisionDataInput input, SystemSnapshotRecord.SystemSnapshotRecordBuilder b) throws IOException {
                b.epoch(input.readCompactLong());
                b.segmentSnapshotRecords(input.readCollection(ELEMENT_DESERIALIZER));
            }
        }
    }
}
