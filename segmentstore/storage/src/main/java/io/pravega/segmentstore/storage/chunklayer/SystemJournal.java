/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataException;
import io.pravega.shared.NameUtils;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

/**
 * This class implements system journaling functionality for critical storage system segments which is useful for bootstrap after failover.
 * It records any layout changes to storage system segments.
 * Storage system segments are the segments that the storage subsystem uses to store all metadata.
 * This create a circular dependency while reading or writing the data about these segment from the metadata segments..
 * System journal is a mechanism to break this circular dependency by having independent log of all layout changes to system segments.
 * Currently only two actions are considered viz. Addition of new chunks and truncation of segments.
 * This log is replayed when the ChunkStorageManager is booted.
 * To avoid data corruption. Each instance writes to its own distinct log file.
 * During bootstrap all the system journal files are read and processed to re-create the state of the storage system segments.
 */
@Slf4j
public class SystemJournal {
    private static final String ADD_RECORD = "ADD";
    private static final String TRUNCATE_RECORD = "TRUNCATE";
    private static final String RECORD_SEPARATOR = ",";
    private static final String START_TOKEN = "BEGIN";
    private static final String END_TOKEN = "END";

    private final Object lock = new Object();

    @Getter
    private final ChunkStorageProvider chunkStorage;

    @Getter
    private final ChunkMetadataStore metadataStore;

    /**
     * Epoch of the current instance.
     */
    @Getter
    private final long epoch;

    /**
     * Container id of the owner container.
     */
    @Getter
    private final int containerId;

    /**
     * Index of current journal file.
     */
    @Getter
    private int currentFileIndex;

    /**
     * String prefix for all system segments.
     */
    @Getter
    @Setter
    private String systemSegmentsPrefix;

    /**
     * System segments to track.
     */
    @Getter
    @Setter
    private String[] systemSegments;

    /**
     * Offset at which next log will be written.
     */
    private long systemJournalOffset;

    /**
     * Configuration {@link ChunkStorageManagerConfig} for the {@link ChunkStorageManager}.
     */
    @Getter
    private final ChunkStorageManagerConfig config;

    /**
     * Constructs an instance of {@link SystemJournal}.
     *
     * @param containerId Container id of the owner container.
     * @param epoch Epoch of the current container instance.
     * @param chunkStorage ChunkStorageProvider instance to use for writing all logs.
     * @param metadataStore ChunkMetadataStore for owner container.
     * @param config       Configuration options for this ChunkStorageManager instance.
     * @throws Exception In case of any errors.
     */
    public SystemJournal(int containerId, long epoch, ChunkStorageProvider chunkStorage, ChunkMetadataStore metadataStore, ChunkStorageManagerConfig config) throws Exception {
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.config = Preconditions.checkNotNull(config, "config");
        this.containerId = containerId;
        this.epoch = epoch;
        this.systemSegments = getChunkStorageSystemSegments(containerId);
        this.systemSegmentsPrefix = NameUtils.INTERNAL_SCOPE_NAME;

        Preconditions.checkState(!chunkStorage.exists(getSystemJournalFileName()));
    }

    /**
     * Initializes this instance.
     *
     * @throws Exception Exception if any.
     */
    public void initialize() throws Exception {
        chunkStorage.create(getSystemJournalFileName());
    }

    /**
     * Commits a given system log record to the underlying log chunk.
     *
     * @param record Record to persist.
     * @throws ChunkStorageException Exception if any.
     */
    public void commitRecord(SystemJournalRecord record) throws ChunkStorageException {
        commitRecords(Arrays.asList(record));
    }

    /**
     * Commits a given list of system log records to the underlying log chunk.
     *
     * @param records List of records to log to.
     * @throws ChunkStorageException Exception in case of any error.
     */
    public void commitRecords(Collection<SystemJournalRecord> records) throws ChunkStorageException {
        Preconditions.checkState(null != records);
        Preconditions.checkState(records.size() > 0);

        // Open the underlying chunk to write.
        ChunkHandle h = getChunkHandleForSystemJournal();

        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("\n");
        stringBuffer.append(START_TOKEN);
        stringBuffer.append(RECORD_SEPARATOR);
        for (val record : records) {
            String logLine = record.serialize();
            stringBuffer.append(logLine);
            stringBuffer.append(RECORD_SEPARATOR);
        }
        stringBuffer.append(END_TOKEN);

        // Persist
        byte[] bytes = stringBuffer.toString().getBytes(StandardCharsets.UTF_8);
        synchronized (lock) {
            val bytesWritten = chunkStorage.write(h, systemJournalOffset, bytes.length, new ByteArrayInputStream(bytes));
            Preconditions.checkState(bytesWritten == bytes.length);
            systemJournalOffset += bytesWritten;

            // Add a new log file if required.
            if (!chunkStorage.supportsAppend() || config.isAppendsDisabled()) {
                currentFileIndex++;
                systemJournalOffset = 0;
            }
        }
    }

    /**
     * Bootstrap the metadata about critical storage segments by reading and processing the journal.
     *
     * @throws Exception Exception in case of any error.
     */
    public void bootstrap() throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            // Keep track of offsets at which chunks were added to the system segments.
            val chunkStartOffsets = new HashMap<String, Long>();

            // Keep track of offsets at which system segments were truncated.
            // We don't need to apply each truncate operation, only need to apply the final truncate offset.
            val finalTruncateOffsets = new HashMap<String, Long>();
            val finalFirstChunkStartsAtOffsets = new HashMap<String, Long>();

            // Step 1: Create metadata records for system segments without any chunk information.
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

            // Step 2: For each epoch, find the corresponding system journal files, process them and apply operations recorded.
            applySystemLogOperations(txn, chunkStartOffsets, finalTruncateOffsets, finalFirstChunkStartsAtOffsets);

            // Step 3: Adjust the length of the last chunk.
            adjustLastChunkLengths(txn);

            // Step 4: Apply the truncate offsets.
            applyFinalTruncateOffsets(txn, finalTruncateOffsets, finalFirstChunkStartsAtOffsets);

            // Step 5: Finally commit all data.
            txn.commit(true, true);
        }
    }

    /**
     * Process all systemLog entries to recreate the state of metadata storage system segments.
     */
    private void applySystemLogOperations(MetadataTransaction txn, HashMap<String, Long> chunkStartOffsets, HashMap<String, Long> finalTruncateOffsets, HashMap<String, Long> finalFirstChunkStartsAtOffsets) throws ChunkStorageException, IOException, StorageMetadataException {
        for (int epochToRecover = 0; epochToRecover < epoch; epochToRecover++) {
            // Start scan with file index 0.
            int fileIndexToRecover = 0;
            while (chunkStorage.exists(getSystemJournalFileName(containerId, epochToRecover, fileIndexToRecover))) {
                val systemLogName = getSystemJournalFileName(containerId, epochToRecover, fileIndexToRecover);
                val info = chunkStorage.getInfo(systemLogName);
                val h = chunkStorage.openRead(systemLogName);
                byte[] contents = new byte[Math.toIntExact(info.getLength())];
                long fromOffset = 0;
                int remaining = contents.length;
                while (remaining > 0) {
                    int bytesRead = chunkStorage.read(h, fromOffset, remaining, contents, Math.toIntExact(fromOffset));
                    remaining -= bytesRead;
                    fromOffset += bytesRead;
                }
                BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(contents), StandardCharsets.UTF_8));
                String line;

                while ((line = reader.readLine()) != null) {
                    // Handle only whole records.
                    if (line.startsWith(START_TOKEN) && line.endsWith(END_TOKEN)) {
                        String[] records = line.split(RECORD_SEPARATOR);
                        for (String record : records) {
                            String[] parts = record.split(":");
                            if (ADD_RECORD.equals(parts[0]) && parts.length == 5) {
                                String segmentName = parts[1];
                                String oldChunkName = parts[2];
                                String newChunkName = parts[3];
                                long offset = Long.parseLong(parts[4]);

                                applyChunkAddition(txn, chunkStartOffsets, segmentName, oldChunkName, newChunkName, offset);
                            }
                            if (TRUNCATE_RECORD.equals(parts[0]) && parts.length == 5) {
                                String segmentName = parts[1];
                                long truncateAt = Long.parseLong(parts[2]);
                                String firstChunkName = parts[3];
                                long truncateStartAt = Long.parseLong(parts[4]);
                                finalTruncateOffsets.put(segmentName, truncateAt);
                                finalFirstChunkStartsAtOffsets.put(segmentName, truncateStartAt);
                            }
                        }
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
    private void adjustLastChunkLengths(MetadataTransaction txn) throws StorageMetadataException, ChunkStorageException {
        for (String systemSegment : systemSegments) {
            SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(systemSegment);
            segmentMetadata.checkInvariants();
            if (null != segmentMetadata.getLastChunk()) {
                val chunkInfo = chunkStorage.getInfo(segmentMetadata.getLastChunk());
                long length = chunkInfo.getLength();

                ChunkMetadata lastChunk = (ChunkMetadata) txn.get(segmentMetadata.getLastChunk());
                Preconditions.checkState(null != lastChunk);
                lastChunk.setLength(Math.toIntExact(length));
                txn.update(lastChunk);
                segmentMetadata.setLength(segmentMetadata.getLength() + length);
            }
            Preconditions.checkState(segmentMetadata.isOwnershipChanged());
            segmentMetadata.checkInvariants();
            txn.update(segmentMetadata);
        }
    }

    /**
     *
     * @param txn
     * @param finalTruncateOffsets
     * @param finalFirstChunkStartsAtOffsets
     * @throws StorageMetadataException
     */
    private void applyFinalTruncateOffsets(MetadataTransaction txn, HashMap<String, Long> finalTruncateOffsets, HashMap<String, Long> finalFirstChunkStartsAtOffsets) throws StorageMetadataException {
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
    private void applyChunkAddition(MetadataTransaction txn, HashMap<String, Long> chunkStartOffsets, String segmentName, String oldChunkName, String newChunkName, long offset) throws StorageMetadataException {
        Preconditions.checkState(null != oldChunkName);
        Preconditions.checkState(null != newChunkName && !newChunkName.isEmpty());

        SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(segmentName);
        segmentMetadata.checkInvariants();

        // set length.
        segmentMetadata.setLength(offset);

        val newChunkMetadata = ChunkMetadata.builder()
                .name(newChunkName)
                .build();
        txn.create(newChunkMetadata);
        txn.markPinned(newChunkMetadata);

        chunkStartOffsets.put(newChunkName, offset);
        // Set first and last pointers.
        if (!oldChunkName.isEmpty()) {
            ChunkMetadata oldChunk = (ChunkMetadata) txn.get(oldChunkName);
            Preconditions.checkState(null != oldChunk);

            // In case the old segment store was still writing some zombie chunks when ownership changed
            // then new offset may invalidate tail part of chunk list.
            // Note that chunk with oldChunkName is still valid, it is the chunks after this that become invalid.
            String toDelete = oldChunk.getNextChunk();
            while (toDelete != null) {
                ChunkMetadata chunkToDelete = (ChunkMetadata) txn.get(toDelete);
                txn.delete(toDelete);
                toDelete = chunkToDelete.getNextChunk();
                segmentMetadata.decrementChunkCount();
            }

            // Set next chunk
            oldChunk.setNextChunk(newChunkName);

            // Set length
            long oldLength = chunkStartOffsets.get(oldChunkName);
            oldChunk.setLength(Math.toIntExact(offset - oldLength));

            txn.update(oldChunk);
        } else {
            segmentMetadata.setFirstChunk(newChunkName);
        }
        segmentMetadata.setLastChunk(newChunkName);
        segmentMetadata.setLastChunkStartOffset(offset);
        segmentMetadata.incrementChunkCount();
        segmentMetadata.checkInvariants();
        // Save the segment metadata.
        txn.update(segmentMetadata);
    }

    private String getSystemJournalFileName() {
        return getSystemJournalFileName(containerId, epoch, currentFileIndex);
    }

    private String getSystemJournalFileName(int containerId, long epoch, long currentFileIndex) {
        return NameUtils.getSystemJournalFileName(containerId, epoch, currentFileIndex);
    }

    private ChunkHandle getChunkHandleForSystemJournal() throws ChunkStorageException {
        ChunkHandle h;
        val systemLogName = getSystemJournalFileName();
        if (!chunkStorage.exists(systemLogName)) {
            h = chunkStorage.create(systemLogName);
        } else {
            h = chunkStorage.openWrite(systemLogName);
        }
        return h;
    }

    /**
     * Apply truncate action to the segment metadata.
     */
    private void applyTruncate(MetadataTransaction txn, String segmentName, long truncateAt, long firstChunkStartsAt) throws StorageMetadataException {
        SegmentMetadata segmentMetadata = (SegmentMetadata) txn.get(segmentName);
        segmentMetadata.checkInvariants();
        String currentChunkName = segmentMetadata.getFirstChunk();
        ChunkMetadata currentMetadata;
        long startOffset = segmentMetadata.getFirstChunkStartOffset();
        while (null != currentChunkName) {
            currentMetadata = (ChunkMetadata) txn.get(currentChunkName);
            // If for given chunk start <= truncateAt < end  then we have found the chunk that will be the first chunk.
            if ((startOffset <= truncateAt) && (startOffset + currentMetadata.getLength() > truncateAt)) {
                break;
            }

            startOffset += currentMetadata.getLength();
            // move to next chunk
            currentChunkName = currentMetadata.getNextChunk();
            txn.delete(currentMetadata.getName());
            segmentMetadata.decrementChunkCount();
        }
        Preconditions.checkState(firstChunkStartsAt == startOffset);
        segmentMetadata.setFirstChunk(currentChunkName);
        segmentMetadata.setStartOffset(truncateAt);
        segmentMetadata.setFirstChunkStartOffset(firstChunkStartsAt);
        segmentMetadata.checkInvariants();

    }

    /**
     * Indicates whether given segment is a system segment.
     *
     * @param segmentName Name of the sgement to check.
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
    public interface SystemJournalRecord {
        String serialize();
    }

    /**
     *  Journal record for chunk addition.
     */
    @Data
    @Builder
    public static class ChunkAddedRecord implements SystemJournalRecord {
        /**
         * Name of the segment.
         */
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
        private final String newChunkName;

        /**
         * Serializes this record.
         * @return String representation.
         */
        public String serialize() {
            return String.format("%s:%s:%s:%s:%d",
                    ADD_RECORD,
                    segmentName,
                    oldChunkName == null ? "" : oldChunkName,
                    newChunkName == null ? "" : newChunkName,
                    offset);
        }
    }

    /**
     *  Journal record for segment truncation.
     */
    @Data
    @Builder
    public static class TruncationRecord implements SystemJournalRecord {
        /**
         * Name of the segment.
         */
        private final String segmentName;

        /**
         * Offset at which chunk is truncated.
         */
        private final long offset;

        /**
         * Name of the new first chunk.
         */
        private final String firstChunkName;

        /**
         * Offset inside the first chunk where data starts.
         */
        private final long startOffset;

        /**
         * Serializes this record.
         * @return String representation.
         */
        public String serialize() {
            return String.format("%s:%s:%d:%s:%d",
                    TRUNCATE_RECORD,
                    segmentName,
                    offset,
                    firstChunkName,
                    startOffset);
        }
    }
}
