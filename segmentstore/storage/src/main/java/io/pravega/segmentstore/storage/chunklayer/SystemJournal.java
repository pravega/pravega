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
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.MetadataTransaction;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadataException;
import io.pravega.shared.NameUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class implements system journaling functionality for critical storage system segments which is useful for bootstrap after failover.
 * It records any layout changes to storage system segments
 */
@Slf4j
public class SystemJournal {
    // TODO : Remove this from the final code.
    private static final ArrayList<String> DEBUG_LOG = new ArrayList<>();
    private static final String ADD_RECORD = "ADD";
    private static final String TRUNCATE_RECORD = "TRUNCATE";

    private static final Object LOCK = new Object();

    private ChunkStorageProvider chunkStorage;
    private ChunkMetadataStore metadataStore;

    /**
     * Epoch of the current instance.
     */
    @Getter
    @Setter
    private long epoch;

    /**
     * Container id of the owner container.
     */
    @Getter
    @Setter
    private int containerId;

    /**
     * Sting prefix for all system segments.
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
    @Getter
    @Setter
    private long systemJournalOffset;

    /**
     * Name of the system journal chunk.
     */
    @Getter
    @Setter
    private String systemJournalName;

    /**
     * Default {@link SegmentRollingPolicy} for the system segments.
     */
    @Getter
    @Setter
    private SegmentRollingPolicy segmentRollingPolicy;

    /**
     * Constructs an instance of {@link SystemJournal}.
     * @param containerId Container id of the owner container.
     * @param epoch Epoch of the current container instance.
     * @param chunkStorage ChunkStorageProvider instance to use for writing all logs.
     * @param metadataStore ChunkMetadataStore for owner container.
     * @param segmentRollingPolicy Default SegmentRollingPolicy for system segments.
     */
    public SystemJournal(int containerId, long epoch, ChunkStorageProvider chunkStorage, ChunkMetadataStore metadataStore, SegmentRollingPolicy segmentRollingPolicy)  {
        this.chunkStorage = Preconditions.checkNotNull(chunkStorage, "chunkStorage");
        this.metadataStore = Preconditions.checkNotNull(metadataStore, "metadataStore");
        this.segmentRollingPolicy = Preconditions.checkNotNull(segmentRollingPolicy, "segmentRollingPolicy");
        this.containerId = containerId;
        this.epoch = epoch;
        this.systemJournalName = NameUtils.getSystemJournalFileName(containerId, epoch);
        this.systemSegments = getChunkStorageSystemSegments(containerId);
        this.systemSegmentsPrefix = NameUtils.INTERNAL_SCOPE_NAME;
    }

    /**
     * Initlaizes this instance.
     * @throws Exception Exception if any.
     */
    public void initialize() throws Exception {
        chunkStorage.create(systemJournalName);
    }

    /**
     * Commits a given system log record to the underlying log chunk.
     * @param logLine Log line to persist.
     * @throws IOException Exception if any.
     */
    public void commitRecord(String logLine) throws IOException {
        synchronized (LOCK) {
            // Open the underlying chunk to write.
            ChunkHandle h = null;
            if (chunkStorage.supportsAppend()) {
                if (!chunkStorage.exists(systemJournalName)) {
                    h = chunkStorage.create(systemJournalName);
                } else {
                    h = chunkStorage.openWrite(systemJournalName);
                }
            }

            if (!chunkStorage.supportsAppend()) {
                h = chunkStorage.create(systemJournalName + "_" + systemJournalOffset);
            }

            // Persist
            byte[] bytes = logLine.getBytes();
            val bytesWritten = chunkStorage.write(h, systemJournalOffset, bytes.length, new ByteArrayInputStream(bytes));
            Preconditions.checkState(bytesWritten == bytes.length);
            systemJournalOffset += bytesWritten;
            DEBUG_LOG.add(logLine);
        }
    }

    /**
     * Commits a given list of system log records to the underlying log chunk.
     * @param records List of records to log to.
     * @throws IOException Exception in case of any error.
     */
    synchronized public void commitRecords(List<String> records) throws IOException {
        Preconditions.checkState(null != records);
        Preconditions.checkState(records.size() > 0);
        synchronized (LOCK) {
            // Open the underlying chunk to write.
            ChunkHandle h = null;
            if (chunkStorage.supportsAppend()) {
                if (!chunkStorage.exists(systemJournalName)) {
                    h = chunkStorage.create(systemJournalName);
                } else {
                    h = chunkStorage.openWrite(systemJournalName);
                }
            }

            // Persist
            for (String logLine : records) {
                if (!chunkStorage.supportsAppend()) {
                    h = chunkStorage.create(systemJournalName + "_" + systemJournalOffset);
                }
                byte[] bytes = logLine.getBytes();
                val bytesWritten = chunkStorage.write(h, systemJournalOffset, bytes.length, new ByteArrayInputStream(bytes));
                Preconditions.checkState(bytesWritten == bytes.length);
                systemJournalOffset += bytesWritten;
                DEBUG_LOG.add(logLine);
            }
        }
    }

    /**
     * Formats a log record for newly added chunk.
     * @param segmentName Name of the segment.
     * @param offset offset at which new chunk was added.
     * @param oldChunkName Name of the previous last chunk.
     * @param newChunkName Name of the new last chunk.
     * @return Formatted log record string.
     */
    public String getChunkAddedRecord(String segmentName, long offset, String oldChunkName, String newChunkName) {
        return ADD_RECORD
            + ":" + segmentName
            + ":" + (oldChunkName == null ? "" : oldChunkName)
            + ":" + (newChunkName == null ? "" : newChunkName)
            + ":" + offset + "\n";
    }

    /**
     * Formats a log record for trucate operation on given segment.
     * @param segmentName Name of the segment.
     * @param offset Offset at which the segment is truncated.
     * @param firstChunkName Name of the new first chunk.
     * @param startOffset Offset at which new chunk starts. The actual start offset of the segment may be anywhere in that chunk.
     * @return Formatted log record string.
     */
    public String getSegmentTruncatedRecord(String segmentName, long offset, String firstChunkName, long startOffset) {
        return TRUNCATE_RECORD
                + ":" + segmentName
                + ":" + offset
                + ":" + firstChunkName
                + ":" + startOffset + "\n";
    }

    /**
     * Bootstrap the metadata about critical storage segments by reading and processing the journal.
     * @throws Exception Exception in case of any error.
     */
    synchronized public void bootstrap() throws Exception {
        synchronized (LOCK) {
            try (val txn = metadataStore.beginTransaction()) {
                // Step 1: Create segment metadata.
                for (String systemSegment : systemSegments) {
                    SegmentMetadata segmentMetadata = SegmentMetadata.builder()
                            .name(systemSegment)
                            .ownerEpoch(epoch)
                            .maxRollinglength(segmentRollingPolicy.getMaxLength())
                            .build();
                    segmentMetadata.setActive(true).setOwnershipChanged(true);
                    segmentMetadata.checkInvariants();
                    metadataStore.create(txn, segmentMetadata);
                }

                // We only need to apply the final truncate offset.
                val finalTruncateOffsets = new HashMap<String, Long>();
                val finalFirstChunkStartsAtOffsets = new HashMap<String, Long>();

                // Step 2: For each epoch, find the corresponding system journal file.
                for (int i = 0; i < epoch; i++) {
                    long offset = 0;
                    String systemlog = NameUtils.getSystemJournalFileName(containerId, i);
                    if (!chunkStorage.supportsAppend()) {
                        systemlog = systemlog + "_" + offset;
                    }
                    if (chunkStorage.exists(systemlog)) {
                        val info = chunkStorage.getInfo(systemlog);
                        val h = chunkStorage.openRead(systemlog);
                        byte[] contents = new byte[Math.toIntExact(info.getLength())];
                        long fromOffset = 0;
                        int remaining = contents.length;
                        while (remaining > 0) {
                            int bytesRead = chunkStorage.read(h, fromOffset, remaining, contents, Math.toIntExact(fromOffset));
                            remaining -= bytesRead;
                            fromOffset += bytesRead;
                        }
                        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(contents)));
                        String fullContent = new String(contents);
                        String line;

                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(":");
                            if (ADD_RECORD.equals(parts[0]) && parts.length == 5) {
                                String segmentName = parts[1];
                                String oldChunkName = parts[2];
                                String newChunkName = parts[3];
                                offset = Long.parseLong(parts[4]);
                                SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, segmentName);
                                segmentMetadata.checkInvariants();
                                // set length.
                                long oldLength = segmentMetadata.getLength();
                                segmentMetadata.setLength(offset);
                                metadataStore.create(txn, ChunkMetadata.builder()
                                        .name(newChunkName)
                                        .build());

                                // Set first and last pointers.
                                if (!oldChunkName.isEmpty()) {
                                    ChunkMetadata oldChunk = (ChunkMetadata) metadataStore.get(txn, oldChunkName);
                                    oldChunk.setNextChunk(newChunkName);
                                    oldChunk.setLength(Math.toIntExact(offset - oldLength));
                                    metadataStore.update(txn, oldChunk);
                                } else {
                                    segmentMetadata.setFirstChunk(newChunkName);
                                }
                                segmentMetadata.setLastChunk(newChunkName);
                                segmentMetadata.setLastChunkStartOffset(offset);
                                segmentMetadata.checkInvariants();
                                // Save the segment metadata.
                                metadataStore.update(txn, segmentMetadata);
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

                // Step 3: Adjust the length of the last chunk.
                for (String systemSegment : systemSegments) {
                    SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, systemSegment);
                    segmentMetadata.checkInvariants();
                    if (null != segmentMetadata.getLastChunk()) {
                        val chunkInfo = chunkStorage.getInfo(segmentMetadata.getLastChunk());
                        long length = chunkInfo.getLength();

                        ChunkMetadata lastChunk = (ChunkMetadata) metadataStore.get(txn, segmentMetadata.getLastChunk());
                        if (null != lastChunk) {
                            lastChunk.setLength(Math.toIntExact(length));
                            metadataStore.update(txn, lastChunk);
                        } else {
                            log.debug("Problem");
                        }
                        segmentMetadata.setLength(segmentMetadata.getLength() + length);
                    }
                    Preconditions.checkState(segmentMetadata.isOwnershipChanged());
                    segmentMetadata.checkInvariants();
                    metadataStore.update(txn, segmentMetadata);
                }

                // Step 4: Apply the truncate offsets.
                for (String systemSegment : systemSegments) {
                    if (finalTruncateOffsets.containsKey(systemSegment)) {
                        val truncateAt = finalTruncateOffsets.get(systemSegment);
                        val firstChunkStartsAt = finalFirstChunkStartsAtOffsets.get(systemSegment);
                        truncate(txn, systemSegment, truncateAt, firstChunkStartsAt);
                    }
                }

                // Step 5: Finally commit all data.
                txn.commit(true, true);
            }
        }
    }

    /**
     * Truncate the segment metadata.
     */
    private void truncate(MetadataTransaction txn, String segmentName, long truncateAt, long firstChunkStartsAt) throws StorageMetadataException {
        SegmentMetadata segmentMetadata = (SegmentMetadata) metadataStore.get(txn, segmentName);
        segmentMetadata.checkInvariants();
        String currentChunkName = segmentMetadata.getFirstChunk();
        ChunkMetadata currentMetadata;
        long startOffset = segmentMetadata.getFirstChunkStartOffset();
        while (null != currentChunkName) {
            currentMetadata = (ChunkMetadata) metadataStore.get(txn, currentChunkName);
            // If for given chunk start <= truncateAt < end  then we have found the chunk that will be the first chunk.
            if ((startOffset <= truncateAt) && (startOffset + currentMetadata.getLength() > truncateAt)) {
                break;
            }

            startOffset += currentMetadata.getLength();
            // move to next chunk
            currentChunkName = currentMetadata.getNextChunk();
            metadataStore.delete(txn, currentMetadata.getName());
        }
        Preconditions.checkState(firstChunkStartsAt == startOffset);
        segmentMetadata.setFirstChunk(currentChunkName);
        segmentMetadata.setStartOffset(truncateAt);
        segmentMetadata.setFirstChunkStartOffset(firstChunkStartsAt);
        segmentMetadata.checkInvariants();

    }

    /**
     * Indicates whether given segment is a system segment.
     * @param segmentName Name of the sgement to check.
     * @return True if given segment is a system segment.
     */
    public boolean isSystemSegment(String segmentName) {
        if (segmentName.startsWith(systemSegmentsPrefix)) {
            for (String systemSegment: systemSegments) {
                if (segmentName.equals(systemSegment)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Gets the names of the critical storage segments.
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
}
