package com.emc.logservice.reading;

import com.emc.logservice.core.AutoReleaseLock;
import com.emc.logservice.core.ReadWriteAutoReleaseLock;
import com.emc.logservice.*;

import java.time.Duration;
import java.util.*;

/**
 * StreamSegment Container Read Index. Provides access to Read Indices for all StreamSegments within this Container.
 * <p>
 * Integrates reading data from the following sources:
 * <ol>
 * <li> The tail-end part of the StreamSegment (the part that is in DurableLog, but not yet in Storage).
 * <li> The part of the StreamSegment that is in Storage, but not in DurableLog. This data will be brought into memory
 * for fast read-ahead access.
 * </ol>
 */
public class ReadIndex implements StreamSegmentCache {
    //region Members

    private final HashMap<Long, StreamSegmentReadIndex> readIndices;
    private final ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();
    private StreamSegmentMetadataSource metadata;
    private boolean recoveryMode;

    //endregion

    //region Constructor

    public ReadIndex(StreamSegmentMetadataSource metadata) {
        this.readIndices = new HashMap<>();
        this.metadata = metadata;
        this.recoveryMode = false;
    }

    //endregion

    //region StreamSegmentCache Implementation

    @Override
    public void append(long streamSegmentId, long offset, byte[] data) {
        // Append the data to the StreamSegment Index. It performs further validation with respect to offsets, etc.
        StreamSegmentReadIndex index = getReadIndex(streamSegmentId, true);
        if (index.isMerged()) {
            throw new IllegalArgumentException("streamSegmentId refers to a StreamSegment that is merged. Cannot append to it anymore.");
        }

        index.append(offset, data);
    }

    @Override
    public void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId, long sourceStreamSegmentLength) {
        StreamSegmentReadIndex targetIndex = getReadIndex(targetStreamSegmentId, true);
        StreamSegmentReadIndex sourceIndex = getReadIndex(sourceStreamSegmentId, true);
        if (sourceIndex.isMerged()) {
            throw new IllegalArgumentException("sourceStreamSegmentId refers to a StreamSegment that is merged. Cannot access it anymore.");
        }

        targetIndex.beginMerge(offset, sourceIndex);
        sourceIndex.markMerged();
    }

    @Override
    public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
        StreamSegmentReadIndex targetIndex = getReadIndex(targetStreamSegmentId, true);
        targetIndex.completeMerge(sourceStreamSegmentId);
        removeReadIndex(sourceStreamSegmentId);
    }

    @Override
    public ReadResult read(long streamSegmentId, long offset, int maxLength, Duration timeout) {
        StreamSegmentReadIndex index = getReadIndex(streamSegmentId, true);
        if (index.isMerged()) {
            throw new IllegalArgumentException("streamSegmentId refers to a StreamSegment that is merged. Cannot access it anymore.");
        }

        return index.read(offset, maxLength, timeout);
    }

    @Override
    public void triggerFutureReads(Collection<Long> streamSegmentIds) {
        HashSet<String> missingIds = new HashSet<>();
        for (long ssId : streamSegmentIds) {
            StreamSegmentReadIndex index = getReadIndex(ssId, false);
            if (index == null) {
                if (this.metadata.getStreamSegmentMetadata(ssId) == null) {
                    missingIds.add(Long.toString(ssId));
                }
            }
            else {
                index.triggerFutureReads();
            }
        }

        // Throw any exception at the end - we want to make sure at least the ones that did have a valid index entry got triggered.
        if (missingIds.size() > 0) {
            throw new IllegalArgumentException("At least one StreamSegmentId does not exist in the metadata: " + String.join(", ", missingIds));
        }
    }

    @Override
    public void clear() {
        if (!this.recoveryMode) {
            throw new IllegalStateException("Read Index is not in recovery mode. Cannot clear cache.");
        }

        try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
            this.readIndices.values().forEach(StreamSegmentReadIndex::close);
            this.readIndices.clear();
        }
    }

    @Override
    public void enterRecoveryMode(StreamSegmentMetadataSource recoveryMetadataSource) {
        if (this.recoveryMode) {
            throw new IllegalStateException("Read Index is already in recovery mode.");
        }

        if (recoveryMetadataSource == null) {
            throw new NullPointerException("recoveryMetadataSource");
        }

        this.recoveryMode = true;
        this.metadata = recoveryMetadataSource;
        clear();
    }

    @Override
    public void exitRecoveryMode(StreamSegmentMetadataSource finalMetadataSource, boolean success) {
        if (!this.recoveryMode) {
            throw new IllegalStateException("Read Index is not in recovery mode.");
        }

        if (finalMetadataSource == null) {
            throw new NullPointerException("finalMetadataSource");
        }

        if (success) {
            for (Map.Entry<Long, StreamSegmentReadIndex> e : this.readIndices.entrySet()) {
                ReadOnlyStreamSegmentMetadata metadata = finalMetadataSource.getStreamSegmentMetadata(e.getKey());
                if (metadata == null) {
                    throw new IllegalArgumentException(String.format("Final Metadata has no knowledge of StreamSegment Id %d.", e.getKey()));
                }

                e.getValue().exitRecoveryMode(metadata);
            }
        }
        else {
            // Recovery was unsuccessful. Clear the contents of the cache to avoid further issues.
            clear();
        }

        this.metadata = finalMetadataSource;
        this.recoveryMode = false;
    }

    //endregion

    //region Helpers

    /**
     * Gets a reference to the existing StreamSegmentRead index for the given StreamSegment Id.
     *
     * @param streamSegmentId
     * @param createIfNotPresent If no Read Index is loaded, creates a new one.
     * @return
     */
    private StreamSegmentReadIndex getReadIndex(long streamSegmentId, boolean createIfNotPresent) {
        StreamSegmentReadIndex index;
        try (AutoReleaseLock readLock = lock.acquireReadLock()) {
            // Try to see if we have the index already in memory.
            if ((index = this.readIndices.getOrDefault(streamSegmentId, null)) != null || !createIfNotPresent) {
                // If we do, or we are told not to create one if not present, then return whatever we have.
                return index;
            }

            try (AutoReleaseLock writeLock = lock.upgradeToWriteLock(readLock)) {
                // We don't have it; we have acquired the exclusive write lock, and check again, just in case, if someone
                // else got it for us.
                if ((index = this.readIndices.getOrDefault(streamSegmentId, null)) != null) {
                    return index;
                }

                // We don't have it, and nobody else got it for us.
                ReadOnlyStreamSegmentMetadata ssm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                if (ssm == null) {
                    throw new IllegalArgumentException(String.format("StreamSegmentId %d does not exist in the metadata.", streamSegmentId));
                }

                index = new StreamSegmentReadIndex(ssm, this.recoveryMode);
                this.readIndices.put(streamSegmentId, index);
            }
        }

        return index;
    }

    private boolean removeReadIndex(long streamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            StreamSegmentReadIndex index = this.readIndices.remove(streamSegmentId);
            if (index != null) {
                index.close();
            }

            return index != null;
        }
    }

    //endregion
}
