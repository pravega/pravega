package com.emc.logservice.server.containers;

import com.emc.logservice.common.*;
import com.emc.logservice.server.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metadata for a Stream Segment Container.
 */
@Slf4j
public class StreamSegmentContainerMetadata implements RecoverableMetadata, UpdateableContainerMetadata {
    //region Members

    private final String traceObjectId;
    private final AtomicLong sequenceNumber;
    private final HashMap<String, Long> streamSegmentIds;
    private final HashMap<Long, UpdateableSegmentMetadata> streamMetadata;
    private final AtomicBoolean recoveryMode;
    private final String streamSegmentContainerId;
    private final ReadWriteAutoReleaseLock lock = new ReadWriteAutoReleaseLock();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainerMetadata.
     */
    public StreamSegmentContainerMetadata(String streamSegmentContainerId) {
        Exceptions.throwIfNullOfEmpty(streamSegmentContainerId, "streamSegmentContainerId");
        //TODO: need to define a MetadataReaderWriter class which we can pass to this. Metadata always need to be persisted somewhere.
        this.traceObjectId = String.format("SegmentContainer[%s]", streamSegmentContainerId);
        this.streamSegmentContainerId = streamSegmentContainerId;
        this.sequenceNumber = new AtomicLong();
        this.streamSegmentIds = new HashMap<>();
        this.streamMetadata = new HashMap<>();
        this.recoveryMode = new AtomicBoolean();
    }

    //endregion

    //region SegmentMetadataCollection Implementation

    /**
     * Gets the Id of the StreamSegment with given name.
     *
     * @param streamSegmentName The case-sensitive StreamSegment name.
     * @return The Id of the StreamSegment, or NoStreamSegmentId if the Metadata has no knowledge of it.
     */
    public long getStreamSegmentId(String streamSegmentName) {
        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            return this.streamSegmentIds.getOrDefault(streamSegmentName, NoStreamSegmentId);
        }
    }

    @Override
    public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            return this.streamMetadata.getOrDefault(streamSegmentId, null);
        }
    }

    //endregion

    //region ReadOnlyStreamSegmentContainerMetadata

    @Override
    public String getContainerId() {
        return this.streamSegmentContainerId;
    }

    @Override
    public boolean isRecoveryMode() {
        return this.recoveryMode.get();
    }

    @Override
    public long getNewOperationSequenceNumber() {
        ensureNonRecoveryMode();
        return this.sequenceNumber.incrementAndGet();
    }

    //endregion

    //region UpdateableContainerMetadata

    @Override
    public void mapStreamSegmentId(String streamSegmentName, long streamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            Exceptions.throwIfIllegalArgument(!this.streamSegmentIds.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.throwIfIllegalArgument(!this.streamMetadata.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

            this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
            this.streamMetadata.put(streamSegmentId, new StreamSegmentMetadata(streamSegmentName, streamSegmentId));
        }

        log.info("{}: MapStreamSegment Id = {}, Name = '{}'", this.traceObjectId, streamSegmentId, streamSegmentName);
    }

    @Override
    public void mapStreamSegmentId(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId) {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            Exceptions.throwIfIllegalArgument(!this.streamSegmentIds.containsKey(streamSegmentName), "streamSegmentName", "StreamSegment '%s' is already mapped.", streamSegmentName);
            Exceptions.throwIfIllegalArgument(!this.streamMetadata.containsKey(streamSegmentId), "streamSegmentId", "StreamSegment Id %d is already mapped.", streamSegmentId);

            UpdateableSegmentMetadata parentMetadata = this.streamMetadata.getOrDefault(parentStreamSegmentId, null);
            Exceptions.throwIfIllegalArgument(parentMetadata != null, "parentStreamSegmentId", "Invalid Parent Stream Id.");
            Exceptions.throwIfIllegalArgument(parentMetadata.getParentId() == SegmentMetadataCollection.NoStreamSegmentId, "parentStreamSegmentId", "Cannot create a batch StreamSegment for another batch StreamSegment.");

            this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
            this.streamMetadata.put(streamSegmentId, new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentStreamSegmentId));
        }

        log.info("{}: MapBatchStreamSegment ParentId = {}, Id = {}, Name = '{}'", this.traceObjectId, parentStreamSegmentId, streamSegmentId, streamSegmentName);
    }

    @Override
    public Collection<String> deleteStreamSegment(String streamSegmentName) {
        Collection<String> result = new ArrayList<>();
        result.add(streamSegmentName);
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            long streamSegmentId = this.streamSegmentIds.getOrDefault(streamSegmentName, SegmentMetadataCollection.NoStreamSegmentId);
            if (streamSegmentId == SegmentMetadataCollection.NoStreamSegmentId) {
                // We have no knowledge in our metadata about this StreamSegment. This means it has no batches associated
                // with it, so no need to do anything else.
                log.info("{}: DeleteStreamSegments {}", this.traceObjectId, result);
                return result;
            }

            // Mark this segment as deleted.
            UpdateableSegmentMetadata segmentMetadata = this.streamMetadata.getOrDefault(streamSegmentId, null);
            if (segmentMetadata != null) {
                segmentMetadata.markDeleted();
            }

            // Find any batches that point to this StreamSegment (as a parent).
            for (UpdateableSegmentMetadata batchSegmentMetadata : this.streamMetadata.values()) {
                if (batchSegmentMetadata.getParentId() == streamSegmentId) {
                    batchSegmentMetadata.markDeleted();
                    result.add(batchSegmentMetadata.getName());
                }
            }
        }

        log.info("{}: DeleteStreamSegments {}", this.traceObjectId, result);
        return result;
    }

    @Override
    public void setOperationSequenceNumber(long value) {
        ensureRecoveryMode();

        // Note: This check-and-set is not atomic, but in recovery mode we are executing in a single thread, so this is ok.
        Exceptions.throwIfIllegalArgument(value >= this.sequenceNumber.get(), "value", "Invalid SequenceNumber. Expecting greater than %d.", this.sequenceNumber.get());

        this.sequenceNumber.set(value);
    }
    //endregion

    //region RecoverableMetadata Implementation

    @Override
    public void enterRecoveryMode() {
        ensureNonRecoveryMode();
        this.recoveryMode.set(true);
        log.info("{}: Enter RecoveryMode.", this.traceObjectId);
    }

    @Override
    public void exitRecoveryMode() {
        ensureRecoveryMode();
        this.recoveryMode.set(false);
        log.info("{}: Exit RecoveryMode.", this.traceObjectId);
    }

    @Override
    public void reset() {
        ensureRecoveryMode();
        this.sequenceNumber.set(0);
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            this.streamSegmentIds.clear();
            this.streamMetadata.clear();
        }

        log.info("{}: Reset.", this.traceObjectId);
    }

    private void ensureRecoveryMode() {
        Exceptions.throwIfIllegalState(isRecoveryMode(), "", "StreamSegmentContainerMetadata is not in recovery mode. Cannot execute this operation.");
    }

    private void ensureNonRecoveryMode() {
        Exceptions.throwIfIllegalState(!isRecoveryMode(), "", "StreamSegmentContainerMetadata is in recovery mode. Cannot execute this operation.");
    }

    //endregion
}
