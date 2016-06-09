package com.emc.logservice.server.containers;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.server.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Metadata for a particular Stream Segment.
 */
@Slf4j
public class StreamSegmentMetadata implements UpdateableSegmentMetadata {
    //region Members

    private final String traceObjectId;
    private final String name;
    private final long streamSegmentId;
    private final long parentStreamSegmentId;
    private final AbstractMap<UUID, AppendContext> lastCommittedAppends;
    private long storageLength;
    private long durableLogLength;
    private boolean sealed;
    private boolean deleted;
    private boolean merged;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a stand-alone StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param streamSegmentId   The Id of the StreamSegment.
     * @throws IllegalArgumentException If either of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId) {
        this(streamSegmentName, streamSegmentId, SegmentMetadataCollection.NoStreamSegmentId);
    }

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a child (batch) StreamSegment.
     *
     * @param streamSegmentName     The name of the StreamSegment.
     * @param streamSegmentId       The Id of the StreamSegment.
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId) {
        Exceptions.throwIfNullOfEmpty(streamSegmentName, "streamSegmentName");
        Exceptions.throwIfIllegalArgument(streamSegmentId != SegmentMetadataCollection.NoStreamSegmentId, "streamSegmentId");

        this.traceObjectId = String.format("StreamSegment[%d]", streamSegmentId);
        this.name = streamSegmentName;
        this.streamSegmentId = streamSegmentId;
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.sealed = false;
        this.deleted = false;
        this.merged = false;
        this.storageLength = this.durableLogLength = -1;
        this.lastCommittedAppends = new HashMap<>();
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean isSealed() {
        return this.sealed;
    }

    @Override
    public boolean isDeleted() {
        return this.deleted;
    }

    @Override
    public long getLength() {
        return this.durableLogLength; // ReadableLength is essentially DurableLogLength.
    }

    @Override
    public Date getLastModified() {
        return new Date(); // TODO: implement properly; maybe change everytime durableLogLength changes...
    }

    //endregion

    //region SegmentMetadata Implementation

    @Override
    public long getId() {
        return this.streamSegmentId;
    }

    @Override
    public long getParentId() {
        return this.parentStreamSegmentId;
    }

    @Override
    public boolean isMerged() {
        return this.merged;
    }

    @Override
    public long getStorageLength() {
        return this.storageLength;
    }

    @Override
    public long getDurableLogLength() {
        return this.durableLogLength;
    }

    @Override
    public AppendContext getLastAppendContext(UUID clientId) {
        return this.lastCommittedAppends.getOrDefault(clientId, null);
    }

    @Override
    public Collection<UUID> getKnownClientIds() {
        return this.lastCommittedAppends.keySet();
    }

    @Override
    public String toString() {
        return String.format("Id = %d, StorageLength = %d, DLOffset = %d, Sealed = %s, Deleted = %s, Name = %s", getId(), getStorageLength(), getDurableLogLength(), isSealed(), isDeleted(), getName());
    }

    //endregion

    //region UpdateableSegmentMetadata Implementation

    @Override
    public void setStorageLength(long value) {
        Exceptions.throwIfIllegalArgument(value >= 0, "value", "Storage Length must be a non-negative number.");
        Exceptions.throwIfIllegalArgument(value >= this.storageLength, "value", "New Storage Length cannot be smaller than the previous one.");

        log.trace("{}: StorageLength changed from {} to {}.", this.traceObjectId, this.storageLength, value);
        this.storageLength = value;
    }

    @Override
    public void setDurableLogLength(long value) {
        Exceptions.throwIfIllegalArgument(value >= 0, "value", "Durable Log Length must be a non-negative number.");
        Exceptions.throwIfIllegalArgument(value >= this.durableLogLength, "value", "New Durable Log Length cannot be smaller than the previous one.");

        log.trace("{}: DurableLogLength changed from {} to {}.", this.traceObjectId, this.durableLogLength, value);
        this.durableLogLength = value;
    }

    @Override
    public void markSealed() {
        log.trace("{}: Sealed = true.", this.traceObjectId);
        this.sealed = true;
    }

    @Override
    public void markDeleted() {
        log.trace("{}: Deleted = true.", this.traceObjectId);
        this.deleted = true;
    }

    @Override
    public void markMerged() {
        Exceptions.throwIfIllegalState(this.parentStreamSegmentId != SegmentMetadataCollection.NoStreamSegmentId, "Cannot merge a non-batch StreamSegment.");

        log.trace("{}: Merged = true.", this.traceObjectId);
        this.merged = true;
    }

    @Override
    public void recordAppendContext(AppendContext appendContext) {
        this.lastCommittedAppends.put(appendContext.getClientId(), appendContext);
    }

    @Override
    public void copyFrom(SegmentMetadata base) {
        Exceptions.throwIfIllegalArgument(this.getId() == base.getId(), "base", "Given SegmentMetadata refers to a different StreamSegment than this one.");
        Exceptions.throwIfIllegalArgument(this.getParentId() == base.getParentId(), "base", "Given SegmentMetadata has a different parent StreamSegment than this one.");

        log.debug("{}: copyFrom {}.", this.traceObjectId, base.getClass().getSimpleName());
        setStorageLength(base.getStorageLength());
        setDurableLogLength(base.getDurableLogLength());
        for (UUID clientId : base.getKnownClientIds()) {
            recordAppendContext(base.getLastAppendContext(clientId));
        }

        if (base.isSealed()) {
            markSealed();
        }

        if (base.isMerged()) {
            markMerged();
        }

        if (base.isDeleted()) {
            markDeleted();
        }
    }

    //endregion
}
