/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import java.util.Collection;

/**
 * Log Operation that represents a StreamSegment Append. As opposed from StreamSegmentAppendOperation, this operation cannot
 * be serialized to a DurableLog. Its purpose is to be added to the In-Memory Transaction Log, where it binds a StreamSegmentAppendOperation
 * to its corresponding Cache entry.
 */
public class CachedStreamSegmentAppendOperation extends StorageOperation implements AttributeUpdaterOperation {
    //region Members

    private final int length;
    private final long streamSegmentOffset;
    private final Collection<AttributeUpdate> attributeUpdates;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CachedStreamSegmentAppendOperation based on the given StreamSegmentAppendOperation.
     * The created operation will have the same SequenceNumber, StreamSegmentId, Offset and Length as the base operation,
     * but it will not directly store the data (the contents of the Append is stored in the Cache, and will have to be
     * retrieved using properties of this object).
     *
     * @param baseOperation The StreamSegmentAppendOperation to use.
     */
    public CachedStreamSegmentAppendOperation(StreamSegmentAppendOperation baseOperation) {
        super(baseOperation.getStreamSegmentId());
        Preconditions.checkArgument(baseOperation.getStreamSegmentOffset() >= 0, "given baseOperation does not have an assigned StreamSegment Offset.");

        this.streamSegmentOffset = baseOperation.getStreamSegmentOffset();
        this.length = baseOperation.getData().getLength();
        if (baseOperation.getSequenceNumber() >= 0) {
            setSequenceNumber(baseOperation.getSequenceNumber());
        }

        this.attributeUpdates = baseOperation.getAttributeUpdates();
    }

    //endregion

    //region Properties

    /**
     * Gets the Attribute updates for this StreamSegmentAppendOperation, if any.
     *
     * @return A Collection of Attribute updates, or null if no updates are available.
     */
    @Override
    public Collection<AttributeUpdate> getAttributeUpdates() {
        return this.attributeUpdates;
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Offset = %d, Length = %d, Attributes = %d",
                super.toString(),
                this.streamSegmentOffset,
                this.length,
                this.attributeUpdates == null ? 0 : this.attributeUpdates.size());
    }

    //endregion

    //region Operation Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    //endregion
}
