/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class SegmentMetadata implements StorageMetadata {
    public static final int ACTIVE  = 0x0001;
    public static final int SEALED  = 0x0002;
    public static final int DELETED = 0x0004;
    public static final int OWNERSHIP_CHANGED = 0x0008;

    String name;

    long length;

    long startOffset;

    int status;

    long maxRollinglength;

    String firstChunk;

    String lastChunk;

    long lastModified;

    long firstChunkStartOffset;

    long lastChunkStartOffset;

    long ownerEpoch;

    @Override
    public String getKey() {
        return name;
    }

    @Override
    public StorageMetadata copy() {
        return toBuilder().build();
    }

    public SegmentMetadata setActive(boolean value) {
        return setFlag(ACTIVE, value);
    }

    public SegmentMetadata setSealed(boolean value) {
        return setFlag(SEALED, value);
    }

    public SegmentMetadata setOwnershipChanged(boolean value) {
        return setFlag(OWNERSHIP_CHANGED, value);
    }

    public boolean isActive() {
        return getFlag(ACTIVE);
    }

    public boolean isSealed() {
        return getFlag(SEALED);
    }

    public boolean isOwnershipChanged() {
        return getFlag(OWNERSHIP_CHANGED);
    }

    public void checkInvariants() {
        Preconditions.checkState(length >= 0);
        Preconditions.checkState(startOffset >= 0);
        Preconditions.checkState(firstChunkStartOffset >= 0);
        Preconditions.checkState(lastChunkStartOffset >= 0);
        Preconditions.checkState(firstChunkStartOffset <= startOffset);
        Preconditions.checkState(length >= lastChunkStartOffset);
        Preconditions.checkState(firstChunkStartOffset <= lastChunkStartOffset);
    }

    private SegmentMetadata setFlag(int mask, boolean value) {
        status = value ? (status | mask ) : (status & (~mask));
        return this;
    }

    private boolean getFlag(int mask) {
        return (status & mask) != 0;
    }
}
