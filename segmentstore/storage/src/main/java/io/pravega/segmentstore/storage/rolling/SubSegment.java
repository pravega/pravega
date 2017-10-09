/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;

/**
 * Represents a range of bytes within a Segment.
 */
@ThreadSafe
class SubSegment {
    //region Private

    /**
     * The name of the SubSegment.
     */
    @Getter
    private final String name;
    /**
     * The offset within the owning Segment where this SubSegment starts.
     */
    @Getter
    private final long startOffset;
    @GuardedBy("this")
    private long length;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private boolean exists = true;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SubSegment class.
     *
     * @param subSegmentName The name of this SubSegment (not of the owning Segment).
     * @param startOffset    The offset within the owning Segment where this SubSegment starts at.
     */
    SubSegment(String subSegmentName, long startOffset) {
        this.name = Exceptions.checkNotNullOrEmpty(subSegmentName, "subSegmentName");
        Preconditions.checkArgument(startOffset >= 0, "startOffset must be a non-negative number.");
        this.startOffset = startOffset;
    }

    /**
     * Creates a new instance of the SubSegment class.
     *
     * @param segmentName The name of the owning Segment (not the name of this SubSegment).
     * @param startOffset The offset within the owning Segment where this SubSegment starts at.
     * @return A new SubSegment.
     */
    static SubSegment forSegment(String segmentName, long startOffset) {
        return new SubSegment(StreamSegmentNameUtils.getSubSegmentName(segmentName, startOffset), startOffset);
    }

    /**
     * Creates a new instance of the SubSegment class with the same information as this one, but with a new offset.
     *
     * @param newOffset The new offset.
     * @return A new SubSegment.
     */
    SubSegment withNewOffset(long newOffset) {
        SubSegment ns = new SubSegment(this.name, newOffset);
        ns.setLength(getLength());
        if (isSealed()) {
            ns.markSealed();
        }

        if (!exists()) {
            ns.markInexistent();
        }

        return ns;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating whether this SubSegment has been sealed.
     */
    synchronized boolean isSealed() {
        return this.sealed;
    }

    /**
     * Records the fact that this SubSegment has been sealed.
     */
    synchronized void markSealed() {
        this.sealed = true;
    }

    /**
     * Gets a value indicating whether this SubSegment exists or not.
     */
    synchronized boolean exists() {
        return this.exists;
    }

    /**
     * Records the fact that this SubSegment no longer exists.
     */
    synchronized void markInexistent() {
        this.exists = false;
    }

    /**
     * Gets a value indicating the current length of this SubSegment.
     */
    synchronized long getLength() {
        return this.length;
    }

    /**
     * Increases the length of the SubSegment by the given delta.
     * @param delta The value to increase by.
     */
    synchronized void increaseLength(long delta) {
        Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SubSegment.");
        Preconditions.checkArgument(delta >= 0, "Cannot decrease the length of a SubSegment.");
        this.length += delta;
    }

    /**
     * Sets the length of the SubSegment.
     * @param length The new length.
     */
    synchronized void setLength(long length) {
        Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SubSegment.");
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");
        this.length = length;
    }

    /**
     * Gets a value indicating the last Offset of this SubSegment.
     */
    synchronized long getLastOffset() {
        return this.startOffset + this.length;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s (%d+%d%s)", this.name, this.startOffset, this.length, this.sealed ? ", sealed" : "");
    }

    //endregion
}