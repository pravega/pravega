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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;

@ThreadSafe
class RollingSegmentHandle implements SegmentHandle {
    //region Members

    /**
     * The name of the Segment for this Handle.
     */
    @Getter
    private final String segmentName;
    /**
     * A pointer to the Handle for this Segment's Header.
     */
    @Getter
    private final SegmentHandle headerHandle;
    /**
     * The Rolling Policy for this Segment.
     */
    @Getter
    private final SegmentRollingPolicy rollingPolicy;
    @GuardedBy("this")
    private int headerLength;
    @GuardedBy("this")
    private List<SubSegment> subSegments;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private SegmentHandle activeSubSegmentHandle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingSegmentHandle class.
     *
     * @param headerHandle  A SegmentHandle for the Header SubSegment.
     * @param rollingPolicy The Rolling Policy to apply for this Segment.
     * @param subSegments   A ordered list of initial SubSegments for this handle.
     */
    RollingSegmentHandle(SegmentHandle headerHandle, SegmentRollingPolicy rollingPolicy, List<SubSegment> subSegments) {
        this.headerHandle = Preconditions.checkNotNull(headerHandle, "headerHandle");
        this.segmentName = StreamSegmentNameUtils.getSegmentNameFromHeader(headerHandle.getSegmentName());
        Exceptions.checkNotNullOrEmpty(this.segmentName, "headerHandle.getSegmentName()");
        this.rollingPolicy = rollingPolicy == null ? SegmentRollingPolicy.NO_ROLLING : rollingPolicy;
        this.subSegments = Preconditions.checkNotNull(subSegments, "subSegments");
    }

    //endregion

    /**
     * Updates the contents of this handle with information from the given one.
     *
     * @param source The RollingSegmentHandle to update from.
     */
    synchronized void refresh(RollingSegmentHandle source) {
        Preconditions.checkState(isReadOnly(), "Cannot refresh a non-readonly handle.");
        Preconditions.checkArgument(source.getSegmentName().equals(this.getSegmentName()), "SegmentName mismatch.");
        this.subSegments = new ArrayList<>(source.subSegments());
        setHeaderLength(source.getHeaderLength());
        setActiveSubSegmentHandle(source.getActiveSubSegmentHandle());
        if (source.isSealed()) {
            markSealed();
        }
    }

    //region Properties

    @Override
    public boolean isReadOnly() {
        return this.headerHandle.isReadOnly();
    }

    /**
     * Records the fact that the Segment represented by this Handle has been sealed.
     */
    synchronized void markSealed() {
        if (!this.sealed) {
            this.sealed = true;
            this.subSegments = Collections.unmodifiableList(this.subSegments);
            setActiveSubSegmentHandle(null);
        }
    }

    /**
     * Gets a value indicating whether the Segment represented by this Handle is sealed.
     */
    synchronized boolean isSealed() {
        return this.sealed;
    }

    /**
     * Gets a pointer to the last SubSegment.
     *
     * @return The last SubSegment, or null if no SubSegments exist.
     */
    synchronized SubSegment lastSubSegment() {
        return this.subSegments.size() == 0 ? null : this.subSegments.get(this.subSegments.size() - 1);
    }

    /**
     * Gets an unmodifiable List of all current SubSegments for this Handle. If the Segment is not sealed, a copy of the
     * current SubSegments is returned (since they may change in the future).
     *
     * @return A List with SubSegments.
     */
    synchronized List<SubSegment> subSegments() {
        if (this.sealed) {
            return this.subSegments; // This is already an unmodifiable list.
        } else {
            return Collections.unmodifiableList(this.subSegments.subList(0, this.subSegments.size()));
        }
    }

    /**
     * Adds a new SubSegment.
     *
     * @param subSegment             The SubSegment to add. This SubSegment must be in continuity of any existing SubSegments.
     * @param activeSubSegmentHandle The newly added SubSegment's write handle.
     */
    synchronized void addSubSegment(SubSegment subSegment, SegmentHandle activeSubSegmentHandle) {
        if (this.subSegments.size() > 0) {
            long expectedOffset = this.subSegments.get(this.subSegments.size() - 1).getLastOffset();
            Preconditions.checkArgument(subSegment.getStartOffset() == expectedOffset,
                    "Invalid SubSegment StartOffset. Expected %s, given %s.", expectedOffset, subSegment.getStartOffset());
        }

        // Update the SubSegment and its Handle atomically.
        Preconditions.checkNotNull(activeSubSegmentHandle, "activeSubSegmentHandle");
        Preconditions.checkArgument(!activeSubSegmentHandle.isReadOnly(), "Active SubSegment handle cannot be readonly.");
        Preconditions.checkArgument(activeSubSegmentHandle.getSegmentName().equals(subSegment.getName()),
                "Active SubSegment handle must be for the last SubSegment.");
        this.activeSubSegmentHandle = activeSubSegmentHandle;
        this.subSegments.add(subSegment);
    }

    /**
     * Adds multiple SubSegments.
     *
     * @param subSegments The SubSegments to add. These SubSegments must be in continuity of any existing SubSegments.
     */
    synchronized void addSubSegments(List<SubSegment> subSegments) {
        if (this.subSegments.size() > 0) {
            long expectedOffset = this.subSegments.get(this.subSegments.size() - 1).getLastOffset();
            for (SubSegment s : subSegments) {
                Preconditions.checkArgument(s.getStartOffset() == expectedOffset,
                        "Invalid SubSegment StartOffset. Expected %s, given %s.", expectedOffset, s.getStartOffset());
                expectedOffset += s.getLength();
            }
        }

        this.subSegments.addAll(subSegments);
        this.activeSubSegmentHandle = null;
    }

    /**
     * Gets a value indicating the current length of the Segment, in bytes.
     *
     * @return The length.
     */
    synchronized long length() {
        SubSegment lastSubSegment = lastSubSegment();
        return lastSubSegment == null ? 0L : lastSubSegment.getLastOffset();
    }

    /**
     * Gets a pointer to the Active SubSegment Handle.
     *
     * @return The handle.
     */
    synchronized SegmentHandle getActiveSubSegmentHandle() {
        return this.activeSubSegmentHandle;
    }

    /**
     * Sets the Active SubSegment handle.
     *
     * @param handle The handle. Must not be read-only and for the last SubSegment.
     *               @return This object.
     */
    synchronized RollingSegmentHandle setActiveSubSegmentHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle == null || !handle.isReadOnly(), "Active SubSegment handle cannot be readonly.");
        Preconditions.checkArgument(handle == null || handle.getSegmentName().equals(lastSubSegment().getName()),
                "Active SubSegment handle must be for the last SubSegment.");
        this.activeSubSegmentHandle = handle;
        return this;
    }

    /**
     * Gets a value indicating the serialized length of the Header.
     */
    synchronized int getHeaderLength() {
        return this.headerLength;
    }

    /**
     * Sets the serialized length of the Header.
     */
    synchronized RollingSegmentHandle setHeaderLength(int value) {
        this.headerLength = value;
        return this;
    }

    /**
     * Increases the serialized length of the Header by the given value.
     */
    synchronized RollingSegmentHandle increaseHeaderLength(int value) {
        this.headerLength += value;
        return this;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s (%s, %s, SubSegments=%d", this.segmentName, this.sealed ? "Sealed" : "Not Sealed",
                isReadOnly() ? "R" : "RW", this.subSegments.size());
    }

    //endregion
}
