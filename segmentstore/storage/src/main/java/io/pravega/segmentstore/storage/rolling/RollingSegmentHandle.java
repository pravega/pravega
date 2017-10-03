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
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;

@ThreadSafe
class RollingSegmentHandle implements SegmentHandle {
    //region Members

    @Getter
    private final String segmentName;
    @Getter
    private final SegmentHandle headerHandle;
    @Getter
    private final RollingPolicy rollingPolicy;
    @GuardedBy("this")
    private int serializedHeaderLength;
    @GuardedBy("this")
    private final List<SubSegment> subSegments;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private SegmentHandle activeSubSegmentHandle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingSegmentHandle class.
     *
     * @param segmentName   The name of the Segment in this Handle, as perceived by users of the Storage interface.
     * @param headerHandle  A SegmentHandle for the Header SubSegment.
     * @param rollingPolicy The Rolling Policy to apply for this Segment.
     * @param subSegments   A ordered list of initial SubSegments for this handle.
     */
    RollingSegmentHandle(String segmentName, SegmentHandle headerHandle, RollingPolicy rollingPolicy, List<SubSegment> subSegments) {
        this.segmentName = Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        this.headerHandle = Preconditions.checkNotNull(headerHandle, "headerHandle");
        this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy, "rollingPolicy");
        this.subSegments = Preconditions.checkNotNull(subSegments, "subSegments");
    }

    //endregion

    @Override
    public boolean isReadOnly() {
        return this.headerHandle.isReadOnly();
    }

    synchronized void forEachSubSegment(Consumer<SubSegment> consumer) {
        this.subSegments.forEach(consumer);
    }

    synchronized void markSealed() {
        this.sealed = true;
    }

    synchronized boolean isSealed() {
        return this.sealed;
    }

    synchronized void addSubSegment(SubSegment subSegment) {
        if (this.subSegments.size() > 0) {
            long expectedOffset = this.subSegments.get(this.subSegments.size() - 1).getLastOffset();
            Preconditions.checkArgument(subSegment.getStartOffset() == expectedOffset,
                    "Invalid SubSegment StartOffset. Expected %s, given %s.", expectedOffset, subSegment.getStartOffset());
        }

        this.subSegments.add(subSegment);
    }

    synchronized SubSegment getLastSubSegment() {
        return this.subSegments.size() == 0 ? null : this.subSegments.get(this.subSegments.size() - 1);
    }

    synchronized void setActiveSubSegmentHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle == null || !handle.isReadOnly(), "Active segment handle cannot be readonly.");
        this.activeSubSegmentHandle = handle;
    }

    synchronized SegmentHandle getActiveSubSegmentHandle() {
        return this.activeSubSegmentHandle;
    }

    synchronized int getSerializedHeaderLength() {
        return this.serializedHeaderLength;
    }

    synchronized void setSerializedHeaderLength(int value) {
        this.serializedHeaderLength = value;
    }

    synchronized void increaseSerializedHeaderLength(int value) {
        this.serializedHeaderLength += value;
    }
}
