package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.SegmentHandle;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@ThreadSafe
class RollingStorageHandle implements SegmentHandle {
    //region Members

    @Getter
    private final String segmentName;
    @Getter
    private final SegmentHandle headerHandle;
    @Getter
    private final RollingPolicy rollingPolicy;
    @GuardedBy("subSegments")
    private final List<SubSegment> subSegments;
    @GuardedBy("subSegments")
    private boolean sealed;
    private SegmentHandle activeSubSegmentHandle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingStorageHandle class.
     *
     * @param segmentName   The name of the Segment in this Handle, as perceived by users of the Storage interface.
     * @param headerHandle  A SegmentHandle for the Header SubSegment.
     * @param rollingPolicy The Rolling Policy to apply for this Segment.
     * @param subSegments   A ordered list of initial SubSegments for this handle.
     */
    RollingStorageHandle(String segmentName, SegmentHandle headerHandle, RollingPolicy rollingPolicy, List<SubSegment> subSegments) {
        this.segmentName = Exceptions.checkNotNullOrEmpty(segmentName, "segmentName");
        this.headerHandle = Preconditions.checkNotNull(headerHandle, "headerHandle");
        this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy, "rollingPolicy");
        this.subSegments = Exceptions.checkNotNullOrEmpty(subSegments, "subSegments");
    }

    //endregion
    @Override
    public boolean isReadOnly() {
        return this.headerHandle.isReadOnly();
    }

    void forEachSubSegment(Consumer<SubSegment> consumer) {
        synchronized (this.subSegments) {
            this.subSegments.forEach(consumer);
        }
    }

    void markSealed() {
        synchronized (this.subSegments) {
            this.sealed = true;
        }
    }

    SubSegment getLastSubSegment() {
        synchronized (this.subSegments) {
            return this.subSegments.size() == 0 ? null : this.subSegments.get(this.subSegments.size() - 1);
        }
    }

    void setActiveSubSegmentHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle == null || !handle.isReadOnly(), "Active segment handle cannot be readonly.");
        synchronized (this.subSegments) {
            this.activeSubSegmentHandle = handle;
        }
    }

    SegmentHandle getActiveSubSegmentHandle() {
        synchronized (this.subSegments) {
            return this.activeSubSegmentHandle;
        }
    }

    //region SubSegment

    @ThreadSafe
    @RequiredArgsConstructor
    static class SubSegment {
        @Getter
        private final String name;
        @Getter
        private final long startOffset;
        @GuardedBy("this")
        private long length;
        @GuardedBy("this")
        private boolean sealed;

        synchronized void markSealed() {
            this.sealed = true;
        }

        synchronized boolean isSealed() {
            return this.sealed;
        }

        synchronized void increaseLength(int delta) {
            Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SubSegment.");
            this.length += delta;
        }

        synchronized void setLength(long length) {
            Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SubSegment.");
            Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");
            this.length = length;
        }

        synchronized long getLength() {
            return this.length;
        }

        @Override
        public synchronized String toString() {
            return String.format("%s (%d%s)", this.name, this.length, this.sealed ? ", sealed" : "");
        }
    }

    //endregion
}
