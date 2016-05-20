package com.emc.logservice;

import java.util.Date;

/**
 * General Stream Segment Information.
 */
public class StreamSegmentInformation implements SegmentProperties {
    //region Members

    private final long length;
    private final boolean sealed;
    private final boolean deleted;
    private final Date lastModified;
    private final String streamSegmentName;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentInformation class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param length            The length of the StreamSegment.
     * @param isSealed          Whether the StreamSegment is sealed (for modifications).
     * @param isDeleted         Whether the StreamSegment is deleted (does not exist).
     * @param lastModified      The last time the StreamSegment was modified.
     */
    public StreamSegmentInformation(String streamSegmentName, long length, boolean isSealed, boolean isDeleted, Date lastModified) {
        this.length = length;
        this.sealed = isSealed;
        this.deleted = isDeleted;
        this.lastModified = lastModified;
        this.streamSegmentName = streamSegmentName;
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public String getName() {
        return this.streamSegmentName;
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
    public Date getLastModified() {
        return this.lastModified;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    //endregion

    //region Properties

    @Override
    public String toString() {
        return String.format("Name = %s, Length = %d, Sealed = %s, Deleted = %s, LastModified = %s", getName(), getLength(), isSealed(), isDeleted(), getLastModified());
    }

    //endregion
}
