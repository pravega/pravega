package com.emc.logservice;

import java.util.Date;

/**
 * General Stream Segment Information.
 */
public class StreamSegmentInformation {
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

    //region Properties

    /**
     * Gets a value indicating the name of the StreamSegment.
     *
     * @return
     */
    public String getName() {
        return this.streamSegmentName;
    }

    /**
     * Gets a value indicating whether the StreamSegment is sealed (for modifications).
     *
     * @return
     */
    public boolean isSealed() {
        return this.sealed;
    }

    /**
     * Gets a value indicating whether the StreamSegment is deleted (not exists).
     *
     * @return
     */
    public boolean isDeleted() {
        return this.deleted;
    }

    /**
     * Gets a value indicating the last modification time of the StreamSegment.
     *
     * @return
     */
    public Date getLastModified() {
        return this.lastModified;
    }

    /**
     * Gets a value indicating the length of the StreamSegment.
     *
     * @return
     */
    public long getLength() {
        return this.length;
    }

    @Override
    public String toString() {
        return String.format("Name = %s, Length = %d, Sealed = %s, Deleted = %s, LastModified = %s", getName(), getLength(), isSealed(), isDeleted(), getLastModified());
    }

    //endregion
}
