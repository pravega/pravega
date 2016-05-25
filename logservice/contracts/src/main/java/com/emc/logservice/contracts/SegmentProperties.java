package com.emc.logservice.contracts;

import java.util.Date;

/**
 * General properties about a StreamSegment.
 */
public interface SegmentProperties {
    /**
     * Gets a value indicating the name of this StreamSegment.
     *
     * @return
     */
    String getName();

    /**
     * Gets a value indicating whether this StreamSegment is sealed for modifications.
     *
     * @return
     */
    boolean isSealed();

    /**
     * Gets a value indicating whether this StreamSegment is deleted (does not exist).
     *
     * @return
     */
    boolean isDeleted();

    /**
     * Gets a value indicating the last modification time of the StreamSegment.
     *
     * @return
     */
    Date getLastModified();

    /**
     * Gets a value indicating the full, readable length of the StreamSegment.
     *
     * @return
     */
    long getLength();
}

