/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

/**
 * Defines various types of Read Result Entries, based on where their data is located.
 */
public enum ReadResultEntryType {
    /**
     * The ReadResultEntry points to a location in the Cache, and data is readily available.
     */
    Cache,

    /**
     * The ReadResultEntry points to a location in Storage, and data will need to be retrieved from there in order
     * to make use of it.
     */
    Storage,

    /**
     * The ReadResultEntry points to a location beyond the end offset of the StreamSegment. It will not be able to return
     * anything until such data is appended to the StreamSegment.
     */
    Future,

    /**
     * The ReadResultEntry indicates that the End of the StreamSegment has been reached. No data can be consumed
     * from it and no further reading can be done on this StreamSegment from its position.
     */
    EndOfStreamSegment
}
