/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.server.segmentstore.storage;

/**
 * Defines a Handle that can be used to operate on Segments in Storage.
 */
public interface SegmentHandle {
    /**
     * Gets the name of the Segment, as perceived by users of the Storage interface.
     */
    String getSegmentName();

    /**
     * Gets a value indicating whether this Handle was open in ReadOnly mode (true) or ReadWrite mode (false).
     */
    boolean isReadOnly();
}
