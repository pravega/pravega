/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import java.util.Collection;

/**
 * Defines a Container for StreamSegments.
 */
public interface SegmentContainer extends StreamSegmentStore, Container {
    /**
     * Gets a collection of SegmentProperties for all active Segments (Active Segment = a segment that is currently allocated
     * in the internal Container's Metadata (usually a segment with recent activity)).
     *
     * @return A Collection with the SegmentProperties for these segments.
     */
    Collection<SegmentProperties> getActiveSegments();
}
