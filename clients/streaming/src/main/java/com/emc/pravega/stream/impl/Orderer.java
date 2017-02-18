/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import java.util.List;

/**
 * Used to select which event should go next when consuming from multiple segments.
 *
 */
public interface Orderer {

    /**
     * Given a list of segments this reader owns, (which contain their positions) returns the one that
     * should be read from next. This is done in a consistent way. i.e. Calling this method with the
     * same readers at the same positions, should yield the same result. (The passed collection is
     * not modified)
     *
     * @param segments The logs to get the next reader for.
     * @return a segment that this reader should read from next.
     */
    SegmentEventReader nextSegment(List<SegmentEventReader> segments);
}
