package com.emc.logservice.reading;

import com.emc.logservice.*;

/**
 * Default implementation for CacheFactory.
 */
public class ReadIndexFactory implements CacheFactory {

    @Override
    public Cache createCache(SegmentMetadataCollection segmentMetadataCollection) {
        return new ReadIndex(segmentMetadataCollection);
    }
}
