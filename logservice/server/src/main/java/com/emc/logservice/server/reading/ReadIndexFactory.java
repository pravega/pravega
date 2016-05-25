package com.emc.logservice.server.reading;

import com.emc.logservice.server.*;

/**
 * Default implementation for CacheFactory.
 */
public class ReadIndexFactory implements CacheFactory {

    @Override
    public Cache createCache(SegmentMetadataCollection segmentMetadataCollection) {
        return new ReadIndex(segmentMetadataCollection);
    }
}
