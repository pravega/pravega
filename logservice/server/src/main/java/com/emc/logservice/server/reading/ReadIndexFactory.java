package com.emc.logservice.server.reading;

import com.emc.logservice.server.*;

/**
 * Default implementation for CacheFactory.
 */
public class ReadIndexFactory implements CacheFactory {

    @Override
    public Cache createCache(ContainerMetadata containerMetadata) {
        return new ReadIndex(containerMetadata, containerMetadata.getContainerId());
    }
}
