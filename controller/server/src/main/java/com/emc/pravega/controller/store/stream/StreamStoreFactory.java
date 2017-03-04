/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.ScheduledExecutorService;

public class StreamStoreFactory {
    public enum StoreType {
        InMemory,
        Zookeeper,
        ECS,
        S3,
        HDFS
    }

    public static StreamMetadataStore createStore(final StoreType type, ScheduledExecutorService executor) {
        switch (type) {
            case InMemory:
                return new InMemoryStreamMetadataStore(executor);
            case Zookeeper:
                return new ZKStreamMetadataStore(executor);
            case ECS:
            case S3:
            case HDFS:
            default:
                throw new NotImplementedException();
        }
    }
}
