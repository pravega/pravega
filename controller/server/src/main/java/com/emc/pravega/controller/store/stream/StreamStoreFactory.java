package com.emc.pravega.controller.store.stream;

import org.apache.commons.lang.NotImplementedException;

public class StreamStoreFactory {
    public enum StoreType {
        InMemory,
        Zookeeper,
        ECS,
        S3
    }

    public static StreamMetadataStore createStore(StoreType type, StoreConfiguration config){
        switch(type){
            case InMemory: return new InMemoryStreamStore();
            case Zookeeper:
            case ECS:
            case S3:
            default: throw new NotImplementedException();
        }
    }
}
