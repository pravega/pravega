/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.store.stream.records.StateRecord;

import java.util.Optional;

public class StreamMetadataStoreTestHelper {
    public static PersistentStreamBase getStream(StreamMetadataStore store, String scopeName, String streamName) {
        return (PersistentStreamBase) ((AbstractStreamMetadataStore) store)
                .newStream(scopeName, streamName);
    }
    
    public static void partiallyCreateStream(StreamMetadataStore store, String scopeName, String streamName, 
                                             Optional<Long> creationTime, boolean createState) {
        addStreamToScope(store, scopeName, streamName);
        PersistentStreamBase stream = getStream(store, scopeName, streamName);
        stream.createStreamMetadata().join();
        creationTime.map(x -> stream.storeCreationTimeIfAbsent(x).join());
        if (createState) {
            stream.createStateIfAbsent(StateRecord.builder().state(State.CREATING).build()).join();
        }
    }
    
    public static void addStreamToScope(StreamMetadataStore store, String scopeName, String streamName) {
        if (store instanceof InMemoryStreamMetadataStore) {
            ((InMemoryStreamMetadataStore) store).addStreamObjToScope(scopeName, streamName);
        } else if (store instanceof PravegaTablesStreamMetadataStore) {
            ((PravegaTablesScope) ((PravegaTablesStreamMetadataStore) store).getScope(scopeName))
                    .addStreamToScope(streamName).join();
        } else if (store instanceof ZKStreamMetadataStore) {
            ((ZKStream) ((ZKStreamMetadataStore) store).getStream(scopeName, streamName, null))
                    .createStreamMetadata().join();
        }
    }
}
