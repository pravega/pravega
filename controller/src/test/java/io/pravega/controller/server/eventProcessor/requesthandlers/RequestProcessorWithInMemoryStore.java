/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import org.junit.After;
import org.junit.Before;

public class RequestProcessorWithInMemoryStore extends RequestProcessorTest {
    private StreamMetadataStore store;

    @Before
    public void setUp() {
        store = StreamStoreFactory.createInMemoryStore(executorService());
    }

    @After
    public void tearDown() throws Exception {
        store.close();
    }

    @Override
    StreamMetadataStore getStore() {
        return store;
    }
}
