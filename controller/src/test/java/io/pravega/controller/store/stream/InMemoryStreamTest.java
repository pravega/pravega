/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

public class InMemoryStreamTest extends StreamTestBase {
    InMemoryStreamMetadataStore store;
    @Override
    public void setup() throws Exception {
        store = new InMemoryStreamMetadataStore(executor);
    }

    @Override
    public void tearDown() throws Exception {
        store.close();
    }

    @Override
    void createScope(String scope) {
        store.createScope(scope);
    }

    @Override
    PersistentStreamBase getStream(String scope, String stream, int chunkSize, int shardSize) {
        return new InMemoryStream(scope, stream, chunkSize, shardSize);
    }
}
