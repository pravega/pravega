/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.eventProcessor.impl;

import io.pravega.server.controller.service.store.checkpoint.CheckpointStoreFactory;

/**
 * Tests for in-memory checkpoint store.
 */
public class InMemoryCheckpointStoreTests extends CheckpointStoreTests {
    @Override
    public void setupCheckpointStore() {
        this.checkpointStore = CheckpointStoreFactory.createInMemoryStore();
    }

    @Override
    public void cleanupCheckpointStore() {

    }
}
