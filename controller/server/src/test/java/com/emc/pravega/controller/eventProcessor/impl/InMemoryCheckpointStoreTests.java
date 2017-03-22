/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.store.checkpoint.CheckpointStoreFactory;

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
