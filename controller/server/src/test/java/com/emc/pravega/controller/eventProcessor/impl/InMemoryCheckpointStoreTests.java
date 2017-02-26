/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

/**
 * Tests for in-memory checkpoint store.
 */
public class InMemoryCheckpointStoreTests extends CheckpointStoreTests {
    @Override
    public void setupCheckpointStore() {
        this.checkpointStore = new InMemoryCheckpointStore();
    }

    @Override
    public void cleanupCheckpointStore() {

    }
}
