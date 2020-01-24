/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor.impl;

import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;

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
