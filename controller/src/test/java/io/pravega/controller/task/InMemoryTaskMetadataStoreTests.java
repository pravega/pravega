/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.store.task.TaskStoreFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In memory task metadata store tests.
 */
public class InMemoryTaskMetadataStoreTests extends TaskMetadataStoreTests {

    private ScheduledExecutorService executor;

    @Override
    public void setupTaskStore() throws Exception {
        executor = Executors.newScheduledThreadPool(10);
        taskMetadataStore = TaskStoreFactory.createInMemoryStore(executor);
    }

    @Override
    public void cleanupTaskStore() throws IOException {
        if (executor != null) {
            ExecutorServiceHelpers.shutdown(executor);
        }
    }
}
