/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.task;

import com.emc.pravega.controller.store.task.TaskStoreFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In memory task metadata store tests.
 */
public class InMemoryTaskMetadataStoreTests extends TaskMetadataStoreTests {

    @Override
    public void setupTaskStore() throws Exception {
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
        taskMetadataStore = TaskStoreFactory.createInMemoryStore(executor);
    }

    @Override
    public void cleanupTaskStore() throws IOException {

    }
}
