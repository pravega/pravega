/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.task.Stream;

import io.pravega.server.controller.service.store.task.Resource;
import io.pravega.server.controller.service.store.task.TaskMetadataStore;
import io.pravega.server.controller.service.task.Task;
import io.pravega.server.controller.service.task.TaskBase;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Set of tasks for test purposes.
 */
public class TestTasks extends TaskBase {

    public TestTasks(TaskMetadataStore taskMetadataStore, ScheduledExecutorService executor, String hostId) {
        super(taskMetadataStore, executor, hostId);
        this.setReady();
    }

    public TestTasks(TaskMetadataStore taskMetadataStore, ScheduledExecutorService executor, Context context) {
        super(taskMetadataStore, executor, context);
        this.setReady();
    }

    @Task(name = "test", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<Void> testStreamLock(String scope, String stream) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream},
                () -> {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    return  CompletableFuture.completedFuture(null);
                });
    }

    @Override
    public TaskBase copyWithContext(Context context) {
        return new TestTasks(taskMetadataStore, executor, context);
    }

    @Override
    public void close() throws Exception {
    }
}
