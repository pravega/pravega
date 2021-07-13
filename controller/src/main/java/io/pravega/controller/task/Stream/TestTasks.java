/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.task.Stream;

import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.task.Task;
import io.pravega.controller.task.TaskBase;

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
