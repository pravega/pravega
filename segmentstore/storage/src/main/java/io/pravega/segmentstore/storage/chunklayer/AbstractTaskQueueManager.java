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

package io.pravega.segmentstore.storage.chunklayer;

import java.util.concurrent.CompletableFuture;

/**
 * Manages group of background task queues.
 *
 * @param <TaskType> Type of tasks.
 */
public interface AbstractTaskQueueManager<TaskType> extends AutoCloseable {
    /**
     * Adds a queue by the given name.
     *
     * @param queueName        Name of the queue.
     * @param ignoreProcessing Whether the processing should be ignored.
     */
    CompletableFuture<Void> addQueue(String queueName, Boolean ignoreProcessing);

    /**
     * Adds a task to queue.
     *
     * @param queueName Name of the queue.
     * @param task      Task to add.
     */
    CompletableFuture<Void> addTask(String queueName, TaskType task);
}
