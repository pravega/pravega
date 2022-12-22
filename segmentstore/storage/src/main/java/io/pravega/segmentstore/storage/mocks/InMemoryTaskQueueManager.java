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
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.chunklayer.AbstractTaskQueueManager;
import io.pravega.segmentstore.storage.chunklayer.GarbageCollector;
import lombok.Getter;
import lombok.val;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryTaskQueueManager implements AbstractTaskQueueManager<GarbageCollector.TaskInfo> {
    @Getter
    private final ConcurrentHashMap<String, LinkedBlockingQueue<GarbageCollector.TaskInfo>> taskQueueMap = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> addQueue(String queueName, Boolean ignoreProcessing) {
        taskQueueMap.putIfAbsent(queueName, new LinkedBlockingQueue<GarbageCollector.TaskInfo>());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> addTask(String queueName, GarbageCollector.TaskInfo task) {
        val queue = taskQueueMap.get(queueName);
        Preconditions.checkState(null != queue, "Attempt to access non existent queue.");
        queue.add(task);
        return CompletableFuture.completedFuture(null);
    }

    public ArrayList<GarbageCollector.TaskInfo> drain(String queueName, int maxElements) {
        val list = new ArrayList<GarbageCollector.TaskInfo>();
        val queue = taskQueueMap.get(queueName);
        Preconditions.checkState(null != queue, "Attempt to access non existent queue.");
        queue.drainTo(list, maxElements);
        return list;
    }

    @Override
    public void close() throws Exception {

    }
}
