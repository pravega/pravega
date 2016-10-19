/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.task;

import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

/**
 * TaskBase contains the following.
 * 1. Environment variables used by tasks.
 * 2. Wrapper method that has boilerplate code for locking, persisting task data and executing the task
 *
 * Actual tasks are implemented in sub-classes of TaskBase and annotated with @Task annotation.
 */
public class TaskBase {

    public interface FutureOperation<T> {
        CompletableFuture<T> apply();
    }

    protected final StreamMetadataStore streamMetadataStore;
    protected final HostControllerStore hostControllerStore;
    protected ConnectionFactoryImpl connectionFactory;
    private final TaskMetadataStore taskMetadataStore;

    public TaskBase(StreamMetadataStore streamMetadataStore,
                    HostControllerStore hostControllerStore,
                    TaskMetadataStore taskMetadataStore) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.taskMetadataStore = taskMetadataStore;
    }

    /**
     * Wrapper method that initially obtains lock, persists the task data then executes the passed method,
     * finally deletes task data and releases lock
     *
     * @param resource resource to be updated by the task
     * @param parameters method parameters
     * @param operation lambda operation that is the actual task
     * @param <T> type parameter
     * @return return value of task execution
     */
    public <T> CompletableFuture<T> execute(String resource, Serializable[] parameters, FutureOperation<T> operation, String oldHost) {
        TaskData taskData = getTaskData(parameters);
        return taskMetadataStore.putChild(resource)
                .thenCompose(x -> executeTask(resource, taskData, operation, oldHost))
                .thenCombine(taskMetadataStore.removeChild(resource), (y, z) -> y);
    }

    private <T> CompletableFuture<T> executeTask(String resource, TaskData taskData, FutureOperation<T> operation, String oldHost) {
        return taskMetadataStore.lock(resource, oldHost)
                .thenCompose(x -> taskMetadataStore.put(resource, taskData.serialize())
                        .thenCompose(y -> operation.apply())
                        .thenCombine(taskMetadataStore.remove(resource), (y, z) -> y))
                .thenCombine(taskMetadataStore.unlock(resource), (y, z) -> y);
    }

    private TaskData getTaskData(Serializable[] parameters) {
        TaskData taskData = new TaskData();
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement e = stacktrace[3];
        Task annotation = getTaskAnnotation(e.getMethodName());
        taskData.setMethodName(annotation.name());
        taskData.setMethodVersion(annotation.version());
        taskData.setParameters(parameters);
        return taskData;
    }

    private Task getTaskAnnotation(String method) {
        for (Method m : this.getClass().getMethods()) {
            if (m.getName().equals(method)) {
                for (Annotation annotation : m.getDeclaredAnnotations()) {
                    if (annotation instanceof Task) {
                        return (Task) annotation;
                    }
                }
                break;
            }
        }
        throw new TaskAnnotationMissingException(method);
    }
}
