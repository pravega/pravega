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

import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import lombok.Data;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * TaskBase contains the following.
 * 1. Environment variables used by tasks.
 * 2. Wrapper method that has boilerplate code for locking, persisting task data and executing the task
 *
 * Actual tasks are implemented in sub-classes of TaskBase and annotated with @Task annotation.
 */
public class TaskBase implements Cloneable {

    public interface FutureOperation<T> {
        CompletableFuture<T> apply();
    }

    @Data
    public static class Context {
        private final String hostId;
        private final String oldHostId;
        private final List<String> oldResourceTag;

        public Context(String hostId) {
            this.hostId = hostId;
            this.oldHostId = null;
            this.oldResourceTag = null;
        }

        public Context(String hostId, String oldHost, List<String> oldResourceTag) {
            this.hostId = hostId;
            this.oldHostId = oldHost;
            this.oldResourceTag = oldResourceTag;
        }
    }

    public static class TaggedResource {
        private final static String SEPARATOR = "_%%%_";

        public static String getResource(String taggedResource) {
            return taggedResource.split(SEPARATOR)[0];
        }

        public static String getTaggedResource(String resource) {
            return resource + SEPARATOR + UUID.randomUUID().toString();
        }
    }

    private Context context;

    private final TaskMetadataStore taskMetadataStore;

    public TaskBase(TaskMetadataStore taskMetadataStore, String hostId) {
        this.taskMetadataStore = taskMetadataStore;
        context = new Context(hostId);
    }

    @Override
    public TaskBase clone() throws CloneNotSupportedException {
        return (TaskBase) super.clone();
    }

    public static String getResource(String scope, String stream) {
        return scope + "_%_" + stream;
    }

    public static String getResource(String scope, String stream, String transactionId) {
        return scope + "_%_" + stream + "_%_" + transactionId;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public Context getContext() {
        return this.context;
    }

    /**
     * Wrapper method that initially obtains lock, persists the task data then executes the passed method,
     * finally deletes task data and releases lock.
     *
     * @param resource resource to be updated by the task.
     * @param parameters method parameters.
     * @param operation lambda operation that is the actual task.
     * @param <T> type parameter of return value of operation to be executed.
     * @return return value of task execution.
     */
    public <T> CompletableFuture<T> execute(String resource, Serializable[] parameters, FutureOperation<T> operation) {
        TaskData taskData = getTaskData(parameters);
        final String resourceTag = TaggedResource.getTaggedResource(resource);

        return
                // PutChild (HostId, resource)
                // Initially store the fact that I am about the update the resource.
                // Since multiple threads within this process could concurrently attempt to modify same resource,
                // we tag the resource name with a random GUID so as not to interfere with other thread's
                // creation or deletion of resource children under HostId node.
                taskMetadataStore.putChild(context.hostId, resourceTag)
                        // After storing that fact, lock the resource, execute task and unlock the resource
                        .thenCompose(x -> executeTask(resource, taskData, operation))
                        // finally delete the resource child created under the controller's HostId
                        .whenComplete((result, e) -> taskMetadataStore.removeChild(context.hostId, resourceTag, true));
    }

    private <T> CompletableFuture<T> executeTask(String resource, TaskData taskData, FutureOperation<T> operation) {
        final CompletableFuture<T> returnFuture = new CompletableFuture<>();

        taskMetadataStore
                .lock(resource, taskData, context.hostId, context.oldHostId)

                // On acquiring lock, the following invariants hold
                // Invariant 1. No other thread within any controller process is running an update task on the resource
                // Invariant 2. We have denoted the fact that current controller's HostId is updating the resource. This
                // fact can be used in case current controller instance crashes.
                // Invariant 3. Any other controller that had created resource child under its HostId, can now be safely
                // deleted, since that information redundant and is not useful during that HostId's fail over.
                .whenComplete((result, e) -> {
                    // Once I acquire the lock, the old HostId that held lock on this resource.
                    if (e == null || !(e instanceof LockFailedException)) {
                        // safe to delete resourceTags from oldHost, if available
                        if (context.oldHostId != null && !context.oldHostId.isEmpty()) {
                            context.getOldResourceTag()
                                    .stream()
                                    .forEach(tag -> taskMetadataStore.removeChild(context.oldHostId, tag, true));
                        }
                    }
                })

                // Exclusively execute the update task on the resource
                .thenCompose(y -> operation.apply())

                // If lock had been obtained, unlock it before completing the task.
                .whenComplete((T value, Throwable e) -> {
                    if (e != null && e instanceof LockFailedException) {
                        returnFuture.completeExceptionally(e);
                    } else {
                        taskMetadataStore.unlock(resource, context.hostId)
                                .thenApply(x -> {
                                    if (e != null) {
                                        returnFuture.completeExceptionally(e);
                                    } else {
                                        returnFuture.complete(value);
                                    }
                                    return null;
                                });
                    }
                });
        return returnFuture;
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
        throw new TaskAnnotationNotFoundException(method);
    }
}
