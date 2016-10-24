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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.task.TaggedResource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.task.TaskBase.Context;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This class
 */
@Slf4j
public class TaskSweeper {

    private final TaskMetadataStore taskMetadataStore;
    private final TaskBase[] taskClassObjects;
    private final Map<String, Method> methodMap = new HashMap<>();
    private final Map<String, TaskBase> objectMap = new HashMap<>();
    private final String hostId;

    @Data
    public class Result {
        private final TaggedResource taggedResource;
        private final Object value;
        private final Throwable error;
    }

    public TaskSweeper(TaskMetadataStore taskMetadataStore, String hostId, TaskBase... classes) {
        this.taskMetadataStore = taskMetadataStore;
        this.hostId = hostId;
        for (TaskBase object : classes) {
            Preconditions.checkArgument(object.getContext().getHostId().equals(hostId));
        }

        // following arrays can alternatively be populated by dynamically finding all sub-classes of TaskBase using
        // reflection library org.reflections. However, this library is flagged by checkstyle as disallowed library.
        this.taskClassObjects = classes;
        initializeMappingTable();
    }

    /**
     * This method is called whenever a node in the controller cluster dies. A ServerSet abstraction may be used as
     * a trigger to invoke this method with one of the dead hostId.
     *
     * It sweeps through all unfinished tasks of failed host and attempts to execute them to completion.
     */
    public CompletableFuture<Void> sweepOrphanedTasks(String oldHostId) {

        return FutureHelpers.loop(
                x -> x == null,
                () -> executeHostTask(oldHostId));
    }

    public CompletableFuture<Result> executeHostTask(String oldHostId) {

        // Get a random child TaggedResource of oldHostId node and attempt to execute corresponding task
        return taskMetadataStore.getRandomChild(oldHostId)
                .thenCompose(taggedResourceOption -> {

                    if (!taggedResourceOption.isPresent()) {

                        // Invariant: If no taggedResources were found, it is safe to delete oldHostId node.
                        // Moreover, no need to get any more children, hence return null.
                        return taskMetadataStore.removeNode(oldHostId)
                                .thenApply(x -> null);

                    } else {

                        // Fetch task corresponding to resourceTag.resource owned by (oldHostId, resourceTag.threadId)
                        // and compete to execute it to completion.
                        return executeResourceTask(oldHostId, taggedResourceOption.get());

                    }
                });
    }

    public CompletableFuture<Result> executeResourceTask(String oldHostId, TaggedResource taggedResource) {
        final CompletableFuture<Result> result = new CompletableFuture<>();
        // Get the task details associated with resource taggedResource.resource
        // that is owned by oldHostId and taggedResource.threadId

        // If the resource taggedResource.resource is owned by pair (oldHostId, taggedResource.threadId), then
        //     getTask shall return that resource.
        //     Compete to lock that resource and execute it to completion by calling the task's method.
        // Else
        //     It is safe to delete the taggedResource child under oldHostId, since there is no pending task on
        //     resource taggedResource.resource and owned by (oldHostId, taggedResource.threadId).
        taskMetadataStore.getTask(taggedResource.getResource(), oldHostId, taggedResource.getThreadId())
                .whenComplete((taskData, ex) -> {
                    if (taskData != null && taskData.isPresent()) {

                        execute(oldHostId, taskData.get(), taggedResource)
                                .whenComplete((value, e) -> result.complete(new Result(taggedResource, value, e)));

                    } else {

                        if (taskData != null) {

                            // taskData.isPresent() is false
                            // If no task was found for the taggedResource.resource owned by
                            // (oldHostId, taggedResource.threadId), then either of the following holds
                            // 1. Old host died immediately after creating the child taggedResource under oldHostId, or
                            // 2. Some host grabbed the task owned by (oldHostId, taggedResource.threadId).
                            // Invariant: In either case it is safe to delete taggedResource under oldHostId node.
                            taskMetadataStore.removeChild(oldHostId, taggedResource, true)
                                    .whenComplete((value, e) -> {
                                        // Ignore the result of remove child operation.
                                        // Even if it fails, ignore it, as it is an optimization anyways.
                                        result.complete(new Result(taggedResource, null, ex));
                                    });

                        } else {

                            // taskData == null and possibly ex != null
                            result.complete(new Result(taggedResource, null, ex));

                        }

                    }
                });
        return result;
    }

    /**
     * This method identifies correct method to execute form among the task classes and executes it.
     * @param oldHostId identifier of old failed host.
     * @param taskData taks data.
     * @param taggedResource resource on which old host had unfinished task.
     * @return the object returned from task method.
     */
    public CompletableFuture<Object> execute(String oldHostId, TaskData taskData, TaggedResource taggedResource) {

        log.debug("Trying to execute {}", taskData.getMethodName());
        try {

            String key = getKey(taskData.getMethodName(), taskData.getMethodVersion());
            if (methodMap.containsKey(key)) {

                // find the method and object
                Method method = methodMap.get(key);
                TaskBase o = objectMap.get(key).clone();
                o.setContext(new Context(hostId, oldHostId, taggedResource.getThreadId(), taggedResource.getResource()));

                // finally execute the task by invoking corresponding method and return its result
                return (CompletableFuture<Object>) method.<CompletableFuture<Object>>invoke(o, (Object[]) taskData.getParameters());

            } else {
                throw new RuntimeException(String.format("Task %s not found", taskData.getMethodName()));
            }

        } catch (Exception e) {
            CompletableFuture<Object> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }
    }

    /**
     * Creates the table mapping method names and versions to Method objects and corresponding TaskBase objects
     */
    private void initializeMappingTable() {
        for (TaskBase taskClassObject : taskClassObjects) {
            Class claz = taskClassObject.getClass();
            for (Method method : claz.getDeclaredMethods()) {
                for (Annotation annotation : method.getAnnotations()) {
                    if (annotation instanceof Task) {
                        String methodName = ((Task) annotation).name();
                        String methodVersion = ((Task) annotation).version();
                        String key = getKey(methodName, methodVersion);
                        if (!methodMap.containsKey(key)) {
                            methodMap.put(key, method);
                            objectMap.put(key, taskClassObject);
                        } else {
                            // duplicate name--version pair
                            throw new DuplicateTaskAnnotationException(methodName, methodVersion);
                        }
                    }
                }
            }
        }
    }

    /**
     * Internal key used in mapping tables.
     * @param taskName method name.
     * @param taskVersion method version.,
     * @return key
     */
    private String getKey(String taskName, String taskVersion) {
        return taskName + "--" + taskVersion;
    }
}
