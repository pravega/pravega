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

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskNotFoundException;
import com.emc.pravega.controller.task.TaskBase.Context;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
     * a trigger to invoke this method.
     *
     * It sweeps through all unfinished tasks of failed host and attempts to execute them to completion.
     */
    public CompletableFuture<List<Object>> sweepOrphanedTasks(String oldHostId) {

        // Initially read the list of resources that the failed host could have been executing tasks on.
        return taskMetadataStore.getChildren(oldHostId)
                .thenCompose(resourceTags -> {

                    if (resourceTags == null || resourceTags.isEmpty()) {

                        // Invariant: If no resources were found, it is safe to delete the old HostId node.
                        return taskMetadataStore.removeNode(oldHostId)
                                .thenApply(x -> Collections.emptyList());

                    } else {
                        Map<String, List<String>> resourceGroups =
                                resourceTags
                                        .stream()
                                        .collect(Collectors.groupingBy(TaskBase.TaggedResource::getResource));

                        // compete to execute tasks left unfinished by old HostId.
                        List<CompletableFuture<Object>> list =
                                resourceGroups
                                        .entrySet()
                                        .stream()
                                        .map(pair -> executeResourceTask(oldHostId, pair.getKey(), pair.getValue()))
                                        .collect(Collectors.toList());

                        return FutureCollectionHelper.sequence(list);
                    }
                });
    }

    public CompletableFuture<Object> executeResourceTask(String oldHostId, String resource, List<String> resourceTags) {
        final CompletableFuture<Object> result = new CompletableFuture<>();
        taskMetadataStore.getTask(resource)
                .whenComplete((taskData, ex) -> {
                    if (taskData != null) {

                        execute(oldHostId, taskData, resourceTags)
                                .whenComplete((value, e) -> {
                                    if (e != null) {
                                        result.completeExceptionally(e);
                                    } else {
                                        result.complete(value);
                                    }
                                });

                    } else {
                        if (ex != null) {
                            if (ex instanceof TaskNotFoundException) {
                                // If no task was found for the given resource, then either
                                // 1. Task has been completely executed by some other controller instance, or
                                // 2. Old host died immediately after creating the resource child under its HostId.
                                // Invariant: In either case it is safe to delete resource child (all tagged variants) under old HostId
                                removeChildren(oldHostId, resourceTags);
                                result.complete(null);

                            } else {

                                result.completeExceptionally(ex);

                            }
                        }
                    }
                });
        return result;
    }

    public void removeChildren(String hostId, List<String> resourceTags) {
        resourceTags
                .stream()
                .forEach(resourceTag -> taskMetadataStore.removeChild(hostId, resourceTag, true));
    }

    /**
     * This method identifies correct method to execute form among the task classes and executes it.
     * @param oldHostId identifier of old failed host.
     * @param taskData taks data.
     * @param resourceTags resource on which old host had unfinished task.
     * @return the object returned from task method.
     */
    public CompletableFuture<Object> execute(String oldHostId, TaskData taskData, List<String> resourceTags) {

        log.debug("Trying to execute {}", taskData.getMethodName());
        try {

            String key = getKey(taskData.getMethodName(), taskData.getMethodVersion());
            if (methodMap.containsKey(key)) {

                // find the method and object
                Method method = methodMap.get(key);
                TaskBase o = objectMap.get(key).clone();
                o.setContext(new Context(hostId, oldHostId, resourceTags));

                // finally execute the method and return its result
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
