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
import com.emc.pravega.controller.store.task.LockData;
import com.emc.pravega.controller.task.TaskBase.Context;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

    public TaskSweeper(TaskMetadataStore taskMetadataStore, TaskBase... classes) {
        this.taskMetadataStore = taskMetadataStore;

        // following arrays can alternatively be populated by dynamically finding all sub-classes of TaskBase using
        // reflection library org.reflections. However, this library is flagged by checkstyle as disallowed library.
        taskClassObjects = classes;
        initializeMappingTable();
    }

    /**
     * This method is called whenever a node in the controller cluster dies. A ServerSet abstraction may be used as
     * a trigger to invoke this method.
     *
     * It sweeps through all stream tasks under path /tasks/stream/, identifies orphaned tasks and attempts to execute
     * them to completion.
     */
    public CompletableFuture<List<Object>> sweepOrphanedTasks(String hostId) {
        return taskMetadataStore.getChildren(hostId)
                .thenCompose(resourceTags -> {

                    Map<String, List<String>> resourceGroups =
                            resourceTags
                                    .stream()
                                    .collect(Collectors.groupingBy(TaskBase.TaggedResource::getResource));

                    List<CompletableFuture<Object>> list =
                            resourceGroups
                                    .entrySet()
                                    .stream()
                                    .map(pair -> executeResourceTask(hostId, pair.getKey(), pair.getValue()))
                                    .collect(Collectors.toList());

                    return FutureCollectionHelper.sequence(list);
                });
    }

    public CompletableFuture<Object> executeResourceTask(String hostId, String resource, List<String> resourceTags) {
        final CompletableFuture<Object> result = new CompletableFuture<>();
        taskMetadataStore.get(resource)
                .whenComplete((bytes, ex) -> {
                    if (ex != null && ex instanceof KeeperException.NoNodeException) {
                        // safe to delete all resourceTags under hostId
                        resourceTags
                                .stream()
                                .forEach(resourceTag -> taskMetadataStore.removeChild(hostId, resourceTag));
                        result.complete(null);
                    } else {
                        LockData lockData = LockData.deserialize(bytes);
                        execute(lockData.getHostId(), TaskData.deserialize(lockData.getTaskData()), resourceTags)
                                .whenComplete((value, e) -> {
                                    if (e != null) {
                                        result.completeExceptionally(e);
                                    } else {
                                        result.complete(result);
                                    }
                                });
                    }
                });
        return result;
    }

    /**
     * This method identifies correct method to execute form among the task classes and executes it
     * @param taskData taks data
     * @return the object returned from task method
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public CompletableFuture<Object> execute(String failedHostId, TaskData taskData, List<String> resourceTags) {

        log.debug("Trying to execute {}", taskData.getMethodName());
        String key = getKey(taskData.getMethodName(), taskData.getMethodVersion());
        if (methodMap.containsKey(key)) {
            try {

                // find the method and object
                Method method = methodMap.get(key);
                TaskBase o = objectMap.get(key).clone();
                o.setContext(new Context(failedHostId, resourceTags));

                // finally execute the method and return its result
                return (CompletableFuture<Object>) method.<CompletableFuture<Object>>invoke(o, (Object[]) taskData.getParameters());

            } catch (IllegalAccessException | InvocationTargetException | CloneNotSupportedException ex) {
                throw new RuntimeException("Error executing task.", ex);
            }
        } else {
            throw new RuntimeException(String.format("Task %s not found", taskData.getMethodName()));
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
