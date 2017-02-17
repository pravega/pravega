/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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

@Slf4j
public class TaskSweeper {

    private final TaskMetadataStore taskMetadataStore;
    private final TaskBase[] taskClassObjects;
    private final Map<String, Method> methodMap = new HashMap<>();
    private final Map<String, TaskBase> objectMap = new HashMap<>();
    private final String hostId;

    @Data
    public static class Result {
        private final TaggedResource taggedResource;
        private final Object value;
        private final Throwable error;
    }

    public TaskSweeper(final TaskMetadataStore taskMetadataStore, final String hostId, final TaskBase... classes) {
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
     * <p>
     * It sweeps through all unfinished tasks of failed host and attempts to execute them to completion.
     * @param oldHostId old host id
     * @return
     */
    public CompletableFuture<Void> sweepOrphanedTasks(final String oldHostId) {

        return FutureHelpers.doWhileLoop(
                () -> executeHostTask(oldHostId),
                x -> x != null);
    }

    public CompletableFuture<Result> executeHostTask(final String oldHostId) {

        // Get a random child TaggedResource of oldHostId node and attempt to execute corresponding task
        return taskMetadataStore.getRandomChild(oldHostId)
                .thenCompose(taggedResourceOption -> {

                    if (!taggedResourceOption.isPresent()) {

                        log.debug("Host={} fetched no child of {}", this.hostId, oldHostId);
                        // Invariant: If no taggedResources were found, it is safe to delete oldHostId node.
                        // Moreover, no need to get any more children, hence return null.
                        return taskMetadataStore.removeNode(oldHostId)
                                .thenApply(x -> null);

                    } else {

                        TaggedResource taggedResource = taggedResourceOption.get();
                        log.debug("Host={} processing child <{}, {}> of {}",
                                this.hostId, taggedResource.getResource(), taggedResource.getTag(), oldHostId);
                        // Fetch task corresponding to resourceTag.resource owned by (oldHostId, resourceTag.threadId)
                        // and compete to execute it to completion.
                        return executeResourceTask(oldHostId, taggedResource);

                    }
                });
    }

    public CompletableFuture<Result> executeResourceTask(final String oldHostId, final TaggedResource taggedResource) {
        final CompletableFuture<Result> result = new CompletableFuture<>();
        // Get the task details associated with resource taggedResource.resource
        // that is owned by oldHostId and taggedResource.threadId

        // If the resource taggedResource.resource is owned by pair (oldHostId, taggedResource.threadId), then
        //     getTask shall return that resource.
        //     Compete to lock that resource and execute it to completion by calling the task's method.
        // Else
        //     It is safe to delete the taggedResource child under oldHostId, since there is no pending task on
        //     resource taggedResource.resource and owned by (oldHostId, taggedResource.threadId).
        taskMetadataStore.getTask(taggedResource.getResource(), oldHostId, taggedResource.getTag())
                .whenComplete((taskData, ex) -> {
                    if (taskData != null && taskData.isPresent()) {

                        log.debug("Host={} found task for child <{}, {}> of {}",
                                this.hostId, taggedResource.getResource(), taggedResource.getTag(), oldHostId);
                        execute(oldHostId, taskData.get(), taggedResource)
                                .whenComplete((value, e) -> result.complete(new Result(taggedResource, value, e)));

                    } else {

                        if (taskData != null) {

                            log.debug("Host={} found no task for child <{}, {}> of {}. Removing child.",
                                    this.hostId, taggedResource.getResource(), taggedResource.getTag(), oldHostId);
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
     *
     * @param oldHostId      identifier of old failed host.
     * @param taskData       taks data.
     * @param taggedResource resource on which old host had unfinished task.
     * @return the object returned from task method.
     */
    public CompletableFuture<Object> execute(final String oldHostId, final TaskData taskData, final TaggedResource taggedResource) {

        log.debug("Host={} attempting to execute task {} for child <{}, {}> of {}",
                this.hostId, taskData.getMethodName(), taggedResource.getResource(), taggedResource.getTag(), oldHostId);
        try {

            String key = getKey(taskData.getMethodName(), taskData.getMethodVersion());
            if (methodMap.containsKey(key)) {

                // find the method and object
                Method method = methodMap.get(key);
                TaskBase o = objectMap.get(key).copyWithContext(new Context(hostId,
                                                                            oldHostId,
                                                                            taggedResource.getTag(),
                                                                            taggedResource.getResource()));

                // finally execute the task by invoking corresponding method and return its result
                return (CompletableFuture<Object>) method.<CompletableFuture<Object>>invoke(o, (Object[]) taskData.getParameters());

            } else {
                CompletableFuture<Object> error = new CompletableFuture<>();
                error.completeExceptionally(
                        new RuntimeException(String.format("Task %s not found", taskData.getMethodName()))
                );
                return error;
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
     *
     * @param taskName    method name.
     * @param taskVersion method version.,
     * @return key
     */
    private String getKey(final String taskName, final String taskVersion) {
        return taskName + "--" + taskVersion;
    }
}
