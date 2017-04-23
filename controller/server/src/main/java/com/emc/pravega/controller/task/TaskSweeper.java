/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.task;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.store.task.TaggedResource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.task.TaskBase.Context;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.emc.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static com.emc.pravega.controller.util.RetryHelper.UNCONDITIONAL_PREDICATE;
import static com.emc.pravega.controller.util.RetryHelper.withRetries;
import static com.emc.pravega.controller.util.RetryHelper.withRetriesAsync;

@Slf4j
public class TaskSweeper {

    private final TaskMetadataStore taskMetadataStore;
    private final TaskBase[] taskClassObjects;
    private final Map<String, Method> methodMap = new HashMap<>();
    private final Map<String, TaskBase> objectMap = new HashMap<>();
    private final String hostId;
    private final ScheduledExecutorService executor;

    @Data
    static class Result {
        private final TaggedResource taggedResource;
        private final Object value;
        private final Throwable error;
    }

    public TaskSweeper(final TaskMetadataStore taskMetadataStore, final String hostId,
                       final ScheduledExecutorService executor, final TaskBase... classes) {
        this.taskMetadataStore = taskMetadataStore;
        this.hostId = hostId;
        this.executor = executor;
        for (TaskBase object : classes) {
            Preconditions.checkArgument(object.getContext().getHostId().equals(hostId));
        }

        // following arrays can alternatively be populated by dynamically finding all sub-classes of TaskBase using
        // reflection library org.reflections. However, this library is flagged by checkstyle as disallowed library.
        this.taskClassObjects = classes;
        initializeMappingTable();
    }

    public CompletableFuture<Void> sweepOrphanedTasks(final Supplier<Set<String>> runningProcesses) {
        return withRetriesAsync(taskMetadataStore::getHosts, RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor)
                .thenComposeAsync(registeredHosts -> {
                    log.info("Hosts {} have ongoing tasks", registeredHosts);
                    registeredHosts.removeAll(withRetries(runningProcesses, UNCONDITIONAL_PREDICATE, Integer.MAX_VALUE));
                    log.info("Failed hosts {} have orphaned tasks", registeredHosts);
                    return FutureHelpers.allOf(registeredHosts.stream()
                            .map(this::sweepOrphanedTasks).collect(Collectors.toList()));
                }, executor);
    }

    /**
     * This method is called whenever a node in the controller cluster dies. A ServerSet abstraction may be used as
     * a trigger to invoke this method with one of the dead hostId.
     * <p>
     * It sweeps through all unfinished tasks of failed host and attempts to execute them to completion.
     * @param oldHostId old host id
     * @return A future that completes when sweeping completes
     */
    public CompletableFuture<Void> sweepOrphanedTasks(final String oldHostId) {

        log.info("Sweeping orphaned tasks for host {}", oldHostId);
        return withRetriesAsync(() -> FutureHelpers.doWhileLoop(
                () -> executeHostTask(oldHostId),
                Objects::nonNull, executor)
                .whenCompleteAsync((result, ex) -> log.info("Sweeping orphaned tasks for host {} complete", oldHostId), executor),
                RETRYABLE_PREDICATE.and(e -> !(ExceptionHelpers.getRealException(e) instanceof LockFailedException)),
                Integer.MAX_VALUE, executor);
    }

    private CompletableFuture<Result> executeHostTask(final String oldHostId) {

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

    private CompletableFuture<Result> executeResourceTask(final String oldHostId, final TaggedResource taggedResource) {
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
    @SuppressWarnings("unchecked")
    private CompletableFuture<Object> execute(final String oldHostId, final TaskData taskData, final TaggedResource taggedResource) {

        log.debug("Host={} attempting to execute task {} for child <{}, {}> of {}",
                this.hostId, taskData.getMethodName(), taggedResource.getResource(), taggedResource.getTag(), oldHostId);
        try {

            String key = getKey(taskData.getMethodName(), taskData.getMethodVersion());
            if (methodMap.containsKey(key)) {

                // find the method and object
                Method method = methodMap.get(key);
                if (objectMap.get(key).isReady()) {
                    TaskBase o = objectMap.get(key).copyWithContext(new Context(hostId,
                            oldHostId,
                            taggedResource.getTag(),
                            taggedResource.getResource()));

                    // finally execute the task by invoking corresponding method and return its result
                    return (CompletableFuture<Object>) method.invoke(o, (Object[]) taskData.getParameters());
                } else {
                    // The then branch of this if-then-else executes the task and releases the lock even if the
                    // task execution fails. Eventually all the orphaned tasks of the failed host shall complete.
                    // However, if the task object of a specific task is not ready, we do not attempt to execute
                    // the task. Instead, we wait for a small random amount of time and then bail out, so that the
                    // task can be retried some time in future, as it will be fetched by the
                    // getRandomChild(failedHostId) statement in executeHostTask(failedHostId).
                    // Eventually, when all task objects are ready, all the orphaned tasks of failed host shall
                    // be executed and the failed host's identifier shall be removed from the hostIndex.
                    log.info("Task module for method {} not yet ready, delaying processing it", method.getName());
                    return FutureHelpers.delayedFuture(Duration.ofMillis(100), executor)
                            .thenApplyAsync(ignore -> null, executor);
                }

            } else {
                CompletableFuture<Object> error = new CompletableFuture<>();
                log.warn("Task {} not found", taskData.getMethodName());
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
            Class<? extends TaskBase> claz = taskClassObject.getClass();
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
