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
package io.pravega.controller.task;

import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaggedResource;
import io.pravega.controller.store.task.TaskMetadataStore;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * TaskBase contains the following.
 * 1. Environment variables used by tasks.
 * 2. Wrapper method that has boilerplate code for locking, persisting task data and executing the task
 *
 * Actual tasks are implemented in sub-classes of TaskBase and annotated with @Task annotation.
 */
@Slf4j
public abstract class TaskBase implements AutoCloseable {

    public interface FutureOperation<T> {
        CompletableFuture<T> apply();
    }

    @Data
    public static class Context {
        private final String hostId;
        private final String oldHostId;
        private final String oldTag;
        private final Resource oldResource;

        public Context(final String hostId) {
            this.hostId = hostId;
            this.oldHostId = null;
            this.oldTag = null;
            this.oldResource = null;
        }

        public Context(final String hostId, final String oldHost, final String oldTag, final Resource oldResource) {
            this.hostId = hostId;
            this.oldHostId = oldHost;
            this.oldTag = oldTag;
            this.oldResource = oldResource;
        }
    }

    protected final ScheduledExecutorService executor;

    protected final Context context;

    protected final TaskMetadataStore taskMetadataStore;

    private volatile boolean ready;
    private final CountDownLatch readyLatch;
    private boolean createIndexOnlyMode;

    public TaskBase(final TaskMetadataStore taskMetadataStore, final ScheduledExecutorService executor,
                    final String hostId) {
        this(taskMetadataStore, executor, new Context(hostId));
    }

    protected TaskBase(final TaskMetadataStore taskMetadataStore, final ScheduledExecutorService executor,
                       final Context context) {
        this.taskMetadataStore = taskMetadataStore;
        this.executor = executor;
        this.context = context;
        this.ready = false;
        readyLatch = new CountDownLatch(1);
        this.createIndexOnlyMode = false;
    }

    public abstract TaskBase copyWithContext(Context context);

    public Context getContext() {
        return this.context;
    }

    /**
     * Wrapper method that initially obtains lock then executes the passed method, and finally releases lock.
     *
     * @param resource resource to be updated by the task.
     * @param parameters method parameters.
     * @param operation lambda operation that is the actual task.
     * @param <T> type parameter of return value of operation to be executed.
     * @return return value of task execution.
     */
    public <T> CompletableFuture<T> execute(final Resource resource, final Serializable[] parameters, final FutureOperation<T> operation) {
        if (!ready) {
            return Futures.failedFuture(new IllegalStateException(getClass().getName() + " not yet ready"));
        }
        final String tag = UUID.randomUUID().toString();
        final TaskData taskData = getTaskData(parameters);
        final CompletableFuture<T> result = new CompletableFuture<>();
        final TaggedResource taggedResource = new TaggedResource(tag, resource);

        log.debug("Host={}, Tag={} starting to execute task {}-{} on resource {}", context.hostId, tag,
                taskData.getMethodName(), taskData.getMethodVersion(), resource);
        if (createIndexOnlyMode) {
            return createIndexes(taggedResource, taskData);
        }
        // PutChild (HostId, resource)
        // Initially store the fact that I am about the update the resource.
        // Since multiple threads within this process could concurrently attempt to modify same resource,
        // we tag the resource name with a random GUID so as not to interfere with other thread's
        // creation or deletion of resource children under HostId node.
        taskMetadataStore.putChild(context.hostId, taggedResource)
                // After storing that fact, lock the resource, execute task and unlock the resource
                .thenComposeAsync(x -> executeTask(resource, taskData, tag, operation), executor)
                // finally delete the resource child created under the controller's HostId
                .whenCompleteAsync((value, e) ->
                                taskMetadataStore.removeChild(context.hostId, taggedResource, true)
                                        .whenCompleteAsync((innerValue, innerE) -> {
                                            // ignore the result of removeChile operations, since it is an optimization
                                            if (e != null) {
                                                result.completeExceptionally(e);
                                            } else {
                                                result.complete(value);
                                            }
                                        }, executor),
                        executor);

        return result;
    }

    private <T> CompletableFuture<T> createIndexes(TaggedResource taggedResource, TaskData taskData) {
        return taskMetadataStore.putChild(context.hostId, taggedResource)
                .thenComposeAsync(x -> taskMetadataStore.lock(taggedResource.getResource(), taskData, context.hostId,
                            taggedResource.getTag(), context.oldHostId, context.oldTag), executor)
                .thenApplyAsync(x -> {
                    throw new IllegalStateException("Index only mode");
                }, executor);
    }

    protected void setReady() {
        ready = true;
        readyLatch.countDown();
    }

    protected void setCreateIndexOnlyMode() {
        this.createIndexOnlyMode = true;
    }

    public boolean isReady() {
        return ready;
    }

    @VisibleForTesting
    public boolean awaitInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return readyLatch.await(timeout, timeUnit);
    }

    public void awaitInitialization() throws InterruptedException {
        readyLatch.await();
    }

    private <T> CompletableFuture<T> executeTask(final Resource resource,
                                                 final TaskData taskData,
                                                 final String tag,
                                                 final FutureOperation<T> operation) {
        final CompletableFuture<T> result = new CompletableFuture<>();

        final CompletableFuture<Void> lockResult = new CompletableFuture<>();

        taskMetadataStore
                .lock(resource, taskData, context.hostId, tag, context.oldHostId, context.oldTag)

                // On acquiring lock, the following invariants hold
                // Invariant 1. No other thread within any controller process is running an update task on the resource
                // Invariant 2. We have denoted the fact that current controller's HostId is updating the resource. This
                // fact can be used in case current controller instance crashes.
                // Invariant 3. Any other controller that had created resource child under its HostId, can now be safely
                // deleted, since that information is redundant and is not useful during that HostId's fail over.
                .whenCompleteAsync((value, e) -> {
                    // Once I acquire the lock, safe to delete context.oldResource from oldHost, if available
                    if (e != null) {

                        log.debug("Host={}, Tag={} lock attempt on resource {} failed", context.hostId, tag, resource);
                        lockResult.completeExceptionally(e);

                    } else {

                        log.debug("Host={}, Tag={} acquired lock on resource {}", context.hostId, tag, resource);
                        removeOldHostChild(tag).whenCompleteAsync((x, y) -> lockResult.complete(value), executor);
                    }
                }, executor);

        lockResult
                // Exclusively execute the update task on the resource
                .thenComposeAsync(y -> operation.apply(), executor)

                // If lock had been obtained, unlock it before completing the task.
                .whenCompleteAsync((T value, Throwable e) -> {
                    if (lockResult.isCompletedExceptionally()) {
                        // If lock was not obtained, complete the operation with error
                        result.completeExceptionally(e);

                    } else {
                        // If lock was obtained, irrespective of result of operation execution,
                        // release lock before completing operation.
                        log.debug("Host={}, Tag={} completed executing task on resource {}", context.hostId, tag, resource);
                        taskMetadataStore.unlock(resource, context.hostId, tag)
                                .whenCompleteAsync((innerValue, innerE) -> {
                                    log.debug("Host={}, Tag={} unlock attempt completed on resource {}", context.hostId, tag, resource);
                                    // If lock was acquired above, unlock operation retries until it is released.
                                    // It throws exception only if non-lock holder tries to release it.
                                    // Hence ignore result of unlock operation and complete future with previous result.
                                    if (e != null) {
                                        result.completeExceptionally(e);
                                    } else {
                                        result.complete(value);
                                    }
                                }, executor);
                    }
                }, executor);
        return result;
    }

    private CompletableFuture<Void> removeOldHostChild(final String tag) {
        if (context.oldHostId != null && !context.oldHostId.isEmpty()) {
            log.debug("Host={}, Tag={} removing child <{}, {}> of {}",
                    context.hostId, tag, context.oldResource, context.oldTag, context.oldHostId);
            return taskMetadataStore.removeChild(
                    context.oldHostId,
                    new TaggedResource(context.oldTag, context.oldResource),
                    true);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private TaskData getTaskData(final Serializable[] parameters) {
        // Quirk of using stack trace shall be rendered redundant when Task Annotation's handler is coded up.
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement e = stacktrace[3];
        Task annotation = getTaskAnnotation(e.getMethodName());
        return new TaskData(annotation.name(), annotation.version(), parameters);
    }

    private Task getTaskAnnotation(final String method) {
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
