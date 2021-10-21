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
package io.pravega.common.concurrent;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.function.RunnableWithException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper methods for ExecutorService.
 */
@Slf4j
public final class ExecutorServiceHelpers {
    private static final ExecutorServiceFactory FACTORY = new ExecutorServiceFactory();

    /**
     * Creates and returns a thread factory that will create threads with the given name prefix.
     *
     * @param groupName the name of the threads
     * @return a thread factory
     */
    public static ThreadFactory getThreadFactory(String groupName) {
        return FACTORY.getThreadFactory(groupName);
    }

    /**
     * Creates and returns a thread factory that will create threads with the given name prefix and thread priority.
     *
     * @param groupName the name of the threads
     * @param priority the priority to be assigned to the thread.
     * @return a thread factory
     */
    public static ThreadFactory getThreadFactory(String groupName, int priority) {
        return FACTORY.getThreadFactory(groupName, priority);
    }

    /**
     * Creates a new ScheduledExecutorService that will use daemon threads with appropriate names the threads.
     * @param size The number of threads in the threadpool
     * @param poolName The name of the pool (this will be printed in logs)
     * @return A new executor service.
     */
    public static ScheduledExecutorService newScheduledThreadPool(int size, String poolName) {
        return newScheduledThreadPool(size, poolName, Thread.NORM_PRIORITY);
    }

    /**
     * Creates a new ScheduledExecutorService that will use daemon threads with specified priority and names.
     *
     * @param size The number of threads in the threadpool
     * @param poolName The name of the pool (this will be printed in logs)
     * @param threadPriority The priority to be assigned to the threads
     * @return A new executor service.
     */
    public static ScheduledExecutorService newScheduledThreadPool(int size, String poolName, int threadPriority) {
        return FACTORY.newScheduledThreadPool(size, poolName, threadPriority);
    }

    /**
     * Gets a snapshot of the given ExecutorService.
     *
     * @param service The ExecutorService to request a snapshot on.
     * @return A Snapshot of the given ExecutorService, or null if not supported.
     */
    public static Snapshot getSnapshot(ExecutorService service) {
        Preconditions.checkNotNull(service, "service");
        if (service instanceof ThreadPoolExecutor) {
            val tpe = (ThreadPoolExecutor) service;
            return new Snapshot(tpe.getQueue().size(), tpe.getActiveCount(), tpe.getPoolSize());
        } else if (service instanceof ForkJoinPool) {
            val fjp = (ForkJoinPool) service;
            return new Snapshot(fjp.getQueuedSubmissionCount(), fjp.getActiveThreadCount(), fjp.getPoolSize());
        } else if (service instanceof ThreadPoolScheduledExecutorService) {
            val tpse = (ThreadPoolScheduledExecutorService) service;
            return new Snapshot(tpse.getRunner().getQueue().size(), tpse.getRunner().getActiveCount(), tpse.getRunner().getPoolSize());
        } else {
            return null;
        }
    }

    /**
     * Operates like Executors.cachedThreadPool but with a custom thread timeout and pool name.
     * @return A new threadPool
     * @param maxThreadCount The maximum number of threads to allow in the pool.
     * @param threadTimeout the number of milliseconds that a thread should sit idle before shutting down.
     * @param poolName The name of the threadpool.
     */
    public static ExecutorService getShrinkingExecutor(int maxThreadCount, int threadTimeout, String poolName) {
        return FACTORY.newShrinkingExecutor(maxThreadCount, threadTimeout, poolName);
    }

    /**
     * Executes the given task on the given Executor.
     *
     * @param task             The RunnableWithException to execute.
     * @param exceptionHandler A Consumer that will be invoked in case the task threw an Exception. This is not invoked if
     *                         the executor could not execute the given task.
     * @param runFinally       A Runnable that is guaranteed to be invoked at the end of this execution. If the executor
     *                         did accept the task, it will be invoked after the task is complete (or ended in failure).
     *                         If the executor did not accept the task, it will be executed when this method returns.
     * @param executor         An Executor to execute the task on.
     */
    public static void execute(RunnableWithException task, Consumer<Throwable> exceptionHandler, Runnable runFinally, Executor executor) {
        Preconditions.checkNotNull(task, "task");
        Preconditions.checkNotNull(exceptionHandler, "exceptionHandler");
        Preconditions.checkNotNull(runFinally, "runFinally");

        boolean scheduledSuccess = false;
        try {
            executor.execute(() -> {
                try {
                    task.run();
                } catch (Throwable ex) {
                    if (!Exceptions.mustRethrow(ex)) {
                        // Invoke the exception handler, but there's no point in rethrowing the exception, as it will simply
                        // be ignored by the executor.
                        exceptionHandler.accept(ex);
                    }
                } finally {
                    runFinally.run();
                }
            });

            scheduledSuccess = true;
        } finally {
            // Invoke the finally callback in case we were not able to successfully schedule the task.
            if (!scheduledSuccess) {
                runFinally.run();
            }
        }
    }

    /**
     * Shuts down the given ExecutorServices in two phases, using a timeout of 5 seconds:
     * 1. Prevents new tasks from being submitted.
     * 2. Awaits for currently running tasks to terminate. If they don't terminate within the given timeout, they will be
     * forcibly cancelled.
     *
     * @param pools   The ExecutorServices to shut down.
     */
    public static void shutdown(ExecutorService... pools) {
        shutdown(Duration.ofSeconds(5), pools);
    }

    /**
     * Shuts down the given ExecutorServices in two phases:
     * 1. Prevents new tasks from being submitted.
     * 2. Awaits for currently running tasks to terminate. If they don't terminate within the given timeout, they will be
     * forcibly cancelled.
     *
     * This is implemented as per the guidelines in the ExecutorService Javadoc:
     * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ExecutorService.html
     *
     * @param timeout Grace period that will be given to tasks to complete.
     * @param pools   The ExecutorServices to shut down.
     */
    public static void shutdown(Duration timeout, ExecutorService... pools) {
        // Prevent new tasks from being submitted.
        for (ExecutorService pool : pools) {
            pool.shutdown();
        }

        TimeoutTimer timer = new TimeoutTimer(timeout);
        for (ExecutorService pool : pools) {
            try {
                // Wait a while for existing tasks to terminate. Note that subsequent pools will be given a smaller timeout,
                // since they all started shutting down at the same time (above), and they can shut down in parallel.
                if (!pool.awaitTermination(timer.getRemaining().toMillis(), TimeUnit.MILLISECONDS)) {
                    // Cancel currently executing tasks and wait for them to respond to being cancelled.
                    pool.shutdownNow();
                    if (!pool.awaitTermination(timer.getRemaining().toMillis(), TimeUnit.MILLISECONDS)) {
                        List<Runnable> remainingTasks = pool.shutdownNow();
                        log.warn("One or more threads from pool " + pool
                                + " did not shutdown properly. Waiting tasks: " + remainingTasks);

                    }
                }
            } catch (InterruptedException ie) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Snapshot {
        @Getter
        final int queueSize;
        @Getter
        final int activeThreadCount;
        @Getter
        final int poolSize;
    }
}
