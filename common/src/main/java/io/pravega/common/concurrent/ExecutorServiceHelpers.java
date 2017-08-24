/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.function.RunnableWithException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

/**
 * Helper methods for ExecutorService.
 */
public final class ExecutorServiceHelpers {
    
    private static class CallerRuns implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            r.run();
        }
    }
    
    /**
     * Creates and returns a thread factory that will create threads with the given name prefix.
     * 
     * @param groupName the name of the threads
     * @return a thread factory
     */
    public static ThreadFactory getThreadFactory(String groupName) {
        return new ThreadFactory() {
            final AtomicInteger threadCount = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, groupName + "-" + threadCount.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        };
    }
    
    /**
     * Creates a new ScheduledExecutorService that will use daemon threads with appropriate names the threads.
     * @param size The number of threads in the threadpool
     * @param poolName The name of the pool (this will be printed in logs)
     * @return A new executor service.
     */
    public static ScheduledExecutorService newScheduledThreadPool(int size, String poolName) {
        // Caller runs only occurs after shutdown, as queue size is unbounded.
        ScheduledThreadPoolExecutor result = new ScheduledThreadPoolExecutor(size, getThreadFactory(poolName), new CallerRuns()); 
        result.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        result.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        result.setRemoveOnCancelPolicy(true);
        return result;
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
        } else {
            return null;
        }
    }
    
    public static ThreadPoolExecutor getShrinkingExecutor(int maxThreads, int threadTimeout, String poolName) {
        return new ThreadPoolExecutor(0, maxThreads, threadTimeout, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                getThreadFactory(poolName), new CallerRuns()); // Caller runs only occurs after shutdown, as queue size is unbounded.
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
                    if (!ExceptionHelpers.mustRethrow(ex)) {
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
