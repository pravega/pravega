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
import io.pravega.common.Exceptions;
import io.pravega.common.function.RunnableWithException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import javax.annotation.concurrent.GuardedBy;

/**
 * An Executor extension that runs the same task asynchronously, but never concurrently. If multiple requests are made
 * during an existing execution of the task, it will be invoked exactly once after the current execution completes.
 */
public class SequentialAsyncProcessor implements AutoCloseable {
    //region Members

    private final RunnableWithException runnable;
    private final BiFunction<Throwable, Integer, Boolean> errorHandler;
    private final Executor executor;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private boolean runAgain;
    @GuardedBy("this")
    private boolean closed;
    private final AtomicInteger consecutiveFailedAttempts;

    //endregion

    //region Constructor

    /**
     * Region Constructor.
     *
     * @param runnable The task to run.
     * @param errorHandler A BiFunction that will be invoked when runnable throws exceptions. First argument is the exception
     *                     itself, while the second one is the number of consecutive failed attempts. This Function will
     *                     return true if we should reinvoke the runnable or false otherwise.
     * @param executor An Executor to run the task on.
     */
    public SequentialAsyncProcessor(RunnableWithException runnable, BiFunction<Throwable, Integer, Boolean> errorHandler, Executor executor) {
        this.runnable = Preconditions.checkNotNull(runnable, "runnable");
        this.errorHandler = Preconditions.checkNotNull(errorHandler, "errorHandler");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.consecutiveFailedAttempts = new AtomicInteger();
    }

    //endregion

    //region Execution

    /**
     * Executes one instance of the task, or queues it up at most once should the task be currently running.
     */
    public void runAsync() {
        // Determine if a task is running. If so, record the fact we want to have it run again, otherwise reserve our spot.
        synchronized (this) {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.running) {
                this.runAgain = true;
                return;
            }

            this.running = true;
        }

        // Execute the task.
        this.executor.execute(() -> {
            boolean canContinue = true;
            while (canContinue) {
                try {
                    this.runnable.run();
                    this.consecutiveFailedAttempts.set(0);
                } catch (Throwable ex) {
                    int c = this.consecutiveFailedAttempts.incrementAndGet();
                    if (ExceptionHelpers.mustRethrow(ex)) {
                        close();
                    }

                    boolean retry = this.errorHandler.apply(ex, c);
                    if (retry) {
                        synchronized (this) {
                            this.runAgain = true;
                        }
                    }
                } finally {
                    // Determine if we need to run the task again. Otherwise release our spot.
                    synchronized (this) {
                        canContinue = this.runAgain && !this.closed;
                        this.runAgain = false;
                        this.running = canContinue;
                    }
                }
            }
        });
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    //endregion
}
