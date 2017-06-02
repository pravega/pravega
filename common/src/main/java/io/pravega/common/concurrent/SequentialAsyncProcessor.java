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
import io.pravega.common.Exceptions;
import java.util.concurrent.Executor;
import javax.annotation.concurrent.GuardedBy;

/**
 * An Executor extension that runs the same task asynchronously, but never concurrently. If multiple requests are made
 * during an existing execution of the task, it will be invoked exactly once after the current execution completes.
 */
public class SequentialAsyncProcessor implements AutoCloseable {
    //region Members

    private final Runnable runnable;
    private final Executor executor;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private boolean runAgain;
    @GuardedBy("this")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Region Constructor.
     *
     * @param runnable The task to run.
     * @param executor An Executor to run the task on.
     */
    public SequentialAsyncProcessor(Runnable runnable, Executor executor) {
        this.runnable = Preconditions.checkNotNull(runnable, "runnable");
        this.executor = Preconditions.checkNotNull(executor, "executor");
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
