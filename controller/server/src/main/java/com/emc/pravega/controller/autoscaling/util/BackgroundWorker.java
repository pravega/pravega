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
package com.emc.pravega.controller.autoscaling.util;

import lombok.Data;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Base class for Background worker class. which asynchronously polls a queue to receive work items.
 * If an item is extracted from the queue it is sent for processing. Otherwise the thread is relinquished and
 * another poll is scheduled after a period.
 */
@Data
public abstract class BackgroundWorker<T> implements Runnable {
    private final ScheduledThreadPoolExecutor executor;

    private volatile boolean stopped = false;

    /**
     * Start the worker.
     */
    public void start() {
        if (!stopped) {
            CompletableFuture.runAsync(this, executor);
        }
    }

    /**
     * Stop the worker.
     */
    public void stop() {
        stopped = true;
        executor.remove(this);
    }

    /**
     * Schedule the work asynchronously after a delay.
     *
     * @param delay duration after which to poll again
     */
    public void schedule(final Duration delay) {
        if (!stopped) {
            try {
                executor.schedule(this, delay.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Schedule the work asynchronously without delay.
     */
    public void schedule() {
        if (!stopped) {
            CompletableFuture.runAsync(this, executor);
        }
    }

    @Override
    public void run() {
        final T e = dequeue();
        if (e == null) {
            // relinquish, go back in the queue. Let it be scheduled again after 1 ms and depending on number of threads
            schedule(Duration.ofMillis(1));
        } else {
            // Process the message asynchronously. Once processing is complete, schedule to poll the queue again.
            CompletableFuture.runAsync(() -> process(e)).thenAccept(x -> schedule());
        }
    }

    public abstract T dequeue();

    public abstract void process(T e);
}
