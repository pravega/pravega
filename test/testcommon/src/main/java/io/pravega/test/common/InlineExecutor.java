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
package io.pravega.test.common;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * An Executor that runs commands that don't have a delay inline when they are submitted.
 * Delayed tasks are run on a background thread.
 */
public class InlineExecutor implements ScheduledExecutorService, AutoCloseable {
    private final ScheduledExecutorService delayedExecutor;

    public InlineExecutor() {
        this.delayedExecutor = ThreadPooledTestSuite.createExecutorService(1);
    }

    @Override
    public void execute(Runnable command) {
        command.run();
    }

    @Override
    public void close() {
        shutdown();
    }
    
    @Override
    public void shutdown() {
        delayedExecutor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delayedExecutor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delayedExecutor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delayedExecutor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delayedExecutor.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        try {
            return completedFuture(task.call());
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        try {
            task.run();
            return completedFuture(result);
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    @Override
    public Future<?> submit(Runnable task) {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        try {
            task.run();
            return completedFuture(null);
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        List<Future<T>> result = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            result.add(submit(task));
        }
        return result;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return invokeAll(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        for (Callable<T> task : tasks) {
            try {
                return task.call();
            } catch (Exception e) {
                continue;
            }
        }
        return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAny(tasks);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        if (delay == 0) {
            try {
                command.run();
                return new NonScheduledFuture<>(completedFuture(null));
            } catch (Exception e) {
                return new NonScheduledFuture<>(failedFuture(e));
            }
        } else {
            return delayedExecutor.schedule(command, delay, unit);
        }
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        if (delay == 0) {
            try {
                return new NonScheduledFuture<>(completedFuture(callable.call()));
            } catch (Exception e) {
                return new NonScheduledFuture<>(failedFuture(e));
            }
        } else {
            return delayedExecutor.schedule(callable, delay, unit);
        }
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        if (initialDelay == 0) {
            try {
                command.run();
                initialDelay = period;
            } catch (Exception e) {
                return new NonScheduledFuture<>(failedFuture(e));
            }
        }
        return delayedExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        Preconditions.checkState(!delayedExecutor.isShutdown());
        if (initialDelay == 0) {
            try {
                command.run();
                initialDelay = delay;
            } catch (Exception e) {
                return new NonScheduledFuture<>(failedFuture(e));
            }
        }
        return delayedExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable exception) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(exception);
        return result;
    }
    
    @RequiredArgsConstructor
    @EqualsAndHashCode
    public static class NonScheduledFuture<T> implements ScheduledFuture<T> {
        private final CompletableFuture<T> wrappedFuture;

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(0, o.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return wrappedFuture.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return wrappedFuture.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return wrappedFuture.get(timeout, unit);
        }
    }
}

