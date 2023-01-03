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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Data;

/**
 * A mock of an executor service that does not execute tasks but instead collects them in memory.
 */
public class CollectingExecutor implements ScheduledExecutorService {

    private final List<Runnable> tasks = new Vector<>();
    
    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return null;
    }

    @Override
    public boolean isShutdown() {
        return true;
    }

    @Override
    public boolean isTerminated() {
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        tasks.add(() -> {
            try {
                future.complete(task.call());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    @Override
    public <T> CompletableFuture<T> submit(Runnable task, T result) {
        return submit(() -> {
            task.run();
            return result;
        });
    }

    @Override
    public CompletableFuture<?> submit(Runnable task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        tasks.add(() -> {
            try {
                task.run();
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
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
    public void execute(Runnable command) {
        tasks.add(command);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        CompletableFuture<?> future = submit(command);
        return new NonScheduledFuture<>(future, TimeUnit.MILLISECONDS.convert(delay, unit));
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        CompletableFuture<V> future = submit(callable);
        return new NonScheduledFuture<>(future, TimeUnit.MILLISECONDS.convert(delay, unit));
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        CompletableFuture<?> future = submit(command);
        return new NonScheduledFuture<>(future, TimeUnit.MILLISECONDS.convert(initialDelay, unit));
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        CompletableFuture<?> future = submit(command);
        return new NonScheduledFuture<>(future, TimeUnit.MILLISECONDS.convert(initialDelay, unit));
    }

    public List<Runnable> getScheduledTasks() { 
        return new ArrayList<>(tasks);
    }
    
    @Data
    private class NonScheduledFuture<T> implements ScheduledFuture<T> {
        private final CompletableFuture<T> wrappedFuture;
        private final long delay;
        
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delay, TimeUnit.MILLISECONDS);
        }
        
        @Override
        public int compareTo(Delayed o) {
            return Long.compare(delay, o.getDelay(TimeUnit.MILLISECONDS));
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return wrappedFuture.cancel(mayInterruptIfRunning);
        }
        
        @Override
        public boolean isCancelled() {
            return wrappedFuture.isCancelled();
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
