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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A fake implementation of ScheduledExecutorService that does nothing. An instance of this class may be used as a
 * placeholder for unused scheduled executor in tests.
 */
public class NoOpScheduledExecutor implements ScheduledExecutorService {

    private static final NoOpScheduledExecutor INSTANCE = new NoOpScheduledExecutor();

    private AtomicBoolean isShutDown = new AtomicBoolean(false);

    public static NoOpScheduledExecutor get() {
        return INSTANCE;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable runnable, long l, TimeUnit timeUnit) {
        return new DummyScheduledFuture(0);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long l, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long l, long l1, TimeUnit timeUnit) {
        return new DummyScheduledFuture(0);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, long l, long l1, TimeUnit timeUnit) {
        return new DummyScheduledFuture(0);
    }

    @Override
    public void shutdown() {
        isShutDown.set(true);
    }

    @Override
    public List<Runnable> shutdownNow() {
        isShutDown.set(true);
        return new ArrayList<>();
    }

    @Override
    public boolean isShutdown() {
        return isShutDown.get();
    }

    @Override
    public boolean isTerminated() {
        return isShutDown.get();
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit) {
        return isShutDown.get();
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <T> Future<T> submit(Runnable runnable, T t) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<?> submit(Runnable runnable) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection) {
        return new ArrayList<>();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) {
        return new ArrayList<>();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable runnable) {
    }

    @EqualsAndHashCode // Required to keep Spotbugs satisfied (owing to code in `compareTo()`).
    @RequiredArgsConstructor
    private static class DummyScheduledFuture implements ScheduledFuture<Integer> {

        @NonNull
        @Getter
        private final Integer value;

        private boolean isCancelled = false;

        @Override
        public long getDelay(TimeUnit timeUnit) {
            return 0L;
        }

        @Override
        public int compareTo(Delayed other) {
            DummyScheduledFuture otherTask = (DummyScheduledFuture) other;
            if (this.value > otherTask.getValue()) {
                return 1;
            } else if (this.value < otherTask.getValue()) {
                return -1;
            } else {
                return 0;
            }
        }

        @Override
        public boolean cancel(boolean b) {
            this.isCancelled = b;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Integer get() {
            return this.value;
        }

        @Override
        public Integer get(long l, TimeUnit timeUnit) {
            return this.value;
        }
    }
}
