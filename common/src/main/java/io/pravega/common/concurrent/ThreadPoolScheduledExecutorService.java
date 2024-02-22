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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Data;
import lombok.Getter;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.AccessLevel;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * An implementation of {@link ScheduledExecutorService} which uses a thread pool. 
 * 
 * This class is similar to ScheduledThreadPoolExecutor but differs in the following ways:
 * 
 * 1. The thread pool supports growing. IE: {@code maxPoolSize} and {@code corePoolSize} don't have to be the same.
 * 2. Queued tasks are stored in a lock-free queue so that no bottlenecks can occur on submit.
 * 3. Scheduling a task without a delay is O(1) as opposed to O(log(n))
 * 4. Canceling a task actually removes it from the queue and is O(n) as opposed to a no-op which leaves it in the queue or O(log(n)) when {@code setRemoveOnCancelPolicy(true)}.
 * 5. The {@code mayInteruptIfRunning} flag on cancel is ignored and assumed to be false.
 * 6. {@code ContinueExistingPeriodicTasksAfterShutdown} and {@code ExecuteExistingDelayedTasksAfterShutdown} are always false.
 */
@Slf4j
@ToString(of = "runner")
public class ThreadPoolScheduledExecutorService extends AbstractExecutorService implements ScheduledExecutorService  {

    private static final AtomicLong COUNTER = new AtomicLong(0);
    @Getter(AccessLevel.PACKAGE)
    private final ThreadPoolExecutor runner;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ScheduledQueue<ScheduledRunnable<?>> queue;

    /**
     * Creates a fixed size thread pool (Similar to ScheduledThreadPoolExecutor).
     * 
     * @param corePoolSize The number of threads in the pool
     * @param threadFactory The factory used to create the threads.
     */
    public ThreadPoolScheduledExecutorService(int corePoolSize, ThreadFactory threadFactory) {
        this.queue = new ScheduledQueue<ScheduledRunnable<?>>();
        // While this cast looks invalid, it is ok because runner is private and will only
        // be given ScheduledRunnable which by definition implement runnable.
        @SuppressWarnings("unchecked")
        BlockingQueue<Runnable> queue = (BlockingQueue) this.queue;
        runner = new ThreadPoolExecutor(corePoolSize,
                                        corePoolSize,
                                        100,
                                        MILLISECONDS,
                                        queue,
                                        threadFactory,
                                        new AbortPolicy());
        runner.prestartAllCoreThreads();
    }

    @RequiredArgsConstructor
    @EqualsAndHashCode
    private final class CancelableFuture<R> implements ScheduledFuture<R> {

        private final ScheduledRunnable<R> task;

        @Override
        public long getDelay(TimeUnit unit) {
            if (!task.isDelayed) {
                return 0;
            }
            return unit.convert(task.scheduledTimeNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            if (other == this) { // compare zero if same object
                return 0;
            }
            if (other instanceof CancelableFuture) {
                return Long.compare(this.task.scheduledTimeNanos,
                                    ((CancelableFuture<?>) other).task.scheduledTimeNanos);
            } else {
                long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
                return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
            }
        }

        /**
         * Cancels a pending task. Note: The {@code mayInterruptIfRunning} parameter is ignored (and
         * assumed to be false) as it is unsupported.
         * 
         * @param mayInterruptIfRunning Ignored.
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return ThreadPoolScheduledExecutorService.this.cancel(task);
        }

        @Override
        public boolean isCancelled() {
            return task.future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return task.future.isDone();
        }

        @Override
        public R get() throws InterruptedException, ExecutionException {
            return task.future.get();
        }

        @Override
        public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return task.future.get(timeout, unit);
        }
    }

    @Data
    private static final class ScheduledRunnable<R> implements Runnable, Scheduled {
        private final long id;
        private final boolean isDelayed;
        private final long scheduledTimeNanos;
        @EqualsAndHashCode.Exclude
        private final Callable<R> task;
        @EqualsAndHashCode.Exclude
        private final CompletableFuture<R> future;

        private ScheduledRunnable(Callable<R> task) {
            this.id = COUNTER.incrementAndGet();
            this.isDelayed = false;
            this.scheduledTimeNanos = 0;
            this.task = task;
            this.future = new CompletableFuture<R>();
        }

        private ScheduledRunnable(Callable<R> task, long delay, TimeUnit unit) {
            this.id = COUNTER.incrementAndGet();
            this.isDelayed = true;
            this.scheduledTimeNanos = unit.toNanos(delay) + System.nanoTime();
            this.task = task;
            this.future = new CompletableFuture<R>();
            if (task instanceof ScheduleLoop) {
                ScheduleLoop loop = (ScheduleLoop) task;
                loop.currentTask.set(this);
            }
        }
        
        private ScheduledRunnable(Callable<R> task, long scheduledTimeNanos) {
            this.id = COUNTER.incrementAndGet();
            this.isDelayed = true;
            this.scheduledTimeNanos = scheduledTimeNanos;
            this.task = task;
            this.future = new CompletableFuture<R>();
        }

        @Override
        public void run() {
            try {
                future.complete(task.call());
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        }
    }

    @Override
    public void shutdown() {
        cancelDelayed();
        runner.shutdown();
    }

    private boolean cancel(ScheduledRunnable<?> task) {
        if (queue.remove(task)) {
            task.future.cancel(false);
            return true;
        }
        return false;
    }
    
    private void cancelDelayed() {
        for (ScheduledRunnable<?> item : queue.drainDelayed()) {
            item.future.cancel(false);
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        cancelDelayed();
        return runner.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return runner.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return runner.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return runner.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        runner.execute(new ScheduledRunnable<>(Executors.callable(command)));
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        ScheduledRunnable<?> task = new ScheduledRunnable<>(Executors.callable(command), delay, unit);
        runner.execute(task);
        return new CancelableFuture<>(task);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        ScheduledRunnable<V> task = new ScheduledRunnable<>(callable, delay, unit);
        runner.execute(task);
        return new CancelableFuture<>(task);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        FixedRateLoop loop = new FixedRateLoop(command, period, unit);
        ScheduledRunnable<?> task = new ScheduledRunnable<>(loop, initialDelay, unit);
        runner.execute(task);
        return loop;
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        FixedDelayLoop loop = new FixedDelayLoop(command, delay, unit);
        ScheduledRunnable<?> task = new ScheduledRunnable<>(loop, initialDelay, unit);
        runner.execute(task);
        return loop;
    }
    
    @RequiredArgsConstructor
    private abstract class ScheduleLoop implements Callable<Void>, ScheduledFuture<Void> {
        final Runnable command;
        final AtomicBoolean canceled = new AtomicBoolean(false);
        final CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        final AtomicReference<ScheduledFuture<Void>> scheduledFuture = new AtomicReference<ScheduledFuture<Void>>();
        final AtomicReference<ScheduledRunnable<?>> currentTask = new AtomicReference<>();
        @Override
        public Void call() {
            if (!canceled.get()) {
                try {
                    command.run();
                } catch (Throwable t) {
                    canceled.set(true);
                    log.error("Exception thrown out of root of recurring task: " + command + " This task will not run again!", t);
                    shutdownFuture.completeExceptionally(t);
                    return null;
                }
                if (!canceled.get()) {
                    try {
                        scheduledFuture.set(schedule());
                    } catch (RejectedExecutionException e) {
                        //Pool has shutdown
                        log.debug("Shutting down task {} because pool {} has shutdown.", command, runner);
                        cancel(false);
                    }
                }
            } 
            return null;
        }

        abstract ScheduledFuture<Void> schedule();

        @Override
        public int compareTo(Delayed other) {
            if (other == this) { // compare zero if same object
                return 0;
            }
            long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
            return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (canceled.getAndSet(true)) {
                return false;
            }
            ScheduledRunnable<?> task = this.currentTask.get();
            if (task != null) {
                ThreadPoolScheduledExecutorService.this.cancel(task);
            }
            ScheduledFuture<Void> future = scheduledFuture.get();
            if (future != null) {
                future.cancel(mayInterruptIfRunning);
            }
            shutdownFuture.completeExceptionally(new CancellationException());
            return true;
        }

        @Override
        public boolean isCancelled() {
            return canceled.get();
        }

        @Override
        public boolean isDone() {
            return canceled.get();
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            return shutdownFuture.get();
        }

        @Override
        public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return shutdownFuture.get(timeout, unit);
        }
    }

    private class FixedDelayLoop extends ScheduleLoop {
        private final long delay;
        private final TimeUnit unit;

        public FixedDelayLoop(Runnable command, long delay, TimeUnit unit) {
            super(command);
            this.delay = delay;
            this.unit = unit;
        }
        
        @Override
        public long getDelay(TimeUnit returnUnit) {
            return returnUnit.convert(delay, unit);
        }

        @Override
        ScheduledFuture<Void> schedule() {
            return ThreadPoolScheduledExecutorService.this.schedule(this, delay, unit);
        }
    }
    
    private class FixedRateLoop extends ScheduleLoop {
        private final long periodNanos;
        private final AtomicLong startTimeNanos;

        public FixedRateLoop(Runnable command, long period, TimeUnit unit) {
            super(command);
            this.startTimeNanos = new AtomicLong(System.nanoTime());
            this.periodNanos = unit.toNanos(period);
        }
        
        
        @Override
        public long getDelay(TimeUnit returnUnit) {
            return returnUnit.convert(periodNanos, NANOSECONDS);
        }

        @Override
        public Void call() {
            startTimeNanos.set(System.nanoTime());
            return super.call();
        }
        
        @Override
        ScheduledFuture<Void> schedule() {
            ScheduledRunnable<Void> task = new ScheduledRunnable<>(this, startTimeNanos.get() + periodNanos);
            runner.execute(task);
            return new CancelableFuture<>(task);
        }
    }

    ThreadFactory getThreadFactory() {
        return runner.getThreadFactory();
    }
    
}
