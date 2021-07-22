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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates Thread Pools for use within Pravega codebase.
 */
@Slf4j
final class ExecutorServiceFactory {
    //region Members

    @VisibleForTesting
    static final String DETECTION_LEVEL_PROPERTY_NAME = "ThreadLeakDetectionLevel";
    @VisibleForTesting
    private final ThreadLeakDetectionLevel detectionLevel;
    private final CreateScheduledExecutor createScheduledExecutor;
    private final CreateShrinkingExecutor createShrinkingExecutor;
    private final Runnable onLeakDetected;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link ExecutorServiceFactory} which extracts the {@link ThreadLeakDetectionLevel}
     * from system properties and halts the VM if a leak is detected and {@link ThreadLeakDetectionLevel} is set to
     * {@link ThreadLeakDetectionLevel#Aggressive}.
     */
    ExecutorServiceFactory() {
        this(getDetectionLevel(), () -> System.exit(99));
    }

    /**
     * Creates a new instance of the {@link ExecutorServiceFactory}.
     *
     * @param level          The {@link ThreadLeakDetectionLevel} to use.
     * @param onLeakDetected A {@link Runnable} that will be invoked if {@link ThreadLeakDetectionLevel} is set to
     *                       {@link ThreadLeakDetectionLevel#Aggressive} and a leak is detected.
     */
    @VisibleForTesting
    ExecutorServiceFactory(@NonNull ThreadLeakDetectionLevel level, @NonNull Runnable onLeakDetected) {
        this.detectionLevel = level;
        this.onLeakDetected = onLeakDetected;

        // In all of the below, the ThreadFactory is created in this class, and its toString() returns the pool name.
        if (this.detectionLevel == ThreadLeakDetectionLevel.None) {
            this.createScheduledExecutor = (size, factory) -> new ThreadPoolScheduledExecutorService(size, factory);
            this.createShrinkingExecutor = (maxThreadCount, threadTimeout, factory) ->
                    new ThreadPoolExecutor(0, maxThreadCount, threadTimeout, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(), factory, new CallerRuns(factory.toString()));
        } else {
            // Light and Aggressive need a special executor that overrides the finalize() method.
            this.createScheduledExecutor = (size, factory) -> {
                logNewThreadPoolCreated(factory.toString());
                return new LeakDetectorScheduledExecutorService(size, factory);
            };
            this.createShrinkingExecutor = (maxThreadCount, threadTimeout, factory) -> {
                logNewThreadPoolCreated(factory.toString());
                return new LeakDetectorThreadPoolExecutor(0, maxThreadCount, threadTimeout, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(), factory, new CallerRuns(factory.toString()));
            };
        }
    }

    /**
     * Gets the {@link ThreadLeakDetectionLevel} from System Properties (Key = {@link #DETECTION_LEVEL_PROPERTY_NAME}).
     *
     * @return The {@link ThreadLeakDetectionLevel}, or {@link ThreadLeakDetectionLevel#None} if not defined in System
     * Properties.
     * @throws IllegalArgumentException If the system property defines a value is not valid.
     */
    @VisibleForTesting
    static ThreadLeakDetectionLevel getDetectionLevel() {
        return ThreadLeakDetectionLevel.valueOf(
                System.getProperty("ThreadLeakDetectionLevel", ThreadLeakDetectionLevel.None.name()));
    }

    //endregion

    //region Factory Methods

    /**
     * Creates and returns a thread factory that will create threads with the given name prefix.
     *
     * @param groupName the name of the threads
     * @return a thread factory
     */
    ThreadFactory getThreadFactory(String groupName) {
        return getThreadFactory(groupName, Thread.NORM_PRIORITY);
    }

    /**
     * Creates and returns a thread factory that will create threads with the given name prefix and thread priority.
     *
     * @param groupName the name of the threads
     * @param priority  the priority to be assigned to the thread.
     * @return a thread factory
     */
    ThreadFactory getThreadFactory(String groupName, int priority) {
        return new ThreadFactory() {
            final AtomicInteger threadCount = new AtomicInteger();

            @Override
            public String toString() {
                return groupName;
            }

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, groupName + "-" + threadCount.incrementAndGet());
                thread.setUncaughtExceptionHandler(new LogUncaughtExceptions());
                thread.setDaemon(true);
                thread.setPriority(priority);
                return thread;
            }
        };
    }

    /**
     * Creates a new ScheduledExecutorService that will use daemon threads with specified priority and names.
     *
     * @param size           The number of threads in the threadpool
     * @param poolName       The name of the pool (this will be printed in logs)
     * @param threadPriority The priority to be assigned to the threads
     * @return A new executor service.
     */
    ScheduledExecutorService newScheduledThreadPool(int size, String poolName, int threadPriority) {
        ThreadFactory threadFactory = getThreadFactory(poolName, threadPriority);

        // Caller runs only occurs after shutdown, as queue size is unbounded.
        ThreadPoolScheduledExecutorService result = this.createScheduledExecutor.apply(size, threadFactory);
        // ThreadPoolScheduledExecutorService implies:
        // setContinueExistingPeriodicTasksAfterShutdownPolicy(false),
        // setExecuteExistingDelayedTasksAfterShutdownPolicy(false),
        // setRemoveOnCancelPolicy(true);
        return result;
    }

    /**
     * Operates like Executors.cachedThreadPool but with a custom thread timeout and pool name.
     *
     * @param maxThreadCount The maximum number of threads to allow in the pool.
     * @param threadTimeout  the number of milliseconds that a thread should sit idle before shutting down.
     * @param poolName       The name of the threadpool.
     * @return A new threadPool
     */
    ThreadPoolExecutor newShrinkingExecutor(int maxThreadCount, int threadTimeout, String poolName) {
        ThreadFactory factory = getThreadFactory(poolName);
        return this.createShrinkingExecutor.apply(maxThreadCount, threadTimeout, factory);
    }

    //endregion

    //region ThreadFactory Helper Classes

    private static final class LogUncaughtExceptions implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("Exception thrown out of root of thread: " + t.getName(), e);
        }
    }

    @Data
    private static class CallerRuns implements RejectedExecutionHandler {
        private final String poolName;

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            log.debug("Caller to executor: " + poolName + " rejected and run in the caller.");
            if (!executor.isShutdown()) {
                r.run();
            }
        }
    }

    //endregion

    //region Leak Detection Pools

    private class LeakDetectorScheduledExecutorService extends ThreadPoolScheduledExecutorService {
        private final Exception stackTraceEx;

        LeakDetectorScheduledExecutorService(int corePoolSize, ThreadFactory threadFactory) {
            super(corePoolSize, threadFactory);
            this.stackTraceEx = new Exception();
        }

        @SuppressWarnings("deprecation")
        @Override
        protected void finalize() throws Throwable {
            checkThreadPoolLeak(this, this.stackTraceEx);
            super.finalize();
        }
    }

    private class LeakDetectorThreadPoolExecutor extends ThreadPoolExecutor {
        private final Exception stackTraceEx;

        LeakDetectorThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue,
                                       ThreadFactory threadFactory, RejectedExecutionHandler handler) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
            this.stackTraceEx = new Exception();
        }

        @SuppressWarnings("deprecation")
        @Override
        protected void finalize() {
            checkThreadPoolLeak(this, this.stackTraceEx);
            super.finalize();
        }
    }

    private void logNewThreadPoolCreated(String poolName) {
        if (this.detectionLevel == ThreadLeakDetectionLevel.Light) {
            log.debug("Created Thread Pool '{}' with leak detection level set to '{}'.", poolName, this.detectionLevel);
        } else if (this.detectionLevel == ThreadLeakDetectionLevel.Aggressive) {
            log.warn("Created Thread Pool '{}' with leak detection level set to '{}'. THE VM WILL BE HALTED IF A LEAK IS DETECTED. DO NOT USE IN PRODUCTION.", poolName, this.detectionLevel);
        }
    }

    @VisibleForTesting
    void checkThreadPoolLeak(ExecutorService e, Exception stackTraceEx) {
        if (this.detectionLevel == ThreadLeakDetectionLevel.None) {
            // Not doing anything in this case.
            return;
        }

        if (!e.isShutdown()) {
            log.warn("THREAD POOL LEAK: {} (ShutDown={}, Terminated={}) finalized without being properly shut down.",
                    e, e.isShutdown(), e.isTerminated(), stackTraceEx);
            if (this.detectionLevel == ThreadLeakDetectionLevel.Aggressive) {
                // Not pretty, but outputting this stack trace on System.err helps with those unit tests that turned off
                // logging.
                stackTraceEx.printStackTrace(System.err);
                log.error("THREAD POOL LEAK DETECTED WITH LEVEL SET TO {}. SHUTTING DOWN.", ThreadLeakDetectionLevel.Aggressive);
                this.onLeakDetected.run();
            }
        }
    }

    enum ThreadLeakDetectionLevel {
        /**
         * No detection. All Executors created are from the JDK.
         */
        None,
        /**
         * Lightweight detection. All Executors are decorated with non-invasive wrapper that collects the stack trace
         * at (Executor) construction time and overrides the {@link #finalize()} method to detect leaks.
         * <p>
         * Logs an ERROR log message, along with a stack trace indicating the place where the thread pool was created.
         */
        Light,
        /**
         * Same as {@link #Light}, but additionally halts the VM if a thread pool leak is detected.
         * DO NOT RUN IN A PRODUCTION ENVIRONMENT.
         */
        Aggressive
    }

    @FunctionalInterface
    private interface CreateScheduledExecutor {
        ThreadPoolScheduledExecutorService apply(int size, ThreadFactory factory);
    }

    @FunctionalInterface
    private interface CreateShrinkingExecutor {
        ThreadPoolExecutor apply(int maxThreadCount, int threadTimeout, ThreadFactory factory);
    }

    //endregion
}
