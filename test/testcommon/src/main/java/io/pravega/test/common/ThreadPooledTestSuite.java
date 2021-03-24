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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for a Unit Test class that makes extensive use of Thread Pools.
 * Automatically creates and shuts down a ScheduledExecutorService with every test, with no further code needed.
 */
public abstract class ThreadPooledTestSuite {
    private static final int INLINE_THREAD_COUNT = 0;
    private ScheduledExecutorService executorService = null;

    @Before
    public void before() throws Exception {
        int threadPoolSize = getThreadPoolSize();
        this.executorService = threadPoolSize == INLINE_THREAD_COUNT ? new InlineExecutor() : createExecutorService(threadPoolSize);
    }

    @After
    public void after() throws Exception {
        this.executorService.shutdownNow();
        this.executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Gets a pointer to the ScheduledExecutorService to use.
     */
    protected ScheduledExecutorService executorService() {
        return this.executorService;
    }

    /**
     * When overridden in a derived class, indicates how many threads should be in the thread pool.
     * If this method returns 0 (default value), then an InlineExecutor is used; otherwise a regular ThreadPool is used.
     */
    protected int getThreadPoolSize() {
        return INLINE_THREAD_COUNT;
    }

    /**
     * Creates a new {@link ScheduledExecutorService} that automatically cancels ongoing tasks when shut down.
     * This is the same as ExecutorServiceHelpers.newScheduledThreadPool, however that class is not accessible from here.
     *
     * @param threadPoolSize Maximum number of threads in the pool.
     * @return A new {@link ScheduledExecutorService} instance.
     */
    static ScheduledExecutorService createExecutorService(int threadPoolSize) {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(threadPoolSize);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        return es;
    }
}
