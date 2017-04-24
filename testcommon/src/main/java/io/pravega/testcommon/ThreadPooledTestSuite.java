/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.testcommon;

import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Base class for a Unit Test class that makes extensive use of Thread Pools.
 * Automatically creates and shuts down a ScheduledExecutorService with every test, with no further code needed.
 */
public abstract class ThreadPooledTestSuite {
    private static final int INLINE_THREAD_COUNT = 0;
    private ScheduledExecutorService executorService = null;

    @Before
    public void before() {
        int threadPoolSize = getThreadPoolSize();
        this.executorService = threadPoolSize == INLINE_THREAD_COUNT ? new InlineExecutor() : Executors.newScheduledThreadPool(threadPoolSize);
    }

    @After
    public void after() {
        this.executorService.shutdown();
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
}
