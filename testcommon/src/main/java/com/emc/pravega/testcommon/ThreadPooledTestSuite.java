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

package com.emc.pravega.testcommon;

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
