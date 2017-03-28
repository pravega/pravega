/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega;

import com.emc.pravega.testcommon.TestUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class to verify correctness of utility methods.
 */
public class UtilityMethodsTest {

    /**
     * Test to verify correctness of getAvailableListenPort() method.
     *
     * @throws InterruptedException Required for ExecutorService.invokeAll
     * @throws ExecutionException   Required for Futures.get()
     */
    @Test(timeout = 10000)
    public void getAvailableListenPortTest() throws InterruptedException, ExecutionException {
        final int threadCount = 5;
        Callable<Integer> task = () -> TestUtils.getAvailableListenPort();
        List<Callable<Integer>> tasks = Collections.nCopies(threadCount, task);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = executorService.invokeAll(tasks);
        Set<Integer> resultList = new HashSet<>(futures.size());

        // Set cannot hold duplicate elements.
        // Add ports returned by getAvailableListenPort() to the set to verify they are unique.
        for (Future<Integer> future : futures) {
            int port = future.get();
            assertTrue(!resultList.contains(port));
            resultList.add(port);
        }
        assertEquals(threadCount, futures.size());
        executorService.shutdown();
    }
}
