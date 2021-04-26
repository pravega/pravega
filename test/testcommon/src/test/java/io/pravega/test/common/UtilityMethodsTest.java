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

import lombok.Cleanup;
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
        @Cleanup("shutdownNow")
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
