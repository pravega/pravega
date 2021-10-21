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

import com.google.common.collect.ImmutableList;
import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LatestItemSequentialProcessorTest {

    @Test
    public void testRunsItems() {
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        AtomicBoolean ran = new AtomicBoolean(false);
        LatestItemSequentialProcessor<String> processor = new LatestItemSequentialProcessor<>(s -> ran.set(true), executor);
        processor.updateItem("Foo");
        assertTrue(ran.get());        
    }
    
    @Test
    public void testSkipsOverItems() throws InterruptedException {
        @Cleanup("shutdown")
        ExecutorService pool = Executors.newFixedThreadPool(1);
        ReusableLatch startedLatch = new ReusableLatch(false); 
        ReusableLatch latch = new ReusableLatch(false);
        Vector<String> processed = new Vector<>();
        LatestItemSequentialProcessor<String> processor = new LatestItemSequentialProcessor<>(s -> {
            startedLatch.release();
            latch.awaitUninterruptibly();
            processed.add(s);
        }, pool);
        processor.updateItem("a");
        processor.updateItem("b");
        processor.updateItem("c");
        startedLatch.await();
        latch.release();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        assertEquals(ImmutableList.of("a", "c"), processed);
    }
    
    @Test
    public void testNotCalledInParallel() throws InterruptedException {
        @Cleanup("shutdown")
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch parCheck = new CountDownLatch(2);
        ReusableLatch latch = new ReusableLatch(false);
        LatestItemSequentialProcessor<String> processor = new LatestItemSequentialProcessor<>(s -> {
            parCheck.countDown();
            latch.awaitUninterruptibly();
        }, pool);
        processor.updateItem("a");
        processor.updateItem("b");
        processor.updateItem("c");
        AssertExtensions.assertBlocks(() -> parCheck.await(), () -> parCheck.countDown());
        latch.release();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }
    
}
