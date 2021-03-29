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
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiKeyLatestItemSequentialProcessorTest {

    @Test
    public void testRunsItems() {
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        AtomicBoolean ran = new AtomicBoolean(false);
        MultiKeyLatestItemSequentialProcessor<String, String> processor =
                new MultiKeyLatestItemSequentialProcessor<>((k, v) -> ran.set(true), executor);
        processor.updateItem("k1", "Foo");
        assertTrue(ran.get());
    }

    @Test
    public void testSkipsOverItemsSingleKey() throws InterruptedException {
        @Cleanup("shutdown")
        ExecutorService pool = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        ReusableLatch startedLatch = new ReusableLatch(false);
        ReusableLatch latch = new ReusableLatch(false);
        Vector<String> processed = new Vector<>();
        MultiKeyLatestItemSequentialProcessor<String, String> processor =
                new MultiKeyLatestItemSequentialProcessor<>((k, v) -> {
                    startedLatch.release();
                    latch.awaitUninterruptibly();
                    processed.add(v);
                }, pool);

        processor.updateItem("k", "a");
        processor.updateItem("k", "b");
        processor.updateItem("k", "c");
        startedLatch.await();
        latch.release();
        ExecutorServiceHelpers.shutdown(pool);
        assertEquals(ImmutableList.of("a", "c"), processed);
    }

    @Test
    public void testSkipsOverItemsMultipleKey() throws InterruptedException {
        @Cleanup("shutdown")
        ExecutorService pool = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        ReusableLatch startedLatch = new ReusableLatch(false);
        ReusableLatch latch = new ReusableLatch(false);
        Vector<String> processed = new Vector<>();
        MultiKeyLatestItemSequentialProcessor<String, String> processor =
                new MultiKeyLatestItemSequentialProcessor<>((k, v) -> {
                    startedLatch.release();
                    latch.awaitUninterruptibly();
                    processed.add(v);
                }, pool);

        processor.updateItem("k", "a");
        processor.updateItem("k1", "x");
        processor.updateItem("k", "b");
        processor.updateItem("k1", "y");
        processor.updateItem("k", "c");
        processor.updateItem("k1", "z");
        startedLatch.await();
        latch.release();
        ExecutorServiceHelpers.shutdown(pool);
        assertEquals(ImmutableList.of("a", "c", "x", "z"), processed);
    }

    @Test
    public void testNotCalledInParallel() {
        @Cleanup("shutdown")
        ExecutorService pool = ExecutorServiceHelpers.newScheduledThreadPool(2, "test");
        CountDownLatch parCheck = new CountDownLatch(2);
        ReusableLatch latch = new ReusableLatch(false);
        MultiKeyLatestItemSequentialProcessor<String, String> processor = new MultiKeyLatestItemSequentialProcessor<>(
                (k, v) -> {
                    parCheck.countDown();
                    latch.awaitUninterruptibly();
                }, pool);

        processor.updateItem("k", "a");
        processor.updateItem("k", "b");
        processor.updateItem("k", "c");
        AssertExtensions.assertBlocks(() -> parCheck.await(), () -> parCheck.countDown());
        latch.release();
        ExecutorServiceHelpers.shutdown(pool);
    }

    @Test
    public void testMultipleKeyParallelInvocation() {
        @Cleanup("shutdown")
        ExecutorService pool = ExecutorServiceHelpers.newScheduledThreadPool(2, "test");

        CountDownLatch key1Check = new CountDownLatch(2);
        CountDownLatch key2Check = new CountDownLatch(2);
        ReusableLatch latch = new ReusableLatch(false);

        MultiKeyLatestItemSequentialProcessor<String, String> processor = new MultiKeyLatestItemSequentialProcessor<>(
                (k, v) -> {
                    if (k.equals("k1")) {
                        key1Check.countDown(); // count down latch only for k1
                    } else {
                        key2Check.countDown(); // count down latch only for k2.
                    }
                    latch.awaitUninterruptibly();
                }, pool);

        processor.updateItem("k1", "a");
        processor.updateItem("k1", "b");
        processor.updateItem("k1", "c");
        processor.updateItem("k2", "x");
        processor.updateItem("k2", "y");
        processor.updateItem("k2", "z");

        // validate parallel invocation for the same key does not happen.
        AssertExtensions.assertBlocks(() -> key1Check.await(), () -> key1Check.countDown());
        AssertExtensions.assertBlocks(() -> key2Check.await(), () -> key2Check.countDown());
        latch.release();
        ExecutorServiceHelpers.shutdown(pool);
    }
}
