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
package io.pravega.segmentstore.server.containers;

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ContainerEventProcessorTests extends ThreadPooledTestSuite {

    /**
     * Check that the max number of elements processed per EventProcessor iteration is respected.
     *
     * @throws Exception
     */
    @Test
    public void testContainerMaxItemsRespected() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(), this.executorService());
        eventProcessorService.startAsync().awaitRunning();

        int maxItemsProcessed = 10;
        int allEventsToProcess = 100;
        AtomicBoolean failedAssertion = new AtomicBoolean(false);
        AtomicLong processedItems = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            failedAssertion.set(failedAssertion.get() || maxItemsProcessed < l.size());
            processedItems.addAndGet(l.size());
            return CompletableFuture.completedFuture(null);
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed);
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegment", handler, config);

        // Write all the events as fast as possible.
        int expectedInternalProcessorOffset = 0;
        for (int i = 0; i < allEventsToProcess; i++) {
            BufferView event = BufferView.builder().add(new ByteArraySegment(("event" + i).getBytes())).build();
            // The offset of the internal processor Segment has an integer header.
            expectedInternalProcessorOffset += event.getLength() + Integer.BYTES;
            Assert.assertEquals(expectedInternalProcessorOffset, processor.add(event, Duration.ofSeconds(10)).join().intValue());
        }

        // Wait for all items to be processed.
        AssertExtensions.assertEventuallyEquals(true, () -> processedItems.get() == allEventsToProcess, 10000);
        // Ensure that no batch processed more than maxItemsProcessed.
        Assert.assertFalse(failedAssertion.get());
        // Ensure that the outstanding bytes metric has been set to 0.
        BufferView event = BufferView.builder().add(new ByteArraySegment(("event").getBytes())).build();
        Assert.assertEquals(0, processor.add(event, Duration.ofSeconds(10)).join().intValue() - (event.getLength() + Integer.BYTES));
    }

    /**
     * Verify that when a faulty handler is passed to an EventProcessor, the Segment is not truncated and the retries
     * continue indefinitely.
     *
     * @throws Exception
     */
    @Test
    public void testFaultyHandler() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(), this.executorService());
        eventProcessorService.startAsync().awaitRunning();

        AtomicLong retries = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            retries.addAndGet(1);
            throw new ArrayIndexOutOfBoundsException("Some random failure"); // Induce some exception here.
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(10);
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegment", handler, config);

        // Write an event and wait for the event to be processed.
        BufferView event = BufferView.builder().add(new ByteArraySegment(("event").getBytes())).build();
        processor.add(event, Duration.ofSeconds(10)).join();

        // Wait until the processor perform  10 retries of the same event.
        AssertExtensions.assertEventuallyEquals(true, () -> retries.get() == 10, 10000);
    }

    /**
     * Check that multiple EventProssors can work in parallel under the same {@link ContainerEventProcessor}, even in if
     * some of them are faulty.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleProcessors() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(), this.executorService());
        eventProcessorService.startAsync().awaitRunning();

        int allEventsToProcess = 100;
        AtomicLong processorResults1 = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler1 = l -> {
            processorResults1.addAndGet(l.size());
            return null;
        };
        AtomicLong processorResults2 = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler2 = l -> {
            processorResults2.addAndGet(l.size());
            return null;
        };
        AtomicLong processorResults3 = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler3 = l -> {
            Object o = null;
            o.toString(); // We should expect NPE here, so the results counter would not be incremented.
            processorResults3.addAndGet(1);
            return null;
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(10);
        ContainerEventProcessor.EventProcessor processor1 = eventProcessorService.forConsumer("testSegment1", handler1, config);
        ContainerEventProcessor.EventProcessor processor2 = eventProcessorService.forConsumer("testSegment2", handler2, config);
        ContainerEventProcessor.EventProcessor processor3 = eventProcessorService.forConsumer("testSegment3", handler3, config);

        for (int i = 0; i < allEventsToProcess; i++) {
            BufferView event = BufferView.builder().add(new ByteArraySegment(("event" + i).getBytes())).build();
            processor1.add(event, Duration.ofSeconds(10)).join();
            processor2.add(event, Duration.ofSeconds(10)).join();
            processor3.add(event, Duration.ofSeconds(10)).join();
        }

        // Wait for all items to be processed.
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults1.get() == allEventsToProcess, 10000);
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults2.get() == allEventsToProcess, 10000);
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults3.get() == 0, 10000);
    }

    /**
     * Check that an EventProcessor does not accept any new event once the maximum outstanding bytes has been reached.
     *
     * @throws Exception
     */
    @Test
    public void testEventRejectionOnMaxOutstanding() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(), this.executorService());
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(1000);
        AtomicLong processorResults = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler1 = l -> {
            processorResults.addAndGet(l.size());
            return null;
        };
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegmentMax", handler1, config);
        boolean foundMaxOutstandingLimit = false;

        // Write a lot of data with the ContainerEventProcessor not started, so we ensure we get the max outstanding exception.
        BufferView event = BufferView.builder().add(new ByteArraySegment(("This needs to be a long string to reach the limit sooner!!!").getBytes())).build();
        while (!foundMaxOutstandingLimit){
            try {
                processor.add(event, Duration.ofSeconds(10)).join();
                processorResults.decrementAndGet();
            } catch (Exception e) {
                // We have reached the max outstanding bytes for this internal Segment.
                Assert.assertTrue(e instanceof IllegalStateException);
                foundMaxOutstandingLimit = true;
            }
        }

        // Now we start the service to read internal Segments.
        eventProcessorService.startAsync().awaitRunning();

        // Wait until all the events are consumed.
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults.get() == 0, 100000);

        // Check that we cannot add empty events.
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> processor.add(BufferView.empty(), Duration.ofSeconds(10)));
    }

    private Function<String, CompletableFuture<DirectSegmentAccess>> mockSegmentSupplier() {
        return s -> CompletableFuture.completedFuture(new SegmentMock(this.executorService()));
    }

}
