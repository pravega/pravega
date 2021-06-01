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

import io.pravega.common.Exceptions;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ContainerEventProcessorTests extends ThreadPooledTestSuite {

    private static final int TIMEOUT_SUITE_MILLIS = 100000;
    private static final Duration TIMEOUT_FUTURE = Duration.ofSeconds(10);
    private static final Duration ITERATION_DELAY = Duration.ofMillis(100);
    private static final Duration CONTAINER_OPERATION_TIMEOUT = Duration.ofMillis(1000);

    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT_SUITE_MILLIS, TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    /**
     * Check that the max number of elements processed per EventProcessor iteration is respected.
     *
     * @throws Exception
     */
    @Test(timeout = TIMEOUT_SUITE_MILLIS)
    public void testContainerMaxItemsRespected() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());

        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int allEventsToProcess = 100;
        AtomicBoolean failedAssertion = new AtomicBoolean(false);
        AtomicLong processedItems = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            failedAssertion.set(failedAssertion.get() || maxItemsProcessed < l.size());
            processedItems.addAndGet(l.size());
            return CompletableFuture.completedFuture(null);
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegment", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Write all the events as fast as possible.
        int expectedInternalProcessorOffset = 0;
        for (int i = 0; i < allEventsToProcess; i++) {
            BufferView event = new ByteArraySegment(("" + i).getBytes());
            // The offset of the internal processor Segment has an integer header.
            expectedInternalProcessorOffset += event.getLength() + ContainerEventProcessorImpl.ProcessorEventSerializer.HEADER_LENGTH;
            Assert.assertEquals(expectedInternalProcessorOffset, processor.add(event, TIMEOUT_FUTURE).join().intValue());
        }

        // Wait for all items to be processed.
        AssertExtensions.assertEventuallyEquals(true, () -> processedItems.get() == allEventsToProcess, 10000);
        // Ensure that no batch processed more than maxItemsProcessed.
        Assert.assertFalse(failedAssertion.get());
        // Ensure that the outstanding bytes metric has been set to 0.
        BufferView event = new ByteArraySegment("event".getBytes());
        Assert.assertEquals(event.getLength() + ContainerEventProcessorImpl.ProcessorEventSerializer.HEADER_LENGTH,
                processor.add(event, TIMEOUT_FUTURE).join().intValue());
    }

    /**
     * Verify that when a faulty handler is passed to an EventProcessor, the Segment is not truncated and the retries
     * continue indefinitely.
     *
     * @throws Exception
     */
    @Test(timeout = TIMEOUT_SUITE_MILLIS)
    public void testFaultyHandler() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());

        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        AtomicLong retries = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            retries.addAndGet(1);
            throw new IntentionalException("Some random failure"); // Induce some exception here.
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegment", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Write an event and wait for the event to be processed.
        BufferView event = new ByteArraySegment("event".getBytes());
        processor.add(event, TIMEOUT_FUTURE).join();

        // Wait until the processor perform 10 retries of the same event.
        AssertExtensions.assertEventuallyEquals(true, () -> retries.get() == 10, 10000);
    }

    /**
     * Check that multiple EventProcessors can work in parallel under the same {@link ContainerEventProcessor}, even in if
     * some of them are faulty.
     *
     * @throws Exception
     */
    @Test(timeout = TIMEOUT_SUITE_MILLIS)
    public void testMultipleProcessors() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());

        int allEventsToProcess = 100;
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
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
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor1 = eventProcessorService.forConsumer("testSegment1", handler1, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor2 = eventProcessorService.forConsumer("testSegment2", handler2, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor3 = eventProcessorService.forConsumer("testSegment3", handler3, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        for (int i = 0; i < allEventsToProcess; i++) {
            BufferView event = new ByteArraySegment(("event" + i).getBytes());
            processor1.add(event, TIMEOUT_FUTURE).join();
            processor2.add(event, TIMEOUT_FUTURE).join();
            processor3.add(event, TIMEOUT_FUTURE).join();
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
    @Test(timeout = 30000)
    public void testEventRejectionOnMaxOutstanding() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 1000;
        int maxOutstandingBytes = 1024 * 1024;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        AtomicLong processorResults = new AtomicLong(0);
        ReusableLatch latch = new ReusableLatch();
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            Exceptions.handleInterrupted(latch::await);
            processorResults.addAndGet(l.size());
            return null;
        };
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegmentMax", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        boolean foundMaxOutstandingLimit = false;

        // Write a lot of data with the ContainerEventProcessor not started, so we ensure we get the max outstanding exception.
        BufferView event = new ByteArraySegment("This needs to be a long string to reach the limit sooner!!!".getBytes());
        while (!foundMaxOutstandingLimit) {
            try {
                processor.add(event, TIMEOUT_FUTURE).join();
                processorResults.decrementAndGet();
            } catch (Exception e) {
                // We have reached the max outstanding bytes for this internal Segment.
                Assert.assertTrue(e instanceof IllegalStateException);
                foundMaxOutstandingLimit = true;
            }
        }
        latch.release();

        // Wait until all the events are consumed.
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults.get() == 0, 100000);

        // Check that we cannot add empty events.
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> processor.add(BufferView.empty(), TIMEOUT_FUTURE));
    }

    @Test(timeout = 10000)
    public void testAppendWithFailingSegment() throws Exception {
        DirectSegmentAccess faultySegment = mock(SegmentMock.class);
        when(faultySegment.append(any(), any(), any())).thenThrow(NullPointerException.class);
        Function<String, CompletableFuture<DirectSegmentAccess>> faultySegmentSupplier = s -> CompletableFuture.completedFuture(faultySegment);
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, faultySegmentSupplier,
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        Function<List<BufferView>, CompletableFuture<Void>> doNothing = l -> null;
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegmentMax", doNothing, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Verify that the client gets the exception if there is some issue on add().
        BufferView event = new ByteArraySegment("Test".getBytes());
        AssertExtensions.assertThrows(NullPointerException.class, () -> processor.add(event, TIMEOUT_FUTURE).join());
    }

    @Test(timeout = 10000)
    public void testReadWithFailingSegment() throws Exception {
        DirectSegmentAccess faultySegment = spy(new SegmentMock(this.executorService()));
        Function<String, CompletableFuture<DirectSegmentAccess>> faultySegmentSupplier = s -> CompletableFuture.completedFuture(faultySegment);
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, faultySegmentSupplier,
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        ReusableLatch latch = new ReusableLatch();
        Function<List<BufferView>, CompletableFuture<Void>> doNothing = l -> {
            latch.release();
            return null;
        };
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegmentMax", doNothing, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Give some time for read errors to be retried and then emulate that the Segment starts working again.
        when(faultySegment.read(anyLong(), anyInt(), any(Duration.class))).thenThrow(IntentionalException.class).thenCallRealMethod();

        // This should work.
        BufferView event = new ByteArraySegment("Test".getBytes());
        processor.add(event, TIMEOUT_FUTURE).join();

        latch.await();
    }

    @Test(timeout = 30000)
    public void testEventProcessorClose() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        AtomicLong processorResults = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            processorResults.addAndGet(l.size());
            return null;
        };
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testClose", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        // Assert that the existing object in the ContainerEventProcessorImpl map is the same as the one we just instantiated.
        Assert.assertEquals(processor, eventProcessorService.forConsumer("testClose", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS));
        // Now, close the EventProcessor object, which should auto-unregister from the map.
        processor.close();
        // After that, we should see a new object being instantiated in ContainerEventProcessorImpl for the same name.
        Assert.assertNotEquals(processor, eventProcessorService.forConsumer("testClose", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS));
    }

    /**
     * This test validates that read order and no event reprocessing is preserved when we have failures reading from the
     * internal Segment.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventOrderWithRandomReadFailures() throws Exception {
        int numEvents = 1000;
        List<Integer> readEvents = new ArrayList<>();
        AtomicBoolean reprocessingFlag = new AtomicBoolean();
        Runnable hasDuplicates = () -> Assert.assertTrue(IntStream.iterate(0, v -> v + 1)
                .limit(numEvents).reduce(0, Integer::sum) == readEvents.stream().reduce(0, Integer::sum)
                && !reprocessingFlag.get());
        Consumer<DirectSegmentAccess> failOnReads = faultySegment -> when(faultySegment.read(anyLong(), anyInt(),
                any(Duration.class))).thenThrow(IntentionalException.class).thenCallRealMethod();
        executeEventOrderingTest(numEvents, readEvents, failOnReads, hasDuplicates, reprocessingFlag);
    }

    /**
     * This test shows that we could have events re-processed in the case that failures are related to Segment truncation.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventOrderWithTruncationFailures() throws Exception {
        int numEvents = 1000;
        List<Integer> readEvents = new ArrayList<>();
        AtomicBoolean reprocessingFlag = new AtomicBoolean();
        Runnable hasDuplicates = () -> Assert.assertTrue(IntStream.iterate(0, v -> v + 1)
                .limit(numEvents).reduce(0, Integer::sum) < readEvents.stream().reduce(0, Integer::sum)
                && reprocessingFlag.get());
        Consumer<DirectSegmentAccess> failOnTruncation = faultySegment -> when(faultySegment.truncate(anyLong(),
                any(Duration.class))).thenThrow(IntentionalException.class).thenCallRealMethod();
        executeEventOrderingTest(numEvents, readEvents, failOnTruncation, hasDuplicates, reprocessingFlag);
    }

    private void executeEventOrderingTest(int numEvents, List<Integer> readEvents, Consumer<DirectSegmentAccess> segmentFailure,
                                          Runnable eventReprocessingAssertion, AtomicBoolean reprocessingFlag) throws Exception {
        DirectSegmentAccess faultySegment = spy(new SegmentMock(this.executorService()));
        Function<String, CompletableFuture<DirectSegmentAccess>> faultySegmentSupplier = s -> CompletableFuture.completedFuture(faultySegment);
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, faultySegmentSupplier,
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 100;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            l.forEach(d -> {
                int event = new ByteArraySegment(d.getCopy()).getInt(0);
                // Assert that events are read in order.
                if (!readEvents.isEmpty() && !reprocessingFlag.get()) {
                    reprocessingFlag.set(event <= readEvents.get(readEvents.size() - 1));
                }
                readEvents.add(event);
            });
            return null;
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSequence", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numEvents; i++) {
                ByteArraySegment b = new ByteArraySegment(new byte[Integer.BYTES]);
                b.setInt(0, i);
                processor.add(b.slice(), TIMEOUT_FUTURE).join();
                // Induce a read failure every 10 events written.
                if (i % 10 == 0) {
                    segmentFailure.accept(faultySegment);
                }
            }
        }, executorService());
        writer.get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        // Make sure that the mock works after writer finishes.
        when(faultySegment.read(anyLong(), anyInt(), any(Duration.class))).thenCallRealMethod();

        // Wait until the last event is read.
        AssertExtensions.assertEventuallyEquals(true, () -> !readEvents.isEmpty() &&
                readEvents.get(readEvents.size() - 1) == numEvents - 1, 100000);
        // Validate the existence or not of re-processed events due to Segment failures.
        eventReprocessingAssertion.run();
    }

    private Function<String, CompletableFuture<DirectSegmentAccess>> mockSegmentSupplier() {
        return s -> CompletableFuture.completedFuture(new SegmentMock(this.executorService()));
    }
}
