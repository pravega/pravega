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
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
     * Check the most basic operation of the {@link ContainerEventProcessor} (add and process few events).
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testBasicContainerEventProcessor() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        testBasicContainerEventProcessor(eventProcessorService);
    }

    public static void testBasicContainerEventProcessor(ContainerEventProcessor containerEventProcessor) throws Exception {
        int maxItemsPerBatch = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        ReusableLatch latch = new ReusableLatch();
        final AtomicReference<String> userEvent = new AtomicReference<>("event1");
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            l.forEach(s -> Assert.assertEquals(userEvent.get().length(), s.getLength()));
            return CompletableFuture.runAsync(latch::release);
        };
        ContainerEventProcessor.EventProcessorConfig eventProcessorConfig = new ContainerEventProcessor.EventProcessorConfig(maxItemsPerBatch,
                maxOutstandingBytes, truncationDataSize);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = containerEventProcessor.forConsumer("testConsumer", handler, eventProcessorConfig)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Test adding one Event to the EventProcessor and wait for the handle to get executed and unblock this thread.
        BufferView event = new ByteArraySegment(userEvent.get().getBytes());
        processor.add(event, TIMEOUT_FUTURE).join();
        latch.await();
        latch.reset();

        // Test the same with a larger events.
        userEvent.set("longerEvent2");
        event = new ByteArraySegment(userEvent.get().getBytes());
        processor.add(event, TIMEOUT_FUTURE).join();
        processor.add(event, TIMEOUT_FUTURE).join();
        processor.add(event, TIMEOUT_FUTURE).join();
        latch.await();

        // Check that if the same EventProcessor name is used, the same object is retrieved.
        Assert.assertEquals(processor, containerEventProcessor.forConsumer("testConsumer", handler, eventProcessorConfig)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS));
    }

    /**
     * Check that the max number of elements processed per EventProcessor iteration is respected.
     *
     * @throws Exception
     */
    @Test(timeout = TIMEOUT_SUITE_MILLIS)
    public void testContainerMaxItemsPerBatchRespected() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        testContainerMaxItemsPerBatchRespected(eventProcessorService);
    }

    public static void testContainerMaxItemsPerBatchRespected(ContainerEventProcessor eventProcessorService) throws Exception {
        int maxItemsPerBatch = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int allEventsToProcess = 1000;
        int truncationDataSize = 500;
        List<Integer> processedItems = new ArrayList<>();
        Function<List<BufferView>, CompletableFuture<Void>> handler = getNumberSequenceHandler(processedItems, maxItemsPerBatch);
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsPerBatch,
                maxOutstandingBytes, truncationDataSize);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegment", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Write all the events as fast as possible.
        for (int i = 0; i < allEventsToProcess; i++) {
            BufferView event = new ByteArraySegment(ByteBuffer.allocate(Integer.BYTES).putInt(i).array());
            processor.add(event, TIMEOUT_FUTURE).join();
        }

        // Perform basic validation on this processor.
        validateProcessorResults(processor, processedItems, allEventsToProcess);
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
        testFaultyHandler(eventProcessorService);
    }

    public static void testFaultyHandler(ContainerEventProcessor eventProcessorService) throws Exception {
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        AtomicLong retries = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            retries.addAndGet(1);
            throw new IntentionalException("Some random failure"); // Induce some exception here.
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegment", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Write an event and wait for the event to be processed.
        BufferView event = new ByteArraySegment("event".getBytes());
        processor.add(event, TIMEOUT_FUTURE).join();

        // Wait until the processor perform 10 retries of the same event.
        AssertExtensions.assertEventuallyEquals(true, () -> retries.get() == 10, 10000);
    }

    public static void testMultipleProcessors(ContainerEventProcessor eventProcessorService) throws Exception {
        int allEventsToProcess = 100;
        int maxItemsPerBatch = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        List<Integer> processorResults1 = new ArrayList<>();
        Function<List<BufferView>, CompletableFuture<Void>> handler1 = getNumberSequenceHandler(processorResults1, maxItemsPerBatch);
        List<Integer> processorResults2 = new ArrayList<>();
        Function<List<BufferView>, CompletableFuture<Void>> handler2 = getNumberSequenceHandler(processorResults2, maxItemsPerBatch);
        AtomicLong processorResults3 = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler3 = l -> {
            Object o = null;
            o.toString(); // We should expect NPE here, so the results counter would not be incremented.
            processorResults3.addAndGet(1);
            return CompletableFuture.completedFuture(null);
        };
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsPerBatch,
                maxOutstandingBytes, truncationDataSize);
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
            BufferView event = new ByteArraySegment(ByteBuffer.allocate(Integer.BYTES).putInt(i).array());
            processor1.add(event, TIMEOUT_FUTURE).join();
            processor2.add(event, TIMEOUT_FUTURE).join();
            processor3.add(event, TIMEOUT_FUTURE).join();
        }

        // Wait for all items to be processed.
        validateProcessorResults(processor1, processorResults1, allEventsToProcess);
        validateProcessorResults(processor2, processorResults2, allEventsToProcess);
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults3.get() == 0, 10000);
        AssertExtensions.assertEventuallyEquals(true, () ->
                ((ContainerEventProcessorImpl.EventProcessorImpl) processor3).getOutstandingBytes() > 0, 10000);
    }

    /**
     * Test the situation in which an EventProcessor gets BufferView.Reader.OutOfBoundsException during deserialization
     * of events.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorWithSerializationError() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
            ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        testEventProcessorWithSerializationError(eventProcessorService);
    }

    public static void testEventProcessorWithSerializationError(ContainerEventProcessor containerEventProcessor) throws Exception {
        int maxItemsPerBatch = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        AtomicLong readEvents = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            readEvents.addAndGet(l.size());
            return CompletableFuture.completedFuture(null);
        };

        ContainerEventProcessor.EventProcessorConfig eventProcessorConfig = spy(new ContainerEventProcessor.EventProcessorConfig(maxItemsPerBatch,
                        maxOutstandingBytes, truncationDataSize));
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = containerEventProcessor.forConsumer("testConsumer", handler, eventProcessorConfig)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Simulate an BufferView.Reader.OutOfBoundsException within the deserializeEvents() method and then behave normally.
        when(eventProcessorConfig.getMaxItemsAtOnce()).thenThrow(new BufferView.Reader.OutOfBoundsException())
                                                      .thenThrow(new RuntimeException(new SerializationException("Intentional exception")))
                                                      .thenCallRealMethod();

        // Write an event and wait for the event to be processed.
        BufferView event = new ByteArraySegment("event".getBytes());
        processor.add(event, TIMEOUT_FUTURE).join();

        // Wait until the processor reads the event.
        AssertExtensions.assertEventuallyEquals(true, () -> readEvents.get() == 1, 10000);
    }

    /**
     * Check that an EventProcessor does not accept any new event once the maximum outstanding bytes has been reached.
     *
     * @throws Exception
     */
    @Test(timeout = TIMEOUT_SUITE_MILLIS)
    public void testEventRejectionOnMaxOutstanding() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        testEventRejectionOnMaxOutstanding(eventProcessorService);
    }

    public static void testEventRejectionOnMaxOutstanding(ContainerEventProcessor eventProcessorService) throws Exception {
        int maxItemsProcessed = 1000;
        int maxOutstandingBytes = 1024 * 1024;
        int truncationDataSize = 500;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);
        AtomicLong processorResults = new AtomicLong(0);
        ReusableLatch latch = new ReusableLatch();
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            Exceptions.handleInterrupted(latch::await);
            processorResults.addAndGet(l.size());
            return CompletableFuture.completedFuture(null);
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
                Assert.assertTrue(e instanceof ContainerEventProcessor.TooManyOutstandingBytesException);
                foundMaxOutstandingLimit = true;
            }
        }
        latch.release();

        // Wait until all the events are consumed.
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults.get() == 0, TIMEOUT_SUITE_MILLIS);
        // Check that we cannot add empty events.
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> processor.add(BufferView.empty(), TIMEOUT_FUTURE));
    }

    /**
     * Check the behavior of the EventProcessor when there are failures when adding events to the internal Segment.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testAppendWithFailingSegment() throws Exception {
        DirectSegmentAccess faultySegment = mock(SegmentMock.class);
        when(faultySegment.append(any(), any(), any())).thenThrow(NullPointerException.class);
        SegmentMetadata mockMetadata = mock(SegmentMetadata.class);
        when(mockMetadata.getLength()).thenReturn(0L);
        when(faultySegment.getInfo()).thenReturn(mockMetadata);
        Function<String, CompletableFuture<DirectSegmentAccess>> faultySegmentSupplier = s -> CompletableFuture.completedFuture(faultySegment);
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, faultySegmentSupplier,
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);
        Function<List<BufferView>, CompletableFuture<Void>> doNothing = l -> null;
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegmentMax", doNothing, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Verify that the client gets the exception if there is some issue on add().
        BufferView event = new ByteArraySegment("Test".getBytes());
        AssertExtensions.assertThrows(NullPointerException.class, () -> processor.add(event, TIMEOUT_FUTURE).join());
    }

    /**
     * Test the behavior of the EventProcessor when internal Segment reads fail.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testReadWithFailingSegment() throws Exception {
        DirectSegmentAccess faultySegment = spy(new SegmentMock(this.executorService()));
        Function<String, CompletableFuture<DirectSegmentAccess>> faultySegmentSupplier = s -> CompletableFuture.completedFuture(faultySegment);
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, faultySegmentSupplier,
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);
        ReusableLatch latch = new ReusableLatch();
        Function<List<BufferView>, CompletableFuture<Void>> doNothing = l -> {
            latch.release();
            return CompletableFuture.completedFuture(null);
        };

        // Make the internal Segment of the processor to fail upon a read.
        when(faultySegment.read(anyLong(), anyInt(), any(Duration.class))).thenThrow(IntentionalException.class).thenCallRealMethod();

        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSegmentMax", doNothing, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Write an event to make sure that the processor is running and await for it to be processed.
        BufferView event = new ByteArraySegment("Test".getBytes());
        processor.add(event, TIMEOUT_FUTURE).join();
        latch.await();
    }

    /**
     * Test the behavior of the EventProcessor when calling concurrently to forConsumer() and forDurableQueue() methods.
     *
     * @throws Exception
     */
    @Test(timeout = 30000)
    public void testConcurrentForConsumerCall() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);

        // Try to instantiate the same EventProcessor concurrently.
        CompletableFuture<ContainerEventProcessor.EventProcessor> ep1 = eventProcessorService.forConsumer("testConcurrentForConsumer", l -> null, config);
        CompletableFuture<ContainerEventProcessor.EventProcessor> ep2 = eventProcessorService.forConsumer("testConcurrentForConsumer", l -> null, config);
        CompletableFuture<ContainerEventProcessor.EventProcessor> ep3 = eventProcessorService.forConsumer("testConcurrentForConsumer", l -> null, config);

        // Wait for all these calls to complete.
        CompletableFuture.allOf(ep1, ep2, ep3).get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // When instantiating an EventProcessor, the service should be started.
        Assert.assertTrue(((ContainerEventProcessorImpl.EventProcessorImpl) ep1.join()).isRunning());

        // Ensure that all EventProcessors are the same object.
        Assert.assertTrue(ep1.join() == ep2.join());
        Assert.assertTrue(ep2.join() == ep3.join());

        // Try to instantiate the same EventProcessor concurrently.
        ep1 = eventProcessorService.forDurableQueue("testConcurrentForDurableQueue");
        ep2 = eventProcessorService.forDurableQueue("testConcurrentForDurableQueue");
        ep3 = eventProcessorService.forDurableQueue("testConcurrentForDurableQueue");

        // When instantiating an EventProcessor as a durable queue, the service should not be started (we just do adds).
        Assert.assertFalse(((ContainerEventProcessorImpl.EventProcessorImpl) ep1.join()).isRunning());

        // Wait for all these calls to complete.
        CompletableFuture.allOf(ep1, ep2, ep3).get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Ensure that all EventProcessors are the same object.
        Assert.assertTrue(ep1.join() == ep2.join());
        Assert.assertTrue(ep2.join() == ep3.join());
    }

    /**
     * Check that if the creation of the EventProcessor fails, the future is completed exceptionally.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testInitializationException() throws Exception {
        AtomicBoolean induceFailure = new AtomicBoolean(true);
        Function<String, CompletableFuture<DirectSegmentAccess>> failingSegmentSupplier = s ->
                induceFailure.getAndSet(!induceFailure.get()) ? CompletableFuture.failedFuture(new IntentionalException()) :
                CompletableFuture.completedFuture(new SegmentMock(this.executorService()));
        @Cleanup
        ContainerEventProcessorImpl eventProcessorService = new ContainerEventProcessorImpl(0, failingSegmentSupplier,
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);

        // Verify that if the creation of the EventProcessor takes too long, the future completes exceptionally.
        AssertExtensions.assertFutureThrows("Expected future exceptionally complete with IntentionalException",
                eventProcessorService.forConsumer("testExceptionForConsumer", l -> null, config),
                ex -> ex instanceof IntentionalException);

        // If the call has failed, the future for that EventProcessor should have been removed from the map.
        Assert.assertNull(eventProcessorService.getEventProcessorMap().get("testExceptionForConsumer"));
        // The next call is expected to succeed, so the future should be in the map when this call completes.
        Assert.assertNotNull(eventProcessorService.forConsumer("testExceptionForConsumer", l -> null, config).join());
        Assert.assertNotNull(eventProcessorService.getEventProcessorMap().get("testExceptionForConsumer"));

        AssertExtensions.assertFutureThrows("Expected future exceptionally complete with IntentionalException",
                eventProcessorService.forDurableQueue("testExceptionForDurableQueue"),
                ex -> ex instanceof IntentionalException);
        Assert.assertNull(eventProcessorService.getEventProcessorMap().get("testExceptionForDurableQueue"));
        Assert.assertNotNull(eventProcessorService.forDurableQueue("testExceptionForDurableQueue").join());
        Assert.assertNotNull(eventProcessorService.getEventProcessorMap().get("testExceptionForDurableQueue"));
    }

    /**
     * Test closing the EventProcessor.
     *
     * @throws Exception
     */
    @Test(timeout = 30000)
    public void testEventProcessorClose() throws Exception {
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);
        AtomicLong processorResults = new AtomicLong(0);
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            processorResults.addAndGet(l.size());
            return CompletableFuture.completedFuture(null);
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
     * Checks that closing multiple EventProcessors does not lead to {@link java.util.ConcurrentModificationException}.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorCloseManyProcessors() throws Exception {
        @Cleanup
        ContainerEventProcessorImpl eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        for (int i = 0; i < 1000; i++) {
            ContainerEventProcessorImpl.EventProcessorImpl processor = mock(ContainerEventProcessorImpl.EventProcessorImpl.class);
            final String key = String.valueOf(i);
            doAnswer(a -> eventProcessorService.getEventProcessorMap().remove(key)).when(processor).close();
            eventProcessorService.getEventProcessorMap().put(key, CompletableFuture.completedFuture(processor));
        }
        eventProcessorService.close();
    }

    /**
     * This test validates that read order and no event reprocessing is preserved when we have failures reading from the
     * internal Segment.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorEventOrderWithRandomReadFailures() throws Exception {
        int numEvents = 1000;
        List<Integer> readEvents = new ArrayList<>();
        Consumer<DirectSegmentAccess> failOnReads = faultySegment -> when(faultySegment.read(anyLong(), anyInt(),
                any(Duration.class))).thenThrow(IntentionalException.class).thenCallRealMethod();
        executeEventOrderingTest(numEvents, readEvents, failOnReads);
        Assert.assertEquals(readEvents.size(), numEvents);
    }

    /**
     * This test shows that we could have events re-processed in the case that failures are related to Segment truncation.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorEventOrderWithTruncationFailures() throws Exception {
        int numEvents = 1000;
        List<Integer> readEvents = new ArrayList<>();
        Consumer<DirectSegmentAccess> failOnTruncation = faultySegment -> when(faultySegment.truncate(anyLong(),
                any(Duration.class))).thenThrow(IntentionalException.class).thenCallRealMethod();
        executeEventOrderingTest(numEvents, readEvents, failOnTruncation);
        Assert.assertTrue(readEvents.size() > numEvents);
    }

    /**
     * Verifies that we truncate the Segment the expected number of times based on the config.
     *
     * @throws Exception
     */
    @Test(timeout = TIMEOUT_SUITE_MILLIS)
    public void testTruncationSizeRespected() throws Exception {
        MockSegmentSupplier mockSegmentSupplier = new MockSegmentSupplier();
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, mockSegmentSupplier.mockSegmentSupplier(),
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int allEventsToProcess = 10;
        int maxItemsPerBatch = 10;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 20;
        List<Integer> processorResults1 = new ArrayList<>();
        Function<List<BufferView>, CompletableFuture<Void>> handler1 = getNumberSequenceHandler(processorResults1, maxItemsPerBatch);
        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsPerBatch,
                maxOutstandingBytes, truncationDataSize);
        @Cleanup
        ContainerEventProcessorImpl.EventProcessorImpl processor1 = (ContainerEventProcessorImpl.EventProcessorImpl) eventProcessorService.forConsumer("testSegment1", handler1, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        for (int i = 0; i < allEventsToProcess; i++) {
            BufferView event = new ByteArraySegment(ByteBuffer.allocate(Integer.BYTES).putInt(i).array());
            processor1.add(event, TIMEOUT_FUTURE).join();
            // After adding one event, wait for it to be processed.
            AssertExtensions.assertEventuallyEquals(true, () -> processor1.getOutstandingBytes() == 0, 10000);
        }
        // Wait for all items to be processed.
        validateProcessorResults(processor1, processorResults1, allEventsToProcess);
        // Each Integer event write is 13 bytes in size after being serialized.
        verify(mockSegmentSupplier.segmentMock, times((13 * allEventsToProcess) / truncationDataSize - 1)).truncate(anyLong(), any());
    }

    private void executeEventOrderingTest(int numEvents, List<Integer> readEvents, Consumer<DirectSegmentAccess> segmentFailure) throws Exception {
        DirectSegmentAccess faultySegment = spy(new SegmentMock(this.executorService()));
        Function<String, CompletableFuture<DirectSegmentAccess>> faultySegmentSupplier = s -> CompletableFuture.completedFuture(faultySegment);
        @Cleanup
        ContainerEventProcessor eventProcessorService = new ContainerEventProcessorImpl(0, faultySegmentSupplier,
                ITERATION_DELAY, CONTAINER_OPERATION_TIMEOUT, this.executorService());
        int maxItemsProcessed = 100;
        int maxOutstandingBytes = 4 * 1024 * 1024;
        int truncationDataSize = 500;
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            l.forEach(d -> readEvents.add(new ByteArraySegment(d.getCopy()).getInt(0)));
            return CompletableFuture.completedFuture(null);
        };

        // Induce some desired failures.
        segmentFailure.accept(faultySegment);

        ContainerEventProcessor.EventProcessorConfig config = new ContainerEventProcessor.EventProcessorConfig(maxItemsProcessed,
                maxOutstandingBytes, truncationDataSize);
        @Cleanup
        ContainerEventProcessor.EventProcessor processor = eventProcessorService.forConsumer("testSequence", handler, config)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // Write some data.
        for (int i = 0; i < numEvents; i++) {
            ByteArraySegment b = new ByteArraySegment(new byte[Integer.BYTES]);
            b.setInt(0, i);
            processor.add(b.slice(), TIMEOUT_FUTURE).join();
        }

        // Wait until the last event is read.
        AssertExtensions.assertEventuallyEquals(true, () -> !readEvents.isEmpty() &&
                readEvents.get(readEvents.size() - 1) == numEvents - 1, 10000);
    }

    /**
     * Simple handler function that stores all the read integers into a list.
     *
     * @param processorResults Sequence of numbers read by the EventProcessor.
     * @return Function that gets as input a list of {@link BufferView} (each element is a single number) and returns
     * a void {@link CompletableFuture}.
     */
    private static Function<List<BufferView>, CompletableFuture<Void>> getNumberSequenceHandler(List<Integer> processorResults,
                                                                                         int maxItemsPerBatch) {
        return l -> {
            // If maxItemsPerBatch is violated, induce a wrong processor result and throw an assertion error.
            if (maxItemsPerBatch < l.size()) {
                processorResults.add(Integer.MAX_VALUE);
                Assert.fail("maxItemsPerBatch has not been respected in EventProcessor");
            }
            l.forEach(b -> {
                try {
                    processorResults.add(ByteBuffer.wrap(b.getReader().readNBytes(Integer.BYTES)).getInt());
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
            return CompletableFuture.completedFuture(null);
        };
    }

    /**
     * Validates that the events processed by an {@link ContainerEventProcessor.EventProcessor} are correct (assuming a
     * sequence of numbers) and checks that the outstanding bytes is equal to 0.
     */
    private static void validateProcessorResults(ContainerEventProcessor.EventProcessor processor, List<Integer> processedItems,
                                          int expectedItemNumber) throws Exception {
        // Wait for all items to be processed.
        AssertExtensions.assertEventuallyEquals(true, () -> processedItems.size() == expectedItemNumber, 10000);
        Assert.assertArrayEquals(processedItems.toArray(), IntStream.iterate(0, v -> v + 1).limit(expectedItemNumber).boxed().toArray());
        // Ensure that the outstanding bytes metric has been set to 0.
        AssertExtensions.assertEventuallyEquals(true, () ->
                ((ContainerEventProcessorImpl.EventProcessorImpl) processor).getOutstandingBytes() == 0, 10000);
    }

    private Function<String, CompletableFuture<DirectSegmentAccess>> mockSegmentSupplier() {
        MockSegmentSupplier mockSegmentSupplier = new MockSegmentSupplier();
        return s -> mockSegmentSupplier.mockSegmentSupplier().apply(s);
    }

    class MockSegmentSupplier {
        SegmentMock segmentMock = spy(new SegmentMock(executorService()));
        private Function<String, CompletableFuture<DirectSegmentAccess>> mockSegmentSupplier() {
            return s -> CompletableFuture.completedFuture(this.segmentMock);
        }
    }
}
