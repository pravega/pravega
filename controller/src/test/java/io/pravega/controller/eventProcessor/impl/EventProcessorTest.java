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
package io.pravega.controller.eventProcessor.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderSegmentDistribution;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.EventReadImpl;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.EventSerializer;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Event processor test.
 */
@Slf4j
public class EventProcessorTest {
    private static final String SCOPE = "scope";
    private static final String STREAM_NAME = "stream";
    private static final String READER_GROUP = "readerGroup";
    private static final String READER_ID = "reader-1";
    private static final String PROCESS = "process";

    @Data
    @AllArgsConstructor
    public static class TestEvent implements ControllerEvent {
        int number;

        @Override
        public String getKey() {
            return null;
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public static class TestEventProcessor extends EventProcessor<TestEvent> {
        long sum;
        final boolean throwErrors;

        public TestEventProcessor(Boolean throwErrors) {
            Preconditions.checkNotNull(throwErrors);
            sum = 0;
            this.throwErrors = throwErrors;
        }

        @Override
        protected void process(TestEvent event, Position position) {
            if (event.getNumber() < 0) {
                throw new RuntimeException();
            } else {
                int val = event.getNumber();
                sum += val;
                if (throwErrors && val % 2 == 0) {
                    throw new IllegalArgumentException();
                }
            }
        }
    }

    public static class StartFailingEventProcessor extends EventProcessor<TestEvent> {
        long sum;

        @Override
        protected void beforeStart() {
            throw new RuntimeException("startup failed");
        }

        @Override
        protected void process(TestEvent event, Position position) {
            sum += event.getNumber();
        }
    }

    public static class StartWritingEventProcessor extends TestEventProcessor {
        private final int[] testEvents;

        public StartWritingEventProcessor(Boolean throwErrors, int[] testEvents) {
            super(throwErrors);
            this.testEvents = testEvents.clone();
        }

        @Override
        protected void beforeStart() {
            for (int i : testEvents) {
                this.getSelfWriter().write(new TestEvent(i));
            }
        }
    }

    public static class RestartFailingEventProcessor extends TestEventProcessor {

        public RestartFailingEventProcessor(Boolean throwErrors) {
            super(throwErrors);
        }

        @Override
        protected void beforeRestart(Throwable t, TestEvent event) {
            throw new RuntimeException("startup failed");
        }
    }

    private static class SequenceAnswer<T> implements Answer<T> {

        private Iterator<T> resultIterator;

        // null is returned once the iterator is exhausted

        public SequenceAnswer(List<T> results) {
            this.resultIterator = results.iterator();
        }

        @Override
        public T answer(InvocationOnMock invocation) throws Throwable {
            if (resultIterator.hasNext()) {
                return resultIterator.next();
            } else {
                return null;
            }
        }
    }

    private static class MockEventRead<T> implements EventRead<T> {

        final T value;
        final Position position;

        MockEventRead(long position, T value) {
            this.value = value;
            Segment segment = new Segment(SCOPE, STREAM_NAME, 0);
            this.position = new PositionImpl(Collections.singletonMap(new SegmentWithRange(segment, 0, 1), position));
        }

        @Override
        public T getEvent() {
            return value;
        }

        @Override
        public Position getPosition() {
            return position;
        }

        @Override
        public EventPointer getEventPointer() {
            return null;
        }

        @Override
        public boolean isCheckpoint() {
            return false;
        }

        @Override
        public String getCheckpointName() {
            return null;
        }

        @Override
        public boolean isReadCompleted() {
            return false;
        }

    }

    private ScheduledExecutorService executor;
    
    @Before
    public void setUp() {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
    }
    
    @After
    public void tearDown() {
        executor.shutdownNow();
    }
    
    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void testEventProcessorCell() throws CheckpointStoreException, ReinitializationRequiredException {
        CheckpointStore checkpointStore = CheckpointStoreFactory.createInMemoryStore();

        CheckpointConfig.CheckpointPeriod period =
                CheckpointConfig.CheckpointPeriod.builder()
                        .numEvents(1)
                        .numSeconds(1)
                        .build();

        CheckpointConfig checkpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(period)
                        .build();

        EventProcessorGroupConfig config =
                EventProcessorGroupConfigImpl.builder()
                        .eventProcessorCount(1)
                        .readerGroupName(READER_GROUP)
                        .streamName(STREAM_NAME)
                        .checkpointConfig(checkpointConfig)
                        .build();

        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int expectedSum = input.length * (input.length + 1) / 2;

        List<MockEventRead<TestEvent>> inputEvents = new ArrayList<>(input.length);
        for (int i = 0; i < input.length; i++) {
            inputEvents.add(new MockEventRead<>(i, new TestEvent(input[i])));
        }
        inputEvents.add(new MockEventRead<>(input.length, new TestEvent(-1)));

        EventProcessorSystem system = Mockito.mock(EventProcessorSystem.class);
        Mockito.when(system.getProcess()).thenReturn(PROCESS);

        EventStreamReader<TestEvent> reader = Mockito.mock(EventStreamReader.class);

        checkpointStore.addReaderGroup(PROCESS, READER_GROUP);

        // Test case 1. Actor does not throw any exception during normal operation.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(false))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, eventProcessorConfig, reader, READER_ID, checkpointStore, expectedSum);

        // Test case 2. Actor throws an error during normal operation, and Directive is to Resume on error.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(true))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) ->
                        (e instanceof IllegalArgumentException) ? ExceptionHandler.Directive.Resume : ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, eventProcessorConfig, reader, READER_ID, checkpointStore, expectedSum);

        // Test case 3. Actor throws an error during normal operation, and Directive is to Restart on error.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(true))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) ->
                        (e instanceof IllegalArgumentException) ? ExceptionHandler.Directive.Restart : ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, eventProcessorConfig, reader, READER_ID, checkpointStore, 0);

        // Test case 3. Actor throws an error during normal operation, and Directive is to Restart on error.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new RestartFailingEventProcessor(true))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) ->
                        (e instanceof IllegalArgumentException) ? ExceptionHandler.Directive.Restart : ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, eventProcessorConfig, reader, READER_ID, checkpointStore, 3);

        // Test case 5. startup fails for an event processor
        eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(StartFailingEventProcessor::new)
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        checkpointStore.addReader(PROCESS, READER_GROUP, READER_ID);
        EventProcessorCell<TestEvent> cell = new EventProcessorCell<>(eventProcessorConfig, reader,
                new EventStreamWriterMock<>(), system.getProcess(), READER_ID, 0, checkpointStore);
        cell.startAsync();
        cell.awaitTerminated();
        checkpointStore.removeReader(PROCESS, READER_GROUP, READER_ID);

        // Test case 6. Close event processor cell when reader/checkpoint store throw exceptions.
        Mockito.doThrow(new IllegalArgumentException("Failing reader")).when(reader).closeAt(any());
        checkpointStore = spy(checkpointStore);
        Mockito.doThrow(new IllegalArgumentException("Failing checkpointStore"))
               .when(checkpointStore)
               .removeReader(anyString(), anyString(), anyString());
        eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(StartFailingEventProcessor::new)
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        checkpointStore.addReader(PROCESS, READER_GROUP, READER_ID);
        cell = new EventProcessorCell<>(eventProcessorConfig, reader, new EventStreamWriterMock<>(), system.getProcess(),
                READER_ID, 0, checkpointStore);
        cell.startAsync();
        cell.awaitTerminated();
    }

    @Test(timeout = 10000)
    public void testEventProcessorCellShutdown() throws InterruptedException {
        CheckpointStore checkpointStore = CheckpointStoreFactory.createInMemoryStore();

        CheckpointConfig.CheckpointPeriod period =
                CheckpointConfig.CheckpointPeriod.builder()
                        .numEvents(1)
                        .numSeconds(1)
                        .build();

        CheckpointConfig checkpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(period)
                        .build();

        EventProcessorGroupConfig config =
                EventProcessorGroupConfigImpl.builder()
                        .eventProcessorCount(1)
                        .readerGroupName(READER_GROUP)
                        .streamName(STREAM_NAME)
                        .checkpointConfig(checkpointConfig)
                        .build();

        EventProcessorSystem system = Mockito.mock(EventProcessorSystem.class);
        Mockito.when(system.getProcess()).thenReturn(PROCESS);

        EventStreamReader<TestEvent> reader = Mockito.mock(EventStreamReader.class);

        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(false))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        EventProcessorCell<TestEvent> cell = new EventProcessorCell<>(eventProcessorConfig, reader,
                new EventStreamWriterMock<>(), system.getProcess(), READER_ID, 0, checkpointStore);

        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer((Answer) invocation -> {
            cell.stopAsync(true);
            //Below sleep is to simulate the scenario where infinite retries happen to connect to segmentstore when SS is down
            Thread.sleep(60000);
            return null;
        });

        cell.startAsync();
        cell.awaitStartupComplete();

        cell.awaitTerminated();
    }

    @Test(timeout = 10000)
    public void testEventProcessorWriter() throws ReinitializationRequiredException, CheckpointStoreException {
        int initialCount = 1;
        String systemName = "testSystem";
        String readerGroupName = "testReaderGroup";
        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        EventStreamWriterMock<TestEvent> writer = new EventStreamWriterMock<>();

        CheckpointStore checkpointStore = CheckpointStoreFactory.createInMemoryStore();
        checkpointStore.addReaderGroup(PROCESS, readerGroupName);

        CheckpointConfig checkpointConfig = CheckpointConfig.builder().type(CheckpointConfig.Type.None).build();

        EventProcessorGroupConfig config = EventProcessorGroupConfigImpl.builder()
                .eventProcessorCount(1)
                .readerGroupName(READER_GROUP)
                .streamName(STREAM_NAME)
                .checkpointConfig(checkpointConfig)
                .build();

        createEventProcessorGroupConfig(initialCount);

        EventProcessorSystemImpl system = createMockSystem(systemName, PROCESS, SCOPE, createEventReaders(1, input),
                writer, readerGroupName);

        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new StartWritingEventProcessor(false, input))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();

        // Create EventProcessorGroup.
        @Cleanup
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>)
                system.createEventProcessorGroup(eventProcessorConfig, checkpointStore, executor);

        // Await until it is ready.
        group.awaitRunning();

        // By now, the events have been written to the Mock EventStreamWriter.
        Integer[] writerList = writer.getEventList().stream()
                .map(TestEvent::getNumber)
                .collect(Collectors.toList()).toArray(new Integer[input.length]);

        // Validate that events are correctly written.
        Assert.assertArrayEquals(input, ArrayUtils.toPrimitive(writerList));
    }

    @Test(timeout = 10000)
    public void testInitialize() throws ReinitializationRequiredException, CheckpointStoreException {
        String systemName = "testSystem";
        String readerGroupName = "testReaderGroup";
        EventStreamWriterMock<TestEvent> writer = new EventStreamWriterMock<>();
        int[] input = {1, 2, 3, 4, 5};

        CheckpointStore checkpointStore = CheckpointStoreFactory.createInMemoryStore();
        checkpointStore.addReaderGroup(PROCESS, readerGroupName);

        CheckpointConfig checkpointConfig = CheckpointConfig.builder().type(CheckpointConfig.Type.None).build();

        EventProcessorGroupConfig config = EventProcessorGroupConfigImpl.builder()
                .eventProcessorCount(3)
                .readerGroupName(READER_GROUP)
                .streamName(STREAM_NAME)
                .checkpointConfig(checkpointConfig)
                .build();

        createEventProcessorGroupConfig(3);

        EventProcessorSystemImpl system = createMockSystem(systemName, PROCESS, SCOPE, createEventReaders(3, input),
                writer, readerGroupName);

        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new StartWritingEventProcessor(false, input))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();

        // Create EventProcessorGroup.
        @Cleanup
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>)
                system.createEventProcessorGroup(eventProcessorConfig, checkpointStore, executor);

        // test idempotent initialize
        group.initialize();
        group.initialize();

        // Await until it is ready.
        group.awaitRunning();

        group.initialize();

        assertEquals(3, group.getEventProcessorMap().values().size());
    }

    @Test(timeout = 10000)
    public void testFailingEventProcessorInGroup() throws ReinitializationRequiredException, CheckpointStoreException {
        String systemName = "testSystem";
        String readerGroupName = "testReaderGroup";
        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        CheckpointStore checkpointStore = CheckpointStoreFactory.createInMemoryStore();

        EventProcessorGroupConfig config = createEventProcessorGroupConfig(1);

        EventProcessorSystemImpl system = createMockSystem(systemName, PROCESS, SCOPE, createEventReaders(1, input),
                new EventStreamWriterMock<>(), readerGroupName);

        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(StartFailingEventProcessor::new)
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();

        // Create EventProcessorGroup.
        @Cleanup
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>) system
                .createEventProcessorGroup(eventProcessorConfig, checkpointStore, executor);

        // awaitRunning should succeed.
        group.awaitRunning();
        Assert.assertTrue(true);
    }

    @Test(timeout = 10000)
    public void testEventProcessorGroup() throws CheckpointStoreException, ReinitializationRequiredException {
        int count = 4;
        int initialCount = count / 2;
        String systemName = "testSystem";
        String readerGroupName = "testReaderGroup";
        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int expectedSum = input.length * (input.length + 1) / 2;

        CheckpointStore checkpointStore = CheckpointStoreFactory.createInMemoryStore();
        checkpointStore.addReaderGroup(PROCESS, readerGroupName);

        EventProcessorGroupConfig config = createEventProcessorGroupConfig(initialCount);

        EventProcessorSystemImpl system = createMockSystem(systemName, PROCESS, SCOPE, createEventReaders(count, input),
                new EventStreamWriterMock<>(), readerGroupName);

        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(false))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();

        // Create EventProcessorGroup.
        @Cleanup
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>) system.createEventProcessorGroup(eventProcessorConfig,
                    checkpointStore, executor);
        group.awaitRunning();

        // Add a few event processors to the group.
        group.changeEventProcessorCount(count - initialCount);

        long actualSum = 0;
        for (EventProcessorCell<TestEvent> cell : group.getEventProcessorMap().values()) {
            cell.awaitTerminated();
            TestEventProcessor actor = (TestEventProcessor) cell.getActor();
            actualSum += actor.sum;
        }
        assertEquals(count * expectedSum, actualSum);

        // Stop the group, and await its termination.
        group.stopAsync();
        group.awaitTerminated();
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void testEventProcessorGroupRebalance() throws CheckpointStoreException, ReinitializationRequiredException {
        String systemName = "rebalance";
        String readerGroupName = "rebalance";

        CheckpointStore checkpointStore = spy(CheckpointStoreFactory.createInMemoryStore());
        checkpointStore.addReaderGroup(PROCESS, readerGroupName);

        EventProcessorGroupConfig config = createEventProcessorGroupConfig(2);
        
        EventStreamClientFactory clientFactory = Mockito.mock(EventStreamClientFactory.class);

        EventStreamReader<TestEvent> reader = Mockito.mock(EventStreamReader.class);
        Mockito.when(reader.readNextEvent(anyLong())).thenReturn(Mockito.mock(EventReadImpl.class));

        Mockito.when(clientFactory.createReader(anyString(), anyString(), any(), any()))
               .thenAnswer(x -> reader);

        Mockito.when(clientFactory.<String>createEventWriter(anyString(), any(), any())).thenReturn(new EventStreamWriterMock<>());

        ReaderGroup readerGroup = Mockito.mock(ReaderGroup.class);
        Mockito.when(readerGroup.getGroupName()).thenReturn(readerGroupName);

        ReaderGroupManager readerGroupManager = Mockito.mock(ReaderGroupManager.class);
        Mockito.when(readerGroupManager.getReaderGroup(anyString())).then(invocation -> readerGroup);

        EventProcessorSystemImpl system = new EventProcessorSystemImpl(systemName, PROCESS, SCOPE, clientFactory, readerGroupManager);

        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(false))
                .serializer(new EventSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .minRebalanceIntervalMillis(0L)
                .build();

        // Create EventProcessorGroup.
        @Cleanup
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>) system.createEventProcessorGroup(eventProcessorConfig,
                    checkpointStore, executor);
        group.awaitRunning();

        ConcurrentHashMap<String, EventProcessorCell<TestEvent>> eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());

        List<String> readerIds = eventProcessorMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());

        // region case 1: even distribution - 2 readers with 2 segments each
        HashMap<String, Integer> distribution = new HashMap<>();
        distribution.put(readerIds.get(0), 2);
        distribution.put(readerIds.get(1), 2);
        
        ReaderSegmentDistribution readerSegmentDistribution = ReaderSegmentDistribution
                .builder().readerSegmentDistribution(distribution).unassignedSegments(0).build();
        Mockito.when(readerGroup.getReaderSegmentDistribution()).thenReturn(readerSegmentDistribution);

        // call rebalance. no new readers should be added or existing reader removed.
        group.rebalance();

        eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());
        // the original readers should not have been replaced
        assertTrue(eventProcessorMap.containsKey(readerIds.get(0)));
        assertTrue(eventProcessorMap.containsKey(readerIds.get(1)));

        // endregion
        
        // region case 2: two external readers with 0 segment assignment and 2 overloaded readers in the 
        // readergroup. unassigned = 0
        String reader2 = "reader2";
        String reader3 = "reader3";

        distribution = new HashMap<>();
        distribution.put(readerIds.get(0), 2);
        distribution.put(readerIds.get(1), 2);
        distribution.put(reader2, 0);
        distribution.put(reader3, 0);
        
        readerSegmentDistribution = ReaderSegmentDistribution
                .builder().readerSegmentDistribution(distribution).unassignedSegments(0).build();
        Mockito.when(readerGroup.getReaderSegmentDistribution()).thenReturn(readerSegmentDistribution);

        // call rebalance. this should replace existing overloaded readers
        group.rebalance();
        
        eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());
        assertFalse(eventProcessorMap.containsKey(readerIds.get(0)));
        assertFalse(eventProcessorMap.containsKey(readerIds.get(1)));

        Enumeration<String> keys = eventProcessorMap.keys();
        String firstReplacement = keys.nextElement();
        String secondReplacement = keys.nextElement();
        
        // verify that checkpointstore.addreader is called twice
        verify(checkpointStore, times(2)).addReader(any(), any(), eq(firstReplacement));
        verify(checkpointStore, times(2)).addReader(any(), any(), eq(secondReplacement));
        
        // update the readers in the readergroup
        readerIds = eventProcessorMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());

        // endregion
        
        // region case 3: even distribution among 4 readers
        distribution = new HashMap<>();
        distribution.put(readerIds.get(0), 1);
        distribution.put(readerIds.get(1), 1);
        distribution.put(reader2, 1);
        distribution.put(reader3, 1);

        readerSegmentDistribution = ReaderSegmentDistribution
                .builder().readerSegmentDistribution(distribution).unassignedSegments(0).build();
        Mockito.when(readerGroup.getReaderSegmentDistribution()).thenReturn(readerSegmentDistribution);

        // call rebalance. nothing should happen
        group.rebalance();
        
        // no change to the group
        eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());
        assertTrue(eventProcessorMap.containsKey(readerIds.get(0)));
        assertTrue(eventProcessorMap.containsKey(readerIds.get(1)));

        // endregion
        
        // region case 4: with 1 overloaded reader and 2 unassigned segments
        distribution = new HashMap<>();
        distribution.put(readerIds.get(0), 2);
        distribution.put(readerIds.get(1), 0);
        distribution.put(reader2, 0);
        distribution.put(reader3, 0);

        readerSegmentDistribution = ReaderSegmentDistribution
                .builder().readerSegmentDistribution(distribution).unassignedSegments(2).build();
        Mockito.when(readerGroup.getReaderSegmentDistribution()).thenReturn(readerSegmentDistribution);

        // call rebalance. overloaded reader should be replaced
        group.rebalance();

        // reader0 should have been replaced. 
        eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());
        assertFalse(eventProcessorMap.containsKey(readerIds.get(0)));
        assertTrue(eventProcessorMap.containsKey(readerIds.get(1)));

        // endregion

        readerIds = eventProcessorMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        distribution = new HashMap<>();
        distribution.put(readerIds.get(0), 2);
        distribution.put(readerIds.get(1), 0);
        distribution.put(reader2, 0);
        distribution.put(reader3, 0);

        readerSegmentDistribution = ReaderSegmentDistribution
                .builder().readerSegmentDistribution(distribution).unassignedSegments(2).build();

        // case 5: region failure cases
        doThrow(new RuntimeException("reader group throws")).when(readerGroup).getReaderSegmentDistribution();

        // exception should be handled and there should be no state change in event processor
        group.rebalance();

        eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());
        assertTrue(eventProcessorMap.containsKey(readerIds.get(0)));
        assertTrue(eventProcessorMap.containsKey(readerIds.get(1)));

        // now reset the distribution
        doReturn(readerSegmentDistribution).when(readerGroup).getReaderSegmentDistribution();
        // throw from checkpoint store
        doThrow(new CheckpointStoreException("checkpoint store exception")).when(checkpointStore).addReader(anyString(), anyString(), anyString());
        
        // exception should have been thrown and handled
        group.rebalance();

        eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());
        assertTrue(eventProcessorMap.containsKey(readerIds.get(0)));
        assertTrue(eventProcessorMap.containsKey(readerIds.get(1)));
        // endregion
        
        // Stop the group, and await its termmination.
        group.stopAsync();
        group.awaitTerminated();
        
        // call rebalance after shutdown such that replace cell is called - this should throw precondition failed exception
        readerIds = eventProcessorMap.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());

        distribution = new HashMap<>();
        distribution.put(readerIds.get(0), 2);
        distribution.put(readerIds.get(1), 2);
        distribution.put(reader2, 0);
        distribution.put(reader3, 0);

        readerSegmentDistribution = ReaderSegmentDistribution
                .builder().readerSegmentDistribution(distribution).unassignedSegments(0).build();
        Mockito.when(readerGroup.getReaderSegmentDistribution()).thenReturn(readerSegmentDistribution);

        // calling rebalance on terminated group will result in Precondition failure with exception logged and ignored 
        // and no rebalance occurring.

        // exception should have been thrown and handled
        group.rebalance();

        eventProcessorMap = group.getEventProcessorMap();
        assertEquals(2, eventProcessorMap.size());
        assertTrue(eventProcessorMap.containsKey(readerIds.get(0)));
        assertTrue(eventProcessorMap.containsKey(readerIds.get(1)));
        // endregion
    }

    private EventProcessorGroupConfig createEventProcessorGroupConfig(int count) {
        CheckpointConfig.CheckpointPeriod period =
                CheckpointConfig.CheckpointPeriod.builder()
                        .numEvents(1)
                        .numSeconds(1)
                        .build();
        CheckpointConfig checkpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(period)
                        .build();
        return EventProcessorGroupConfigImpl.builder()
                .eventProcessorCount(count)
                .readerGroupName(READER_GROUP)
                .streamName(STREAM_NAME)
                .checkpointConfig(checkpointConfig)
                .build();
    }

    private EventProcessorSystemImpl createMockSystem(final String name, final String processId, final String scope,
                                                      final SequenceAnswer<EventStreamReader<TestEvent>> readers,
                                                      final EventStreamWriter<TestEvent> writer,
                                                      final String readerGroupName) {
        EventStreamClientFactory clientFactory = Mockito.mock(EventStreamClientFactory.class);
        Mockito.when(clientFactory.createReader(anyString(), anyString(), any(), any()))
                .thenAnswer(readers);

        Mockito.when(clientFactory.<TestEvent>createEventWriter(anyString(), any(), any())).thenReturn(writer);

        ReaderGroup readerGroup = Mockito.mock(ReaderGroup.class);
        Mockito.when(readerGroup.getGroupName()).thenReturn(readerGroupName);

        ReaderGroupManager readerGroupManager = Mockito.mock(ReaderGroupManager.class);
        Mockito.when(readerGroupManager.getReaderGroup(anyString())).then(invocation -> readerGroup);

        return new EventProcessorSystemImpl(name, processId, scope, clientFactory, readerGroupManager);
    }

    private SequenceAnswer<EventStreamReader<TestEvent>> createEventReaders(int count, int[] input) throws ReinitializationRequiredException {
        List<EventStreamReader<TestEvent>> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(createMockReader(input));
        }
        return new SequenceAnswer<>(list);
    }

    @SuppressWarnings("unchecked")
    private EventStreamReader<TestEvent> createMockReader(int[] input) throws ReinitializationRequiredException {
        List<MockEventRead<TestEvent>> inputEvents = new ArrayList<>(input.length);
        for (int i = 0; i < input.length; i++) {
            inputEvents.add(new MockEventRead<>(i, new TestEvent(input[i])));
        }
        inputEvents.add(new MockEventRead<>(input.length, new TestEvent(-1)));

        EventStreamReader<TestEvent> reader = Mockito.mock(EventStreamReader.class);
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));
        return reader;
    }

    private void testEventProcessor(final EventProcessorSystem system,
                                    final EventProcessorConfig<TestEvent> eventProcessorConfig,
                                    final EventStreamReader<TestEvent> reader,
                                    final String readerId,
                                    final CheckpointStore checkpointStore,
                                    final int expectedSum) throws CheckpointStoreException {
        checkpointStore.addReader(PROCESS, READER_GROUP, readerId);
        EventProcessorCell<TestEvent> cell = new EventProcessorCell<>(eventProcessorConfig, reader,
                new EventStreamWriterMock<>(), system.getProcess(), readerId, 0, checkpointStore);

        cell.startAsync();

        cell.awaitTerminated();

        TestEventProcessor actor = (TestEventProcessor) cell.getActor();
        assertEquals(expectedSum, actor.sum);
        checkpointStore.removeReader(PROCESS, READER_GROUP, readerId);
        assertTrue(checkpointStore.getPositions(PROCESS, READER_GROUP).isEmpty());
    }
}
