/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.PositionImpl;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

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
            this.position = new PositionImpl(Collections.singletonMap(segment, position));
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
        Assert.assertTrue(true);

        // Test case 6. Close event processor cell when reader/checkpoint store throw exceptions.
        Mockito.doThrow(new IllegalArgumentException("Failing reader")).when(reader).closeAt(any());
        checkpointStore = Mockito.spy(checkpointStore);
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
        Assert.assertTrue(true);
    }

    @Test(timeout = 10000)
    public void testEventProcessorWriter() throws ReinitializationRequiredException, CheckpointStoreException {
        int initialCount = 1;
        String systemName = "testSystem";
        String readerGroupName = "testReaderGroup";
        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        EventStreamWriterMock<TestEvent> writer = new EventStreamWriterMock<>();

        CheckpointStore checkpointStore = CheckpointStoreFactory.createInMemoryStore();

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
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>)
                system.createEventProcessorGroup(eventProcessorConfig, checkpointStore);

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
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>)
                system.createEventProcessorGroup(eventProcessorConfig, checkpointStore);

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
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>) system
                .createEventProcessorGroup(eventProcessorConfig, checkpointStore);

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
        EventProcessorGroupImpl<TestEvent> group = (EventProcessorGroupImpl<TestEvent>) system.createEventProcessorGroup(eventProcessorConfig,
                    checkpointStore);
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

        // Stop the group, and await its termmination.
        group.stopAsync();
        group.awaitTerminated();
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
        assertTrue(checkpointStore.getPositions(PROCESS, READER_GROUP).isEmpty());
    }
}
