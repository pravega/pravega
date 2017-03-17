/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import com.emc.pravega.stream.EventPointer;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.PositionImpl;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;

/**
 * Event processor test.
 */
public class EventProcessorTest {

    private static final String SCOPE = "scope";
    private static final String STREAM_NAME = "stream";
    private static final String READER_GROUP = "readerGroup";
    private static final String READER_ID = "reader-1";
    private static final String PROCESS = "process";

    @Data
    @AllArgsConstructor
    public static class TestEvent implements ControllerEvent, Serializable {
        int number;
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
        protected void process(TestEvent event) {
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
                .serializer(new JavaSerializer<>())
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, eventProcessorConfig, reader, READER_ID, checkpointStore, expectedSum);

        // Test case 2. Actor throws an error during normal operation, and Directive is to Resume on error.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(true))
                .serializer(new JavaSerializer<>())
                .decider((Throwable e) ->
                        (e instanceof IllegalArgumentException) ? ExceptionHandler.Directive.Resume : ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, eventProcessorConfig, reader, READER_ID, checkpointStore, expectedSum);

        // Test case 3. Actor throws an error during normal operation, and Directive is to Restart on error.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(true))
                .serializer(new JavaSerializer<>())
                .decider((Throwable e) ->
                        (e instanceof IllegalArgumentException) ? ExceptionHandler.Directive.Restart : ExceptionHandler.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, eventProcessorConfig, reader, READER_ID, checkpointStore, 0);
    }

    private void testEventProcessor(final EventProcessorSystem system,
                                    final EventProcessorConfig<TestEvent> eventProcessorConfig,
                                    final EventStreamReader<TestEvent> reader,
                                    final String readerId,
                                    final CheckpointStore checkpointStore,
                                    final int expectedSum) throws CheckpointStoreException {
        checkpointStore.addReader(PROCESS, READER_GROUP, readerId);
        EventProcessorCell<TestEvent> cell = new EventProcessorCell<>(eventProcessorConfig, reader, system.getProcess(),
                readerId, 0, checkpointStore);

        cell.startAsync();

        cell.awaitTerminated();

        TestEventProcessor actor = (TestEventProcessor) cell.getActor();
        assertEquals(expectedSum, actor.sum);
        assertTrue(checkpointStore.getPositions(PROCESS, READER_GROUP).isEmpty());
    }
}
