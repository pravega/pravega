/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.eventProcessor.impl;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.CheckpointStore;
import com.emc.pravega.controller.eventProcessor.Decider;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.Props;
import com.emc.pravega.controller.eventProcessor.StreamEvent;
import com.emc.pravega.stream.EventPointer;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Sequence;
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

    private static String scope = "scope";
    private static String streamName = "stream";
    private static String readerGroup = "readerGroup";
    private static String readerId = "reader-1";
    private static String process = "process";

    @Data
    @AllArgsConstructor
    public static class TestEvent implements StreamEvent, Serializable {
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
            if (event == null) {
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
            Segment segment = new Segment(scope, streamName, 0);
            this.position = new PositionImpl(Collections.singletonMap(segment, position));
        }

        @Override
        public Sequence getEventSequence() {
            return null;
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
        public boolean isRoutingRebalance() {
            return false;
        }
    }

    @Test
    public void testEventProcessorCell() {
        CheckpointStore checkpointStore = new InMemoryCheckpointStore();

        CheckpointConfig.CheckpointPeriod period =
                CheckpointConfig.CheckpointPeriod.builder()
                        .numEvents(1)
                        .numSeconds(1)
                        .build();

        CheckpointConfig checkpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .storeType(CheckpointConfig.StoreType.InMemory)
                        .checkpointPeriod(period)
                        .build();

        EventProcessorGroupConfig config =
                EventProcessorGroupConfigImpl.builder()
                        .eventProcessorCount(1)
                        .readerGroupName(readerGroup)
                        .streamName(streamName)
                        .checkpointConfig(checkpointConfig)
                        .build();

        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int expectedSum = input.length * (input.length + 1) / 2;

        List<MockEventRead<TestEvent>> inputEvents = new ArrayList<>(input.length);
        for (int i = 0; i < input.length; i++) {
            inputEvents.add(new MockEventRead<>(i, new TestEvent(input[i])));
        }
        inputEvents.add(new MockEventRead<>(input.length, null));

        EventProcessorSystem system = Mockito.mock(EventProcessorSystem.class);
        Mockito.when(system.getProcess()).thenReturn(process);

        EventStreamReader<TestEvent> reader = Mockito.mock(EventStreamReader.class);

        checkpointStore.addReaderGroup(process, readerGroup);

        // Test case 1. Actor does not throw any exception during normal operation.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        Props<TestEvent> props = Props.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(false))
                .serializer(new JavaSerializer<>())
                .decider((Throwable e) -> Decider.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, props, reader, readerId, checkpointStore, expectedSum);

        // Test case 2. Actor throws an error during normal operation, and Directive is to Resume on error.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        props = Props.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(true))
                .serializer(new JavaSerializer<>())
                .decider((Throwable e) ->
                        (e instanceof IllegalArgumentException) ? Decider.Directive.Resume : Decider.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, props, reader, readerId, checkpointStore, expectedSum);

        // Test case 3. Actor throws an error during normal operation, and Directive is to Restart on error.
        Mockito.when(reader.readNextEvent(anyLong())).thenAnswer(new SequenceAnswer<>(inputEvents));

        props = Props.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(true))
                .serializer(new JavaSerializer<>())
                .decider((Throwable e) ->
                        (e instanceof IllegalArgumentException) ? Decider.Directive.Restart : Decider.Directive.Stop)
                .config(config)
                .build();
        testEventProcessor(system, props, reader, readerId, checkpointStore, 0);
    }

    private void testEventProcessor(final EventProcessorSystem system,
                                    final Props<TestEvent> props,
                                    final EventStreamReader<TestEvent> reader,
                                    final String readerId,
                                    final CheckpointStore checkpointStore,
                                    final int expectedSum) {
        EventProcessorCell<TestEvent> cell =
                new EventProcessorCell<>(props, reader, system.getProcess(), readerId, checkpointStore);

        cell.startAsync();

        cell.awaitStopped();

        TestEventProcessor actor = (TestEventProcessor) cell.getActor();
        assertEquals(expectedSum, actor.sum);
        assertTrue(checkpointStore.getPositions(process, readerGroup).isEmpty());
    }
}
