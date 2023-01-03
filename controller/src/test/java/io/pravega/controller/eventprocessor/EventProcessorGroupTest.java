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
package io.pravega.controller.eventprocessor;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.ConcurrentEventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.shared.controller.event.ControllerEvent;

import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@Slf4j
public class EventProcessorGroupTest {
    private ScheduledExecutorService executor;
    private ScheduledExecutorService rebalanceExecutor;
    private EventProcessorGroup<ControllerEvent> requestEventProcessors;
    private ReaderGroupManager readerGroupMgr;
    private EventStreamClientFactory clientFactory;
    private EventProcessorSystem system;
    private StreamRequestHandler streamRequestHandler = mock(StreamRequestHandler.class);
    private CheckpointStore checkpointStore;
    private ReaderGroup mockReaderGroup;
    private EventStreamWriter<ControllerEvent> writer;
    private EventStreamReader<ControllerEvent> reader;
    private EventProcessorGroupConfig requestReadersConfig =
            EventProcessorGroupConfigImpl.builder()
                    .streamName("_requestStream")
                    .readerGroupName("scaleGroup")
                    .eventProcessorCount(1)
                    .checkpointConfig(CheckpointConfig.none())
                    .build();
    private EventProcessorConfig<ControllerEvent> requestConfig =
            EventProcessorConfig.builder()
                    .config(requestReadersConfig)
                    .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                    .serializer(ControllerEventProcessors.CONTROLLER_EVENT_SERIALIZER)
                    .supplier(() -> new ConcurrentEventProcessor<>(streamRequestHandler, executor))
                    .minRebalanceIntervalMillis(12000L)
                    .build();

    @Before
    public void setUp() {
        this.rebalanceExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "event-processor");
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        this.readerGroupMgr = mock(ReaderGroupManager.class);
        this.clientFactory = mock(EventStreamClientFactory.class);
        this.system = new EventProcessorSystemImpl("Controller", "host1", "_system", clientFactory, readerGroupMgr);
        this.checkpointStore = mock(CheckpointStore.class);
        try {
            doNothing().when(checkpointStore).addReaderGroup(anyString(), anyString());
            doNothing().when(checkpointStore).addReader(anyString(), anyString(), anyString());
        } catch (CheckpointStoreException ex) {
            ex.printStackTrace();
        }

        this.writer = mock(EventStreamWriter.class);
        doReturn(writer).when(clientFactory).createEventWriter(anyString(), any(), any());
        this.reader = mock(EventStreamReader.class);
        doReturn(reader).when(clientFactory).createReader(anyString(), anyString(), any(), any());

        doReturn(true).when(readerGroupMgr).createReaderGroup(anyString(), any());
        this.mockReaderGroup = mock(ReaderGroup.class);
        doReturn("scaleGroup").when(mockReaderGroup).getGroupName();
        doReturn(mockReaderGroup).when(readerGroupMgr).getReaderGroup(anyString());
    }

    @After
    public void tearDown() {
        requestEventProcessors.stopAsync();
        requestEventProcessors.awaitTerminated();
        ExecutorServiceHelpers.shutdown(executor);
        ExecutorServiceHelpers.shutdown(rebalanceExecutor);
    }

    @Test(timeout = 10000)
    public void testFailingSealReaderGroupInShutdown() throws CheckpointStoreException {
        this.requestEventProcessors = system.createEventProcessorGroup(requestConfig, checkpointStore, rebalanceExecutor);
        requestEventProcessors.awaitRunning();
        assertTrue(requestEventProcessors.isRunning());
        doThrow(new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, new Exception())).when(checkpointStore).sealReaderGroup(anyString(), anyString());
        requestEventProcessors.stopAsync();
        requestEventProcessors.awaitTerminated();
        verify(checkpointStore, times(1)).sealReaderGroup("host1", "scaleGroup");
        verify(reader, times(1)).closeAt(any());
        verify(checkpointStore, times(0)).removeReader(anyString(), anyString(), anyString());
        verify(checkpointStore, times(1)).removeReaderGroup("host1", "scaleGroup");
        verify(mockReaderGroup, times(1)).close();
        verify(writer, times(1)).close();
    }

    @Test(timeout = 10000)
    public void testShutdown() throws CheckpointStoreException {
        this.requestEventProcessors = system.createEventProcessorGroup(requestConfig, checkpointStore, rebalanceExecutor);
        requestEventProcessors.awaitRunning();
        assertTrue(requestEventProcessors.isRunning());
        Position mockReaderPosition = mock(Position.class);
        doReturn(ImmutableMap.of("reader1", mockReaderPosition)).when(checkpointStore).sealReaderGroup("host1", "scaleGroup");
        requestEventProcessors.stopAsync();
        requestEventProcessors.awaitTerminated();
        verify(checkpointStore, times(1)).sealReaderGroup("host1", "scaleGroup");
        verify(checkpointStore, times(1)).removeReader(anyString(), anyString(), anyString());
        verify(checkpointStore, times(1)).removeReaderGroup("host1", "scaleGroup");
        verify(mockReaderGroup, times(1)).readerOffline(anyString(), any());
        verify(mockReaderGroup, times(1)).close();
        verify(writer, times(1)).close();
    }

    @Test(timeout = 10000)
    public void testNotifyProcessFailureSuccess() throws CheckpointStoreException {
        this.requestEventProcessors = system.createEventProcessorGroup(requestConfig, checkpointStore, rebalanceExecutor);
        requestEventProcessors.awaitRunning();
        assertTrue(requestEventProcessors.isRunning());
        Position mockReaderPosition = mock(Position.class);
        doReturn(ImmutableMap.of("reader1", mockReaderPosition)).when(checkpointStore).sealReaderGroup("host1", "scaleGroup");
        doNothing().when(mockReaderGroup).readerOffline(anyString(), any());
        doNothing().when(checkpointStore).removeReader(anyString(), anyString(), anyString());
        doNothing().when(checkpointStore).removeReaderGroup(anyString(), anyString());
        requestEventProcessors.notifyProcessFailure("host1");
        verify(checkpointStore, times(1)).sealReaderGroup("host1", "scaleGroup");
        verify(mockReaderGroup, times(1)).readerOffline(anyString(), any());
        verify(checkpointStore, times(1)).removeReader(anyString(), anyString(), anyString());
        verify(checkpointStore, times(1)).removeReaderGroup("host1", "scaleGroup");
    }

    @Test(timeout = 10000)
    public void testNotifyProcessFailureError() throws CheckpointStoreException {
        this.requestEventProcessors = system.createEventProcessorGroup(requestConfig, checkpointStore, rebalanceExecutor);
        requestEventProcessors.awaitRunning();
        assertTrue(requestEventProcessors.isRunning());

        doThrow(new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, new Exception())).when(checkpointStore).sealReaderGroup(anyString(), anyString());
        AssertExtensions.assertThrows(CheckpointStoreException.class, () -> requestEventProcessors.notifyProcessFailure("host1"));
        verify(checkpointStore, times(1)).sealReaderGroup("host1", "scaleGroup");
        verify(mockReaderGroup, times(0)).readerOffline(anyString(), any());
        verify(checkpointStore, times(0)).removeReader(anyString(), anyString(), anyString());
        verify(checkpointStore, times(0)).removeReaderGroup("host1", "scaleGroup");
    }
}
