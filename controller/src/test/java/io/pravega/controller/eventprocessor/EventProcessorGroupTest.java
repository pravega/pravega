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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
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
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;

@Slf4j
public class EventProcessorGroupTest {
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);
    private ScheduledExecutorService executor;
    private ScheduledExecutorService rebalanceExecutor;
    private EventProcessorGroup<ControllerEvent> requestEventProcessors;
    private ReaderGroupManager readerGroupMgr;
    private EventStreamClientFactory clientFactory;
    private EventProcessorSystem system;
    private StreamRequestHandler streamRequestHandler = mock(StreamRequestHandler.class);
    private CheckpointStore checkpointStore;
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
        } catch(CheckpointStoreException ex){
            ex.printStackTrace();
        }

        EventStreamWriter<ControllerEvent> writer = mock(EventStreamWriter.class);
        doReturn(writer).when(clientFactory).createEventWriter(anyString(), any(), any());
        EventStreamReader<ControllerEvent> reader = mock(EventStreamReader.class);
        doReturn(reader).when(clientFactory).createReader(anyString(), anyString(), any(), any());

        doReturn(true).when(readerGroupMgr).createReaderGroup(anyString(), any());
        ReaderGroup mockReaderGroup = mock(ReaderGroup.class);
        doReturn(mockReaderGroup).when(readerGroupMgr).getReaderGroup(anyString());
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
        rebalanceExecutor.shutdownNow();
    }

    @Test(timeout = 10000)
    public void testEventProcessorGroup() throws CheckpointStoreException {
        this.requestEventProcessors = system.createEventProcessorGroup(requestConfig, checkpointStore, rebalanceExecutor);
        requestEventProcessors.awaitRunning();
        assertTrue(requestEventProcessors.isRunning());
        doThrow(new CheckpointStoreException(CheckpointStoreException.Type.Connectivity, new Exception())).when(checkpointStore).sealReaderGroup(anyString(), anyString());
        //EventProcessorGroupImpl eventProcessor = (EventProcessorGroupImpl) requestEventProcessors;
        requestEventProcessors.stopAsync();
    }
}
