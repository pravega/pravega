/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server.eventProcessor;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.server.controller.service.mocks.SegmentHelperMock;
import io.pravega.server.controller.service.eventProcessor.EventProcessorGroup;
import io.pravega.server.controller.service.eventProcessor.EventProcessorSystem;
import io.pravega.server.controller.service.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.server.controller.service.store.checkpoint.CheckpointStore;
import io.pravega.server.controller.service.store.checkpoint.CheckpointStoreException;
import io.pravega.server.controller.service.store.host.HostControllerStore;
import io.pravega.server.controller.service.store.stream.StreamMetadataStore;
import io.pravega.server.controller.service.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.netty.ConnectionFactory;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ControllerEventProcessorsTest {
    ScheduledExecutorService executor;

    @Test(timeout = 10000)
    public void testEventKey() {
        UUID txid = UUID.randomUUID();
        String scope = "test";
        String stream = "test";
        AbortEvent abortEvent = new AbortEvent(scope, stream, txid);
        CommitEvent commitEvent = new CommitEvent(scope, stream, txid);
        assertEquals(abortEvent.getKey(), "test/test");
        assertEquals(commitEvent.getKey(), "test/test");
    }

    @Test(timeout = 10000)
    public void testHandleOrphaned() {
        Controller localController = mock(Controller.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        StreamMetadataStore streamStore = mock(StreamMetadataStore.class);
        HostControllerStore hostStore = mock(HostControllerStore.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        executor = Executors.newSingleThreadScheduledExecutor();
        ControllerEventProcessorConfig config = ControllerEventProcessorConfigImpl.withDefault();
        EventProcessorSystem system = mock(EventProcessorSystem.class);
        EventProcessorGroup<ControllerEvent> processor = new EventProcessorGroup<ControllerEvent>() {
            @Override
            public void notifyProcessFailure(String process) throws CheckpointStoreException {

            }

            @Override
            public EventStreamWriter<ControllerEvent> getWriter() {
                return null;
            }

            @Override
            public Set<String> getProcesses() throws CheckpointStoreException {
                return Sets.newHashSet("host1", "host2");
            }

            @Override
            public Service startAsync() {
                return null;
            }

            @Override
            public boolean isRunning() {
                return false;
            }

            @Override
            public State state() {
                return null;
            }

            @Override
            public Service stopAsync() {
                return null;
            }

            @Override
            public void awaitRunning() {

            }

            @Override
            public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {

            }

            @Override
            public void awaitTerminated() {

            }

            @Override
            public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {

            }

            @Override
            public Throwable failureCause() {
                return null;
            }

            @Override
            public void addListener(Listener listener, Executor executor) {

            }

            @Override
            public void close() throws Exception {

            }
        };

        try {
            when(system.createEventProcessorGroup(any(), any())).thenReturn(processor);
        } catch (CheckpointStoreException e) {
            e.printStackTrace();
        }

        ControllerEventProcessors processors = new ControllerEventProcessors("host1",
                config, localController, checkpointStore, streamStore,
                hostStore, SegmentHelperMock.getSegmentHelperMock(), connectionFactory, streamMetadataTasks, system, executor);
        processors.startAsync();
        processors.awaitRunning();
        assertTrue(FutureHelpers.await(processors.handleOrphanedReaders(() -> Sets.newHashSet("host1"))));
        assertTrue(FutureHelpers.await(processors.notifyProcessFailure("host1")));
        processors.shutDown();
    }
}
