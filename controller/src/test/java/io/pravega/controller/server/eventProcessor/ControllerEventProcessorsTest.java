/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.impl.Controller;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ControllerEventProcessorsTest {
    ScheduledExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }
    
    @Test(timeout = 10000)
    public void testEventKey() {
        UUID txid = UUID.randomUUID();
        String scope = "test";
        String stream = "test";
        AbortEvent abortEvent = new AbortEvent(scope, stream, 0, txid);
        CommitEvent commitEvent = new CommitEvent(scope, stream, 0);
        assertEquals(abortEvent.getKey(), "test/test");
        assertEquals(commitEvent.getKey(), "test/test");
    }

    @Test(timeout = 10000)
    public void testHandleOrphaned() {
        Controller localController = mock(Controller.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        StreamMetadataStore streamStore = mock(StreamMetadataStore.class);
        BucketStore bucketStore = mock(BucketStore.class);
        HostControllerStore hostStore = mock(HostControllerStore.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = mock(StreamTransactionMetadataTasks.class);
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
                config, localController, checkpointStore, streamStore, bucketStore, 
                connectionFactory, streamMetadataTasks, streamTransactionMetadataTasks,
                system, executor);
        processors.startAsync();
        processors.awaitRunning();
        assertTrue(Futures.await(processors.sweepFailedProcesses(() -> Sets.newHashSet("host1"))));
        assertTrue(Futures.await(processors.handleFailedProcess("host1")));
        processors.shutDown();
    }
    
    @Test(timeout = 30000L)
    public void testBootstrap() {
        Controller controller = mock(Controller.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        StreamMetadataStore streamStore = mock(StreamMetadataStore.class);
        BucketStore bucketStore = mock(BucketStore.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = mock(StreamTransactionMetadataTasks.class);
        ControllerEventProcessorConfig config = ControllerEventProcessorConfigImpl.withDefault();
        EventProcessorSystem system = mock(EventProcessorSystem.class);

        doAnswer(x -> null).when(streamMetadataTasks).initializeStreamWriters(any(), any());
        doAnswer(x -> null).when(streamTransactionMetadataTasks).initializeStreamWriters(any(EventStreamClientFactory.class), 
                any(ControllerEventProcessorConfig.class));

        LinkedBlockingQueue<CompletableFuture<Boolean>> createScopeResponses = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<CompletableFuture<Void>> createScopeSignals = new LinkedBlockingQueue<>();
        List<CompletableFuture<Boolean>> createScopeResponsesList = new LinkedList<>();
        List<CompletableFuture<Void>> createScopeSignalsList = new LinkedList<>();
        for (int i = 0; i < 2; i ++) {
            CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();
            CompletableFuture<Void> signalFuture = new CompletableFuture<>();
            createScopeResponsesList.add(responseFuture);
            createScopeResponses.add(responseFuture);
            createScopeSignalsList.add(signalFuture);
            createScopeSignals.add(signalFuture);
        }

        // return a future from latches queue
        doAnswer(x -> {
            createScopeSignals.take().complete(null);
            return createScopeResponses.take();
        }).when(controller).createScope(anyString());

        LinkedBlockingQueue<CompletableFuture<Boolean>> createStreamResponses = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<CompletableFuture<Void>> createStreamSignals = new LinkedBlockingQueue<>();
        List<CompletableFuture<Boolean>> createStreamResponsesList = new LinkedList<>();
        List<CompletableFuture<Void>> createStreamSignalsList = new LinkedList<>();
        for (int i = 0; i < 6; i ++) {
            CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();
            CompletableFuture<Void> signalFuture = new CompletableFuture<>();
            createStreamResponsesList.add(responseFuture);
            createStreamResponses.add(responseFuture);
            createStreamSignalsList.add(signalFuture);
            createStreamSignals.add(signalFuture);
        }

        // return a future from latches queue
        doAnswer(x -> {
            createStreamSignals.take().complete(null);
            return createStreamResponses.take();
        }).when(controller).createStream(anyString(), anyString(), any());

        ControllerEventProcessors processors = new ControllerEventProcessors("host1",
                config, controller, checkpointStore, streamStore, bucketStore,
                connectionFactory, streamMetadataTasks, streamTransactionMetadataTasks,
                system, executor);

        // call bootstrap on ControllerEventProcessors
        processors.bootstrap(streamTransactionMetadataTasks, streamMetadataTasks);
        
        // wait on create scope being called.
        createScopeSignalsList.get(0).join();
        
        verify(controller, times(1)).createScope(any());
        
        // complete scopeFuture1 exceptionally. this should result in a retry. 
        createScopeResponsesList.get(0).completeExceptionally(new RuntimeException());

        // wait on second scope signal being called
        createScopeSignalsList.get(1).join();

        verify(controller, times(2)).createScope(any());
        
        // so far no create stream should have been invoked
        verify(controller, times(0)).createStream(anyString(), anyString(), any());

        // complete scopeFuture2 successfully
        createScopeResponsesList.get(1).complete(true);

        // create streams should be called now
        // since we call three create streams. We will wait on first three signal futures
        createStreamSignalsList.get(0).join();
        createStreamSignalsList.get(1).join();
        createStreamSignalsList.get(2).join();

        verify(controller, times(3)).createStream(anyString(), anyString(), any());

        // fail first three requests
        createStreamResponsesList.get(0).completeExceptionally(new RuntimeException());
        createStreamResponsesList.get(1).completeExceptionally(new RuntimeException());
        createStreamResponsesList.get(2).completeExceptionally(new RuntimeException());
        
        // this should result in a retry for three create streams. wait on next three signals
        createStreamSignalsList.get(3).join();
        createStreamSignalsList.get(4).join();
        createStreamSignalsList.get(5).join();

        verify(controller, times(6)).createStream(anyString(), anyString(), any());
        
        // complete successfully
        createStreamResponsesList.get(3).complete(true);
        createStreamResponsesList.get(4).complete(true);
        createStreamResponsesList.get(5).complete(true);
    }
    
}
