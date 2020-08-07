/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.controller.server.eventProcessor.ControllerEventProcessors.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ControllerEventProcessorsTest {
    ScheduledExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newScheduledThreadPool(10);
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
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = mock(StreamTransactionMetadataTasks.class);
        KVTableMetadataStore kvtStore = mock(KVTableMetadataStore.class);
        TableMetadataTasks kvtTasks = mock(TableMetadataTasks.class);
        ControllerEventProcessorConfig config = ControllerEventProcessorConfigImpl.withDefault();
        EventProcessorSystem system = mock(EventProcessorSystem.class);
        EventProcessorGroup<ControllerEvent> processor = getProcessor(Collections::emptyMap);

        try {
            when(system.createEventProcessorGroup(any(), any(), any())).thenReturn(processor);
        } catch (CheckpointStoreException e) {
            e.printStackTrace();
        }

        ControllerEventProcessors processors = new ControllerEventProcessors("host1",
                config, localController, checkpointStore, streamStore, bucketStore, 
                connectionPool, streamMetadataTasks, streamTransactionMetadataTasks, 
                kvtStore, kvtTasks, system, executor);
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
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = mock(StreamTransactionMetadataTasks.class);
        KVTableMetadataStore kvtStore = mock(KVTableMetadataStore.class);
        TableMetadataTasks kvtTasks = mock(TableMetadataTasks.class);
        ControllerEventProcessorConfig config = ControllerEventProcessorConfigImpl.withDefault();
        EventProcessorSystem system = mock(EventProcessorSystem.class);

        doAnswer(x -> null).when(streamMetadataTasks).initializeStreamWriters(any(), any());
        doAnswer(x -> null).when(streamTransactionMetadataTasks).initializeStreamWriters(any(EventStreamClientFactory.class), 
                any(ControllerEventProcessorConfig.class));

        LinkedBlockingQueue<CompletableFuture<Boolean>> createScopeResponses = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<CompletableFuture<Void>> createScopeSignals = new LinkedBlockingQueue<>();
        List<CompletableFuture<Boolean>> createScopeResponsesList = new LinkedList<>();
        List<CompletableFuture<Void>> createScopeSignalsList = new LinkedList<>();
        for (int i = 0; i < 2; i++) {
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
        for (int i = 0; i < 8; i++) {
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
                connectionPool, streamMetadataTasks, streamTransactionMetadataTasks,
                kvtStore, kvtTasks, system, executor);

        // call bootstrap on ControllerEventProcessors
        processors.bootstrap(streamTransactionMetadataTasks, streamMetadataTasks, kvtTasks);
        
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
        // since we call four create streams. We will wait on first three signal futures
        createStreamSignalsList.get(0).join();
        createStreamSignalsList.get(1).join();
        createStreamSignalsList.get(2).join();
        createStreamSignalsList.get(3).join();

        verify(controller, times(4)).createStream(anyString(), anyString(), any());

        // fail first four requests
        createStreamResponsesList.get(0).completeExceptionally(new RuntimeException());
        createStreamResponsesList.get(1).completeExceptionally(new RuntimeException());
        createStreamResponsesList.get(2).completeExceptionally(new RuntimeException());
        createStreamResponsesList.get(3).completeExceptionally(new RuntimeException());
        
        // this should result in a retry for four create streams. wait on next four signals
        createStreamSignalsList.get(4).join();
        createStreamSignalsList.get(5).join();
        createStreamSignalsList.get(6).join();
        createStreamSignalsList.get(7).join();

        verify(controller, times(8)).createStream(anyString(), anyString(), any());
        
        // complete successfully
        createStreamResponsesList.get(4).complete(true);
        createStreamResponsesList.get(5).complete(true);
        createStreamResponsesList.get(6).complete(true);
        createStreamResponsesList.get(7).complete(true);
    }
    
    @Test(timeout = 30000L)
    public void testTruncate() throws CheckpointStoreException, InterruptedException {
        Controller controller = mock(Controller.class);
        CheckpointStore checkpointStore = mock(CheckpointStore.class);
        StreamMetadataStore streamStore = mock(StreamMetadataStore.class);
        BucketStore bucketStore = mock(BucketStore.class);
        HostControllerStore hostStore = mock(HostControllerStore.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = mock(StreamTransactionMetadataTasks.class);
        KVTableMetadataStore kvtStore = mock(KVTableMetadataStore.class);
        TableMetadataTasks kvtTasks = mock(TableMetadataTasks.class);
        ControllerEventProcessorConfig config = ControllerEventProcessorConfigImpl.withDefault();
        EventProcessorSystem system = mock(EventProcessorSystem.class);

        CountDownLatch latch = new CountDownLatch(4);
        Map<Segment, Long> map = new HashMap<>();
        map.put(new Segment("scope", "stream", 0L), 0L);
        map.put(new Segment("scope", "stream", 1L), 0L);
        map.put(new Segment("scope", "stream", 2L), 0L);
        EventProcessorGroup<ControllerEvent> processor = spy(getProcessor(() -> {
            latch.countDown();
            if (latch.getCount() == 1) {
                // let one of the processors throw the exception. this should still be retried in the next cycle.
                throw new RuntimeException();
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return map;
        }));

        doAnswer(x -> processor).when(system).createEventProcessorGroup(any(), any(), any());

        doAnswer(x -> CompletableFuture.completedFuture(null)).when(controller).createScope(anyString());
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(controller).createStream(anyString(), anyString(), any());

        doAnswer(x -> null).when(streamMetadataTasks).initializeStreamWriters(any(), anyString());
        doAnswer(x -> null).when(streamTransactionMetadataTasks).initializeStreamWriters(any(EventStreamClientFactory.class),
                any(ControllerEventProcessorConfig.class));

        doAnswer(x -> CompletableFuture.completedFuture(null)).when(streamMetadataTasks).notifyTruncateSegment(anyString(), anyString(), any(), any(), anyLong());

        ControllerEventProcessors processors = new ControllerEventProcessors("host1",
                config, controller, checkpointStore, streamStore, bucketStore,
                connectionPool, streamMetadataTasks, streamTransactionMetadataTasks,
                kvtStore, kvtTasks, system, executor);
        
        // set truncation interval
        processors.setTruncationInterval(100L);
        processors.startAsync();
        processors.awaitRunning();
        ControllerEventProcessors processorsSpied = spy(processors);
        processorsSpied.bootstrap(streamTransactionMetadataTasks, streamMetadataTasks, kvtTasks);

        latch.await();
        
        // verify that truncate method is being called periodically. 
        verify(processorsSpied, atLeastOnce()).truncate(processor, streamMetadataTasks, STREAM_REQUEST_EP);
        verify(processorsSpied, atLeastOnce()).truncate(processor, streamMetadataTasks, KVT_REQUEST_EP);
        verify(processorsSpied, atLeastOnce()).truncate(processor, streamMetadataTasks, COMMIT_TXN_EP);
        verify(processorsSpied, atLeastOnce()).truncate(processor, streamMetadataTasks, ABORT_TXN_EP);
    }

    private EventProcessorGroup<ControllerEvent> getProcessor(Supplier<Map<Segment, Long>> checkpointMap) {
        return new EventProcessorGroup<ControllerEvent>() {
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
            public Map<Segment, Long> getCheckpoint() {
                return checkpointMap.get();
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
    }

}
