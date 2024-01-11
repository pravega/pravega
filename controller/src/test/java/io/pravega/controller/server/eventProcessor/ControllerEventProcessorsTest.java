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
package io.pravega.controller.server.eventProcessor;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.controller.store.checkpoint.ZKCheckpointStore;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;

public class ControllerEventProcessorsTest extends ThreadPooledTestSuite {

    @Override
    public int getThreadPoolSize() {
        return 10;
    }
    
    @Test(timeout = 10000)
    public void testEventKey() {
        UUID txid = UUID.randomUUID();
        String scope = "test";
        String stream = "test";
        AbortEvent abortEvent = new AbortEvent(scope, stream, 0, txid, 21L);
        CommitEvent commitEvent = new CommitEvent(scope, stream, 0);
        assertEquals(abortEvent.getKey(), "test/test");
        assertEquals(commitEvent.getKey(), "test/test");
    }

    @Test(timeout = 30000L)
    public void testIsReady() throws Exception {
        LocalController controller = mock(LocalController.class);
        StreamMetadataStore streamStore = mock(StreamMetadataStore.class);
        BucketStore bucketStore = mock(BucketStore.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        StreamMetadataTasks streamMetadataTasks = mock(StreamMetadataTasks.class);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = mock(StreamTransactionMetadataTasks.class);
        KVTableMetadataStore kvtStore = mock(KVTableMetadataStore.class);
        TableMetadataTasks kvtTasks = mock(TableMetadataTasks.class);
        ControllerEventProcessorConfig config = ControllerEventProcessorConfigImpl.withDefault();
        EventProcessorSystem system = mock(EventProcessorSystem.class);
        CuratorZookeeperClient curatorZKClientMock = mock(CuratorZookeeperClient.class);
        CuratorFramework client = mock(CuratorFramework.class);
        Listenable listen = mock(Listenable.class);
        doNothing().when(listen).addListener(any(ConnectionStateListener.class));
        doReturn(listen).when(client).getConnectionStateListenable();
        doReturn(curatorZKClientMock).when(client).getZookeeperClient();
        doReturn(true).when(curatorZKClientMock).isConnected();
        ZKCheckpointStore checkpointStore = (ZKCheckpointStore) CheckpointStoreFactory.createZKStore(client);

        doAnswer(x -> null).when(streamMetadataTasks).initializeStreamWriters(any(), any());
        doAnswer(x -> null).when(streamTransactionMetadataTasks).initializeStreamWriters(any(EventStreamClientFactory.class),
                any(ControllerEventProcessorConfig.class));
        CompletableFuture<Boolean> createScopeResponseFuture = new CompletableFuture<>();
        CompletableFuture<Void> createScopeSignalFuture = new CompletableFuture<>();
        doAnswer(x -> {
            createScopeSignalFuture.complete(null);
            return createScopeResponseFuture;
        }).when(controller).createScope(anyString());

        LinkedBlockingQueue<CompletableFuture<Boolean>> createStreamResponses = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<CompletableFuture<Void>> createStreamSignals = new LinkedBlockingQueue<>();
        List<CompletableFuture<Boolean>> createStreamResponsesList = new LinkedList<>();
        List<CompletableFuture<Void>> createStreamSignalsList = new LinkedList<>();
        for (int i = 0; i < 4; i++) {
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
        }).when(controller).createInternalStream(anyString(), anyString(), any());

        @Cleanup
        ControllerEventProcessors processors = spy(new ControllerEventProcessors("host1",
                config, controller, checkpointStore, streamStore, bucketStore,
                connectionPool, streamMetadataTasks, streamTransactionMetadataTasks,
                kvtStore, kvtTasks, system, executorService()));

        //Check isReady() method before invoking bootstrap
        Assert.assertFalse(processors.getBootstrapCompleted().get());
        Assert.assertTrue(processors.isMetadataServiceConnected());
        Assert.assertFalse(processors.isRunning());
        Assert.assertFalse(processors.isReady());

        // Call bootstrap on ControllerEventProcessors
        processors.bootstrap(streamTransactionMetadataTasks, streamMetadataTasks, kvtTasks);

        // Wait on create scope being called.
        createScopeSignalFuture.join();
        createScopeResponseFuture.complete(true);
        createStreamSignalsList.get(0).join();
        createStreamSignalsList.get(1).join();
        createStreamSignalsList.get(2).join();
        createStreamSignalsList.get(3).join();
        createStreamResponsesList.get(0).complete(true);
        createStreamResponsesList.get(1).complete(true);
        createStreamResponsesList.get(2).complete(true);
        createStreamResponsesList.get(3).complete(true);

        AssertExtensions.assertEventuallyEquals(true, () -> processors.getBootstrapCompleted().get(), 10000);
        Assert.assertTrue(processors.isMetadataServiceConnected());
        Assert.assertFalse(processors.isRunning());
        Assert.assertFalse(processors.isReady());

        EventProcessorGroup mockEventProcessorGroup = mock(EventProcessorGroup.class);
        doNothing().when(mockEventProcessorGroup).awaitRunning();
        when(system.createEventProcessorGroup(any(EventProcessorConfig.class), any(CheckpointStore.class), any(ScheduledExecutorService.class)))
                .thenThrow(new RuntimeException("Error occurred") )
                .thenReturn(mockEventProcessorGroup)
                .thenThrow(new RuntimeException("Error occurred") )
                .thenReturn(mockEventProcessorGroup)
                .thenThrow(new RuntimeException("Error occurred") )
                .thenReturn(mockEventProcessorGroup)
                .thenThrow(new RuntimeException("Error occurred") )
                .thenReturn(mockEventProcessorGroup);

        processors.startAsync();
        processors.awaitRunning();
        Assert.assertTrue(processors.isMetadataServiceConnected());
        Assert.assertTrue(processors.isBootstrapCompleted());
        Assert.assertTrue(processors.isRunning());
        Assert.assertTrue(processors.isReady());
    }

    @Test(timeout = 10000)
    public void testHandleOrphaned() throws CheckpointStoreException {
        LocalController localController = mock(LocalController.class);
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
        EventProcessorGroup<ControllerEvent> processor = getProcessor();
        EventProcessorGroup<ControllerEvent> mockProcessor = spy(processor);

        doThrow(new CheckpointStoreException("host not found")).when(mockProcessor).notifyProcessFailure("host3");
        when(system.createEventProcessorGroup(any(), any(), any())).thenReturn(mockProcessor);

        @Cleanup
        ControllerEventProcessors processors = new ControllerEventProcessors("host1",
                config, localController, checkpointStore, streamStore, bucketStore, 
                connectionPool, streamMetadataTasks, streamTransactionMetadataTasks, 
                kvtStore, kvtTasks, system, executorService());
        //check for a case where init is not initialized so that kvtRequestProcessors don't get initialized and will be null
        assertTrue(Futures.await(processors.sweepFailedProcesses(() -> Sets.newHashSet("host1"))));
        Assert.assertFalse(processors.isReady());
        Assert.assertFalse(processors.isBootstrapCompleted());
        Assert.assertFalse(processors.isMetadataServiceConnected());
        processors.startAsync();
        processors.awaitRunning();
        assertTrue(Futures.await(processors.sweepFailedProcesses(() -> Sets.newHashSet("host1"))));
        assertTrue(Futures.await(processors.handleFailedProcess("host1")));
        AssertExtensions.assertFutureThrows("host not found", processors.handleFailedProcess("host3"), e -> e instanceof CheckpointStoreException);
        processors.shutDown();
    }
    
    @Test(timeout = 30000L)
    public void testBootstrap() throws Exception {
        LocalController controller = mock(LocalController.class);
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
        }).when(controller).createInternalStream(anyString(), anyString(), any());

        @Cleanup
        ControllerEventProcessors processors = new ControllerEventProcessors("host1",
                config, controller, checkpointStore, streamStore, bucketStore,
                connectionPool, streamMetadataTasks, streamTransactionMetadataTasks,
                kvtStore, kvtTasks, system, executorService());

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
        verify(controller, times(0)).createInternalStream(anyString(), anyString(), any());

        // complete scopeFuture2 successfully
        createScopeResponsesList.get(1).complete(true);

        // create streams should be called now
        // since we call four create streams. We will wait on first three signal futures
        createStreamSignalsList.get(0).join();
        createStreamSignalsList.get(1).join();
        createStreamSignalsList.get(2).join();
        createStreamSignalsList.get(3).join();

        verify(controller, times(4)).createInternalStream(anyString(), anyString(), any());

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

        verify(controller, times(8)).createInternalStream(anyString(), anyString(), any());
        
        // complete successfully
        createStreamResponsesList.get(4).complete(true);
        createStreamResponsesList.get(5).complete(true);
        createStreamResponsesList.get(6).complete(true);
        createStreamResponsesList.get(7).complete(true);
        AssertExtensions.assertEventuallyEquals(true, () -> processors.getBootstrapCompleted().get(), 10000);
    }
    
    @Test(timeout = 10000L)
    public void testTruncate() throws CheckpointStoreException, InterruptedException {
        LocalController controller = mock(LocalController.class);
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

        Map<SegmentWithRange, Long> map1 = new HashMap<>();
        map1.put(new SegmentWithRange(new Segment("scope", "stream", 0L), 0.0, 0.33), 10L);
        map1.put(new SegmentWithRange(new Segment("scope", "stream", 1L), 0.33, 0.66), 10L);
        map1.put(new SegmentWithRange(new Segment("scope", "stream", 2L), 0.66, 1.0), 20L);
        Map<SegmentWithRange, Long> map2 = new HashMap<>();
        map2.put(new SegmentWithRange(new Segment("scope", "stream", 0L), 0.0, 0.33), 20L);
        map2.put(new SegmentWithRange(new Segment("scope", "stream", 2L), 0.66, 1.0), 10L);
        Map<SegmentWithRange, Long> map3 = new HashMap<>();
        map3.put(new SegmentWithRange(new Segment("scope", "stream", 3L), 0.0, 0.33), 0L);
        map3.put(new SegmentWithRange(new Segment("scope", "stream", 4L), 0.33, 0.66), 10L);
        map3.put(new SegmentWithRange(new Segment("scope", "stream", 5L), 0.66, 1.0), 20L);

        PositionImpl position1 = new PositionImpl(map1);
        PositionImpl position2 = new PositionImpl(map2);
        PositionImpl position3 = new PositionImpl(map3);
        
        doReturn(getProcessor()).when(system).createEventProcessorGroup(any(), any(), any());

        doReturn(CompletableFuture.completedFuture(null)).when(controller).createScope(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(controller).createInternalStream(anyString(), anyString(), any());

        doNothing().when(streamMetadataTasks).initializeStreamWriters(any(), anyString());
        doNothing().when(streamTransactionMetadataTasks).initializeStreamWriters(any(EventStreamClientFactory.class),
                any(ControllerEventProcessorConfig.class));
        
        AtomicBoolean requestCalled = new AtomicBoolean(false);
        AtomicBoolean commitCalled = new AtomicBoolean(false);
        CompletableFuture<Void> requestStreamTruncationFuture = new CompletableFuture<>();
        CompletableFuture<Void> kvtStreamTruncationFuture = new CompletableFuture<>();
        CompletableFuture<Void> abortStreamTruncationFuture = new CompletableFuture<>();
        CompletableFuture<Void> commitStreamTruncationFuture = new CompletableFuture<>();
        doAnswer(x -> {
            String argument = x.getArgument(1);
            if (argument.equals(config.getRequestStreamName())) {
                // let one of the processors throw the exception. this should still be retried in the next cycle.
                if (!requestCalled.get()) {
                    requestCalled.set(true);
                    throw new RuntimeException("inducing sporadic failure");
                } else {
                    requestStreamTruncationFuture.complete(null);
                }
            } else if (argument.equals(config.getCommitStreamName())) {
                // let one of the processors throw the exception. this should still be retried in the next cycle.
                if (commitCalled.get()) {
                    commitStreamTruncationFuture.complete(null);
                } else {
                    commitCalled.set(true);
                    return CompletableFuture.completedFuture(false);
                }
            } else if (argument.equals(config.getAbortStreamName())) {
                abortStreamTruncationFuture.complete(null);
            } else if (argument.equals(config.getKvtStreamName())) {
                kvtStreamTruncationFuture.complete(null);
            }
            return CompletableFuture.completedFuture(true);
        }).when(streamMetadataTasks).startTruncation(anyString(), anyString(), any(), any());

        Set<String> processes = Sets.newHashSet("p1", "p2", "p3");

        // first throw checkpoint store exception
        AtomicBoolean signal = new AtomicBoolean(false);
        CountDownLatch cd = new CountDownLatch(4);
        doAnswer(x -> {
            // this ensures that the call to truncate has been invoked for all 4 internal streams. 
            cd.countDown();
            cd.await();
            if (!signal.get()) {
                throw new CheckpointStoreException("CheckpointStoreException");
            } else {
                return processes;
            }
        }).when(checkpointStore).getProcesses();
        Map<String, PositionImpl> r1 = Collections.singletonMap("r1", position1);
        doReturn(r1).when(checkpointStore).getPositions(eq("p1"), anyString());
        Map<String, PositionImpl> r2 = Collections.singletonMap("r2", position1);
        doReturn(r2).when(checkpointStore).getPositions(eq("p2"), anyString());
        Map<String, PositionImpl> r3 = Collections.singletonMap("r3", position1);
        doReturn(r3).when(checkpointStore).getPositions(eq("p3"), anyString());

        @Cleanup
        ControllerEventProcessors processors = new ControllerEventProcessors("host1",
                config, controller, checkpointStore, streamStore, bucketStore,
                connectionPool, streamMetadataTasks, streamTransactionMetadataTasks,
                kvtStore, kvtTasks, system, executorService());

        // set truncation interval
        processors.setTruncationInterval(100L);
        processors.startAsync();
        processors.awaitRunning();
        ControllerEventProcessors processorsSpied = spy(processors);
        processorsSpied.bootstrap(streamTransactionMetadataTasks, streamMetadataTasks, kvtTasks);

        // wait for all 4 countdown exceptions to have been thrown. 
        cd.await();

        verify(processorsSpied, atLeast(4)).truncate(any(), any(), any());
        verify(checkpointStore, atLeast(4)).getProcesses();
        verify(checkpointStore, never()).getPositions(anyString(), anyString());
        verify(streamMetadataTasks, never()).startTruncation(anyString(), anyString(), any(), any());

        signal.set(true);

        CompletableFuture.allOf(requestStreamTruncationFuture, commitStreamTruncationFuture, abortStreamTruncationFuture, kvtStreamTruncationFuture).join();

        // verify that truncate method is being called periodically. 
        verify(processorsSpied, atLeastOnce()).truncate(config.getRequestStreamName(), config.getRequestReaderGroupName(), streamMetadataTasks);
        verify(processorsSpied, atLeastOnce()).truncate(config.getCommitStreamName(), config.getCommitReaderGroupName(), streamMetadataTasks);
        verify(processorsSpied, atLeastOnce()).truncate(config.getAbortStreamName(), config.getAbortReaderGroupName(), streamMetadataTasks);
        verify(processorsSpied, atLeastOnce()).truncate(config.getKvtStreamName(), config.getKvtReaderGroupName(), streamMetadataTasks);

        for (int i = 1; i <= 3; i++) {
            verify(checkpointStore, atLeastOnce()).getPositions("p" + i, config.getRequestReaderGroupName());
            verify(checkpointStore, atLeastOnce()).getPositions("p" + i, config.getCommitReaderGroupName());
            verify(checkpointStore, atLeastOnce()).getPositions("p" + i, config.getAbortReaderGroupName());
            verify(checkpointStore, atLeastOnce()).getPositions("p" + i, config.getKvtReaderGroupName());
        }
    }

    private EventProcessorGroup<ControllerEvent> getProcessor() {
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
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            public Service startAsync() {
                return null;
            }

            @Override
            public boolean isRunning() {
                return false;
            }

            @Override
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
            public State state() {
                return null;
            }

            @Override
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
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
            @SuppressFBWarnings("NP_NONNULL_RETURN_VIOLATION")
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
