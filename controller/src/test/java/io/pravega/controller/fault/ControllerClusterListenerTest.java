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
package io.pravega.controller.fault;

import com.google.common.collect.Lists;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.Stream.RequestSweeper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.task.Stream.TestTasks;
import io.pravega.controller.task.Stream.TxnSweeper;
import io.pravega.controller.task.TaskSweeper;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * ControllerClusterListener tests.
 */
@Slf4j
public class ControllerClusterListenerTest {

    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource();

    private ScheduledExecutorService executor;
    private ClusterZKImpl clusterZK;

    private LinkedBlockingQueue<String> nodeAddedQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<String> nodeRemovedQueue = new LinkedBlockingQueue<>();

    @Before
    public void setup() throws Exception {

        // 1. Start executor service.
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");

        // 2. start cluster event listener
        clusterZK = new ClusterZKImpl(PRAVEGA_ZK_CURATOR_RESOURCE.client, ClusterType.CONTROLLER);

        clusterZK.addListener((eventType, host) -> {
            switch (eventType) {
                case HOST_ADDED:
                    nodeAddedQueue.offer(host.getHostId());
                    break;
                case HOST_REMOVED:
                    nodeRemovedQueue.offer(host.getHostId());
                    break;
                case ERROR:
                default:
                    break;
            }
        });
    }

    @After
    public void shutdown() throws Exception {
        clusterZK.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 60000L)
    public void clusterListenerTest() throws Exception {
        String hostName = "localhost";
        Host host = new Host(hostName, 10, "host1");

        // Create task sweeper.
        TaskMetadataStore taskStore = TaskStoreFactory.createInMemoryStore(executor);
        TaskSweeper taskSweeper = new TaskSweeper(taskStore, host.getHostId(), executor,
                new TestTasks(taskStore, executor, host.getHostId()));

        // Create txn sweeper.
        @Cleanup
        StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore();
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        StreamTransactionMetadataTasks txnTasks = new StreamTransactionMetadataTasks(streamStore,
                segmentHelper, executor, host.getHostId(), GrpcAuthHelper.getDisabledAuthHelper());
        txnTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());
        TxnSweeper txnSweeper = new TxnSweeper(streamStore, txnTasks, 100, executor);

        // Create ControllerClusterListener.
        ControllerClusterListener clusterListener = new ControllerClusterListener(host, clusterZK, executor,
                Lists.newArrayList(taskSweeper, txnSweeper));
        clusterListener.startAsync();

        clusterListener.awaitRunning();

        Assert.assertTrue(clusterListener.areAllSweepersReady());

        validateAddedNode(host.getHostId());

        // Add a new host
        Host host1 = new Host(hostName, 20, "host2");
        clusterZK.registerHost(host1);
        validateAddedNode(host1.getHostId());

        clusterZK.deregisterHost(host1);
        validateRemovedNode(host1.getHostId());

        clusterListener.stopAsync();

        clusterListener.awaitTerminated();
        Assert.assertFalse(clusterListener.isRunning());
        validateRemovedNode(host.getHostId());
    }

    @Test(timeout = 60000L)
    @SuppressWarnings("unchecked")
    public void clusterListenerStarterTest() throws InterruptedException, ExecutionException {
        String hostName = "localhost";
        Host host = new Host(hostName, 10, "originalhost");
        // Following futures are used as latches. When awaitRunning a sweeper, we wait on a latch by calling
        // Futures.await across the test case.
        // Future for ensuring that task sweeper is ready and we let the sweep happen.
        CompletableFuture<Void> taskSweep = new CompletableFuture<>();
        // Future for when taskSweeper.failedHost is called once
        CompletableFuture<Void> taskHostSweep1 = new CompletableFuture<>();
        // Future for when taskSweeper.failedHost is called second time
        CompletableFuture<Void> taskHostSweep2 = new CompletableFuture<>();
        // Future for txn sweeper to get ready.
        CompletableFuture<Void> txnSweep = new CompletableFuture<>();
        // Future for txnsweeper.failedProcess to be called the first time
        CompletableFuture<Void> txnHostSweepIgnore = new CompletableFuture<>();
        CompletableFuture<Void> txnHostSweep2 = new CompletableFuture<>();
        // Create task sweeper.
        TaskMetadataStore taskStore = TaskStoreFactory.createZKStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        TaskSweeper taskSweeper = spy(new TaskSweeper(taskStore, host.getHostId(), executor,
                new TestTasks(taskStore, executor, host.getHostId())));

        doAnswer(invocation -> {
            if (!taskSweep.isDone()) {
                // we complete the future when this method is called for the first time.
                taskSweep.complete(null);
            }
            return CompletableFuture.completedFuture(null);
        }).when(taskSweeper).sweepFailedProcesses(any(Supplier.class));
        doAnswer(invocation -> {
            if (!taskHostSweep1.isDone()) {
                // we complete this future when task sweeper for a failed host is called for the first time.
                taskHostSweep1.complete(null);
            } else if (!taskHostSweep2.isDone()) {
                // we complete this future when task sweeper for a failed host is called for the second time
                taskHostSweep2.complete(null);
            }
            return CompletableFuture.completedFuture(null);
        }).when(taskSweeper).handleFailedProcess(anyString());

        // Create txn sweeper.
        StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore();
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        // create streamtransactionmetadatatasks but dont initialize it with writers. this will not be
        // ready until writers are supplied.
        StreamTransactionMetadataTasks txnTasks = new StreamTransactionMetadataTasks(streamStore,
                segmentHelper, executor, host.getHostId(), GrpcAuthHelper.getDisabledAuthHelper());

        TxnSweeper txnSweeper = spy(new TxnSweeper(streamStore, txnTasks, 100, executor));
        // any attempt to sweep txnHost should have been ignored
        AtomicBoolean txnSweeperRealMethod = new AtomicBoolean(false);
        doAnswer(invocation -> {
            if (txnSweeperRealMethod.get()) {
                return invocation.callRealMethod();
            }
            txnHostSweepIgnore.complete(null);
            return false;
        }).when(txnSweeper).isReady();

        doAnswer(invocation -> {
            if (!txnSweep.isDone()) {
                txnSweep.complete(null);
            }
            return CompletableFuture.completedFuture(null);
        }).when(txnSweeper).sweepFailedProcesses(any());

        doAnswer(invocation -> {
            if (!txnHostSweep2.isDone()) {
                txnHostSweep2.complete(null);
            }
            return CompletableFuture.completedFuture(null);
        }).when(txnSweeper).handleFailedProcess(anyString());

        // Create request sweeper.
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, mock(BucketStore.class), taskStore, segmentHelper, executor,
                host.getHostId(), GrpcAuthHelper.getDisabledAuthHelper(), mock(EventHelper.class));

        RequestSweeper requestSweeper = spy(new RequestSweeper(streamStore, executor, streamMetadataTasks));
        // any attempt to sweep requests should have been ignored
        CompletableFuture<Void> requestSweep = new CompletableFuture<>();
        // Future for txnsweeper.failedProcess to be called the first time
        CompletableFuture<Void> requestHostSweepIgnore = new CompletableFuture<>();
        CompletableFuture<Void> requestHostSweep2 = new CompletableFuture<>();
        AtomicBoolean requestSweeperRealMethod = new AtomicBoolean(false);
        doAnswer(invocation -> {
            if (requestSweeperRealMethod.get()) {
                return invocation.callRealMethod();
            }
            requestHostSweepIgnore.complete(null);
            return false;
        }).when(requestSweeper).isReady();

        doAnswer(invocation -> {
            if (!requestSweep.isDone()) {
                requestSweep.complete(null);
            }
            return CompletableFuture.completedFuture(null);
        }).when(requestSweeper).sweepFailedProcesses(any());

        doAnswer(invocation -> {
            if (!requestHostSweep2.isDone()) {
                requestHostSweep2.complete(null);
            }
            return CompletableFuture.completedFuture(null);
        }).when(requestSweeper).handleFailedProcess(anyString());

        // Create ControllerClusterListener.
        ControllerClusterListener clusterListener = new ControllerClusterListener(host, clusterZK, executor,
                Lists.newArrayList(taskSweeper, txnSweeper, requestSweeper));

        clusterListener.startAsync();
        clusterListener.awaitRunning();
        log.info("cluster started");
        // ensure that task sweep happens after cluster listener becomes ready.
        assertTrue(Futures.await(taskSweep, 3000));
        log.info("task sweeper completed");

        // ensure only tasks are swept
        verify(taskSweeper, times(1)).sweepFailedProcesses(any(Supplier.class));
        verify(txnSweeper, times(0)).sweepFailedProcesses(any());
        verify(requestSweeper, times(0)).sweepFailedProcesses(any());
        verify(taskSweeper, times(0)).handleFailedProcess(anyString());
        verify(txnSweeper, times(0)).handleFailedProcess(anyString());
        verify(requestSweeper, times(0)).handleFailedProcess(anyString());
        validateAddedNode(host.getHostId());

        log.info("adding new host");

        // now add and remove a new host
        Host newHost = new Host(hostName, 20, "newHost1");
        clusterZK.registerHost(newHost);
        validateAddedNode(newHost.getHostId());
        clusterZK.deregisterHost(newHost);
        validateRemovedNode(newHost.getHostId());
        log.info("deregistering new host");

        assertTrue(Futures.await(taskHostSweep1, 3000));
        assertTrue(Futures.await(txnHostSweepIgnore, 10000));

        log.info("task sweep for new host done");

        // verify that all tasks are not swept again.
        verify(taskSweeper, times(1)).sweepFailedProcesses(any(Supplier.class));
        // verify that host specific sweep happens once.
        verify(taskSweeper, atLeast(1)).handleFailedProcess(anyString());
        // verify that txns are not yet swept as txnsweeper is not yet ready.
        verify(txnSweeper, times(0)).sweepFailedProcesses(any());
        verify(txnSweeper, times(0)).handleFailedProcess(anyString());
        // verify that txn sweeper was checked to be ready. It would have found it not ready at this point
        verify(txnSweeper, atLeast(1)).isReady();

        // request sweeper
        // verify that txns are not yet swept as txnsweeper is not yet ready.
        verify(requestSweeper, times(0)).sweepFailedProcesses(any());
        verify(requestSweeper, times(0)).handleFailedProcess(anyString());
        // verify that txn sweeper was checked to be ready. It would have found it not ready at this point
        verify(requestSweeper, atLeast(1)).isReady();

        // Reset the mock to call real method on txnsweeper.isReady.
        txnSweeperRealMethod.set(true);

        // Complete txn sweeper initialization by adding event writers.
        txnTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());
        txnSweeper.awaitInitialization();

        assertTrue(Futures.await(txnSweep, 3000));

        // verify that post initialization txns are swept. And host specific txn sweep is also performed.
        verify(txnSweeper, times(1)).sweepFailedProcesses(any());

        // Reset the mock to call real method on requestSweeper.isReady.
        requestSweeperRealMethod.set(true);

        // Complete requestSweeper initialization by adding event writers.
        streamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());

        assertTrue(Futures.await(requestSweep, 3000));

        // verify that post initialization requests are swept. And host specific request sweep is also performed.
        verify(requestSweeper, times(1)).sweepFailedProcesses(any());

        // now add another host
        newHost = new Host(hostName, 20, "newHost2");
        clusterZK.registerHost(newHost);
        validateAddedNode(newHost.getHostId());
        clusterZK.deregisterHost(newHost);
        log.info("removing newhost2");

        validateRemovedNode(newHost.getHostId());
        assertTrue(Futures.await(taskHostSweep2, 3000));
        assertTrue(Futures.await(txnHostSweep2, 3000));
        assertTrue(Futures.await(requestHostSweep2, 3000));

        verify(taskSweeper, atLeast(2)).handleFailedProcess(anyString());
        verify(txnSweeper, atLeast(1)).handleFailedProcess(anyString());
        verify(requestSweeper, atLeast(1)).handleFailedProcess(anyString());

        clusterListener.stopAsync();
        clusterListener.awaitTerminated();
    }

    private void validateAddedNode(String host) throws InterruptedException {
        assertEquals(host, nodeAddedQueue.poll(2, TimeUnit.SECONDS));
    }

    private void validateRemovedNode(String host) throws InterruptedException {
        assertEquals(host, nodeRemovedQueue.poll(2, TimeUnit.SECONDS));
    }
}