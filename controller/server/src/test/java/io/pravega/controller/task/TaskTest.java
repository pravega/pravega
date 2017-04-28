/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.task;

import io.pravega.ClientFactory;
import io.pravega.ReaderGroupManager;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.requests.ControllerEvent;
import io.pravega.controller.server.eventProcessor.AbortEvent;
import io.pravega.controller.server.eventProcessor.AbortEventProcessor;
import io.pravega.controller.server.eventProcessor.CommitEvent;
import io.pravega.controller.server.eventProcessor.CommitEventProcessor;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.ReaderGroup;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.impl.netty.ConnectionFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamAlreadyExistsException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.LockFailedException;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaggedResource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.TestTasks;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.netty.ConnectionFactoryImpl;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Task test cases.
 */
@Slf4j
public class TaskTest {
    private static final String HOSTNAME = "host-1234";
    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
    private final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final StreamMetadataStore streamStore;

    private final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

    private final TaskMetadataStore taskMetadataStore;

    private final TestingServer zkServer;

    private final StreamMetadataTasks streamMetadataTasks;
    private final SegmentHelper segmentHelperMock;
    private final CuratorFramework cli;

    public TaskTest() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        streamStore = StreamStoreFactory.createZKStore(cli, executor);
        taskMetadataStore = TaskStoreFactory.createZKStore(cli, executor);

        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();

        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelperMock,
                executor, HOSTNAME, new ConnectionFactoryImpl(false));
    }

    @Before
    public void setUp() throws ExecutionException, InterruptedException {
        final String stream2 = "stream2";
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scope(SCOPE).streamName(stream2).scalingPolicy(policy2).build();

        // region createStream
        streamStore.createScope(SCOPE).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, stream1, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).join();
        streamStore.createStream(SCOPE, stream2, configuration2, start, null, executor).join();
        streamStore.setState(SCOPE, stream2, State.ACTIVE, null, executor).join();
        // endregion

        // region scaleSegments

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Integer> sealedSegments = Collections.singletonList(1);
        List<Segment> segmentsCreated = streamStore.startScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), start + 20, null, executor).get();
        streamStore.scaleNewSegmentsCreated(SCOPE, stream1, sealedSegments, segmentsCreated, start + 20, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream1, sealedSegments, segmentsCreated, start + 20, null, executor).get();

        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.5);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Integer> sealedSegments1 = Arrays.asList(0, 1, 2);
        segmentsCreated = streamStore.startScale(SCOPE, stream2, sealedSegments1, Arrays.asList(segment3, segment4, segment5), start + 20, null, executor).get();
        streamStore.scaleNewSegmentsCreated(SCOPE, stream2, sealedSegments1, segmentsCreated, start + 20, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream2, sealedSegments1, segmentsCreated, start + 20, null, executor).get();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        cli.close();
        zkServer.stop();
        zkServer.close();
        executor.shutdown();
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException {
        try {
            streamMetadataTasks.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis()).join();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof StreamAlreadyExistsException);
        }

        streamStore.createScope(SCOPE);
        CreateStreamStatus.Status result = streamMetadataTasks.createStream(SCOPE, "dummy", configuration1,
                System.currentTimeMillis()).join();
        assertEquals(result, CreateStreamStatus.Status.SUCCESS);
    }

    @Test
    public void testTaskSweeper() throws ExecutionException, InterruptedException {
        final String deadHost = "deadHost";
        final String deadThreadId = UUID.randomUUID().toString();
        final String scope = SCOPE;
        final String stream = "streamSweeper";
        final StreamConfiguration configuration = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();

        final Resource resource = new Resource(scope, stream);
        final long timestamp = System.currentTimeMillis();
        final TaskData taskData = new TaskData("createStream", "1.0", new Serializable[]{scope, stream, configuration, timestamp});

        for (int i = 0; i < 5; i++) {
            final TaggedResource taggedResource = new TaggedResource(UUID.randomUUID().toString(), resource);
            taskMetadataStore.putChild(deadHost, taggedResource).join();
        }
        final TaggedResource taggedResource = new TaggedResource(deadThreadId, resource);
        taskMetadataStore.putChild(deadHost, taggedResource).join();

        taskMetadataStore.lock(resource, taskData, deadHost, deadThreadId, null, null).join();

        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, HOSTNAME, executor, streamMetadataTasks);
        taskSweeper.sweepOrphanedTasks(deadHost).get();

        Optional<TaskData> data = taskMetadataStore.getTask(resource, deadHost, deadThreadId).get();
        assertFalse(data.isPresent());

        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(deadHost).get();
        assertFalse(child.isPresent());

        // ensure that the stream streamSweeper is created
        StreamConfiguration config = streamStore.getConfiguration(SCOPE, stream, null, executor).get();
        assertTrue(config.getStreamName().equals(configuration.getStreamName()));
        assertTrue(config.getScope().equals(configuration.getScope()));
        assertTrue(config.getScalingPolicy().equals(configuration.getScalingPolicy()));
    }

    @Test(timeout = 5000)
    public void testTaskSweeperNotReady() throws ExecutionException, InterruptedException {
        final String deadHost = "deadHost";
        final String deadThreadId = UUID.randomUUID().toString();

        final Resource resource = new Resource(SCOPE, stream1);
        final TaskData taskData = new TaskData("createTransaction", "1.0",
                new Serializable[]{SCOPE, stream1, 10000, 10000, 10000, null});
        final TaggedResource taggedResource = new TaggedResource(deadThreadId, resource);

        // Create entries for partial task execution on failed host in task metadata store.
        taskMetadataStore.putChild(deadHost, taggedResource).join();
        taskMetadataStore.lock(resource, taskData, deadHost, deadThreadId, null, null).join();

        // Create TaskSweeper instance.
        StreamTransactionMetadataTasks txnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                taskMetadataStore, segmentHelperMock, executor, HOSTNAME, Mockito.mock(ConnectionFactory.class));
        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, HOSTNAME, executor, txnTasks);

        // Start sweeping tasks. This should not complete, since mockTxnTasks object is not yet ready.
        CompletableFuture<Void> future = taskSweeper.sweepOrphanedTasks(deadHost);

        // Timeout should kick in.
        try {
            FutureHelpers.getAndHandleExceptions(future, RuntimeException::new, 500);
            Assert.fail("Failed, task sweeping complete, when timeout exception is expected");
        } catch (TimeoutException e) {
            Assert.assertTrue("Timeout exception expected", true);
        }

        // Now, set mockTxnTasks to ready.
        txnTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(),
                "abortStream", new EventStreamWriterMock<>());

        // Task sweeping should now complete, wait for it.
        future.join();

        // Ensure that a transaction is created, and task store entries are cleaned up.
        Map<UUID, ActiveTxnRecord> map = streamStore.getActiveTxns(SCOPE, stream1, null, executor).join();
        assertEquals(1, map.size());

        Optional<TaskData> data = taskMetadataStore.getTask(resource, deadHost, deadThreadId).get();
        assertFalse(data.isPresent());

        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(deadHost).get();
        assertFalse(child.isPresent());
    }

    @Test(timeout = 10000)
    public void testStreamTaskSweeping() {
        final String stream = "testPartialCreationStream";
        final String deadHost = "deadHost";
        final int initialSegments = 2;
        final ScalingPolicy policy1 = ScalingPolicy.fixed(initialSegments);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        final ArrayList<Integer> sealSegments = new ArrayList<>();
        sealSegments.add(0);
        final ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.25, 0.5));
        final int newSegments = initialSegments - sealSegments.size() + newRanges.size();

        // Create objects.
        StreamMetadataTasks mockStreamTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                segmentHelperMock, executor, deadHost, Mockito.mock(ConnectionFactory.class));
        mockStreamTasks.setCreateIndexOnlyMode();
        TaskSweeper sweeper = new TaskSweeper(taskMetadataStore, HOSTNAME, executor, streamMetadataTasks);

        // Create stream test.
        completePartialTask(mockStreamTasks.createStream(SCOPE, stream, configuration1, System.currentTimeMillis()),
                deadHost, sweeper);
        Assert.assertEquals(initialSegments, streamStore.getActiveSegments(SCOPE, stream, null, executor).join().size());

        // Alter stream test.
        completePartialTask(mockStreamTasks.alterStream(SCOPE, stream, configuration1, null), deadHost, sweeper);

        // Scale test.
        completePartialTask(mockStreamTasks.scale(SCOPE, stream, sealSegments, newRanges,
                System.currentTimeMillis(), null), deadHost, sweeper);
        Assert.assertEquals(newSegments, streamStore.getActiveSegments(SCOPE, stream, null, executor).join().size());

        // Seal stream test.
        completePartialTask(mockStreamTasks.sealStream(SCOPE, stream, null), deadHost, sweeper);
        Assert.assertEquals(0, streamStore.getActiveSegments(SCOPE, stream, null, executor).join().size());

        // Delete stream test.
        completePartialTask(mockStreamTasks.deleteStream(SCOPE, stream, null), deadHost, sweeper);
        List<StreamConfiguration> streams = streamStore.listStreamsInScope(SCOPE).join();
        Assert.assertTrue(streams.stream().allMatch(x -> !x.getStreamName().equals(stream)));
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void testTransactionTaskSweeping() throws CheckpointStoreException, InterruptedException {
        final String deadHost = "deadHost";
        final String commitStream = "commitStream";
        final String abortStream = "abortStream";

        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> mockCommitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> mockAbortWriter = new EventStreamWriterMock<>();

        // Create task and sweeper objects
        StreamTransactionMetadataTasks mockTxnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                taskMetadataStore, segmentHelperMock, executor, deadHost, Mockito.mock(ConnectionFactory.class));
        mockTxnTasks.setCreateIndexOnlyMode();
        mockTxnTasks.initializeStreamWriters(commitStream, mockCommitWriter, abortStream, mockAbortWriter);
        StreamTransactionMetadataTasks txnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                taskMetadataStore, segmentHelperMock, executor, HOSTNAME, Mockito.mock(ConnectionFactory.class));
        txnTasks.initializeStreamWriters(commitStream, mockCommitWriter, abortStream, mockAbortWriter);
        TaskSweeper sweeper = new TaskSweeper(taskMetadataStore, HOSTNAME, executor, txnTasks);

        // Create transaction.
        completePartialTask(mockTxnTasks.createTxn(SCOPE, stream1, 10000, 10000, 10000, null), deadHost, sweeper);

        // Ensure that a transaction is created.
        Map<UUID, ActiveTxnRecord> map = streamStore.getActiveTxns(SCOPE, stream1, null, executor).join();
        assertEquals(1, map.size());
        UUID txId = map.keySet().iterator().next();

        // Abort the transaction
        completePartialTask(mockTxnTasks.abortTxn(SCOPE, stream1, txId, null, null), deadHost, sweeper);
        // Ensure that transactions state is ABORTING.
        TxnStatus status = streamStore.getTransactionData(SCOPE, stream1, txId, null, executor).join().getStatus();
        assertEquals(TxnStatus.ABORTING, status);

        // Create another transaction for committing.
        completePartialTask(mockTxnTasks.createTxn(SCOPE, stream1, 10000, 10000, 10000, null), deadHost, sweeper);

        // Ensure that a transaction is created.
        map = streamStore.getActiveTxns(SCOPE, stream1, null, executor).join();
        assertEquals(2, map.size());
        Optional<UUID> txIdOpt = map.entrySet().stream()
                .filter(e -> e.getValue().getTxnStatus() == TxnStatus.OPEN)
                .map(Map.Entry::getKey)
                .findAny();
        Assert.assertTrue(txIdOpt.isPresent());

        // Commit the transaction.
        UUID txId2 = txIdOpt.get();
        completePartialTask(mockTxnTasks.commitTxn(SCOPE, stream1, txId2, null), deadHost, sweeper);
        // Ensure that transaction state is COMMITTING.
        status = streamStore.getTransactionData(SCOPE, stream1, txId2, null, executor).join().getStatus();
        assertEquals(TxnStatus.COMMITTING, status);

        // Create commit and abort event processors.
        ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
        BlockingQueue<CommitEvent> processedCommitEvents = new LinkedBlockingQueue<>();
        BlockingQueue<AbortEvent> processedAbortEvents = new LinkedBlockingQueue<>();
        createEventProcessor("commitRG", "commitStream", mockCommitWriter.getReader(), mockCommitWriter,
                () -> new CommitEventProcessor(streamStore, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedCommitEvents));
        createEventProcessor("abortRG", "abortStream", mockAbortWriter.getReader(), mockAbortWriter,
                () -> new AbortEventProcessor(streamStore, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedAbortEvents));

        // Wait until the abort event is processed and ensure that the txn state is ABORTED.
        AbortEvent abortEvent = processedAbortEvents.take();
        assertEquals(txId, abortEvent.getTxid());
        status = streamStore.transactionStatus(SCOPE, stream1, txId, null, executor).join();
        assertEquals(TxnStatus.ABORTED, status);

        // Wait until the commit event is processed and ensure that the txn state is COMMITTED.
        CommitEvent commitEvent = processedCommitEvents.take();
        assertEquals(txId2, commitEvent.getTxid());
        status = streamStore.transactionStatus(SCOPE, stream1, txId2, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, status);
    }

    private <T> void completePartialTask(CompletableFuture<T> task, String hostId, TaskSweeper sweeper) {
        AssertExtensions.assertThrows("IllegalStateException expected", task, e -> e instanceof IllegalStateException);
        sweeper.sweepOrphanedTasks(hostId).join();
        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(hostId).join();
        assertFalse(child.isPresent());
    }

    @Test
    public void parallelTaskSweeperTest() throws InterruptedException, ExecutionException {
        final String deadHost = "deadHost";
        final String deadThreadId1 = UUID.randomUUID().toString();
        final String deadThreadId2 = UUID.randomUUID().toString();

        final String scope = SCOPE;
        final String stream1 = "parallelSweeper1";
        final String stream2 = "parallelSweeper2";

        final StreamConfiguration config1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        final StreamConfiguration config2 = StreamConfiguration.builder().scope(SCOPE).streamName(stream2).scalingPolicy(policy1).build();

        final Resource resource1 = new Resource(scope, stream1);
        final long timestamp1 = System.currentTimeMillis();
        final TaskData taskData1 = new TaskData("createStream", "1.0", new Serializable[]{scope, stream1, config1, timestamp1});

        final Resource resource2 = new Resource(scope, stream2);
        final long timestamp2 = System.currentTimeMillis();
        final TaskData taskData2 = new TaskData("createStream", "1.0", new Serializable[]{scope, stream2, config2, timestamp2});

        for (int i = 0; i < 5; i++) {
            final TaggedResource taggedResource = new TaggedResource(UUID.randomUUID().toString(), resource1);
            taskMetadataStore.putChild(deadHost, taggedResource).join();
        }
        final TaggedResource taggedResource1 = new TaggedResource(deadThreadId1, resource1);
        taskMetadataStore.putChild(deadHost, taggedResource1).join();

        final TaggedResource taggedResource2 = new TaggedResource(deadThreadId2, resource2);
        taskMetadataStore.putChild(deadHost, taggedResource2).join();

        taskMetadataStore.lock(resource1, taskData1, deadHost, deadThreadId1, null, null).join();
        taskMetadataStore.lock(resource2, taskData2, deadHost, deadThreadId2, null, null).join();

        final SweeperThread sweeperThread1 = new SweeperThread(HOSTNAME, executor, taskMetadataStore, streamMetadataTasks,
                deadHost);
        final SweeperThread sweeperThread2 = new SweeperThread(HOSTNAME, executor, taskMetadataStore, streamMetadataTasks,
                deadHost);

        sweeperThread1.start();
        sweeperThread2.start();

        sweeperThread1.getResult().join();
        sweeperThread2.getResult().join();

        Optional<TaskData> data = taskMetadataStore.getTask(resource1, deadHost, deadThreadId1).get();
        assertFalse(data.isPresent());

        data = taskMetadataStore.getTask(resource2, deadHost, deadThreadId2).get();
        assertFalse(data.isPresent());

        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(deadHost).get();
        assertFalse(child.isPresent());

        // ensure that the stream streamSweeper is created
        StreamConfiguration config = streamStore.getConfiguration(SCOPE, stream1, null, executor).get();
        assertTrue(config.getStreamName().equals(stream1));

        config = streamStore.getConfiguration(SCOPE, stream2, null, executor).get();
        assertTrue(config.getStreamName().equals(stream2));
    }

    @Test
    public void testLocking() {
        TestTasks testTasks = new TestTasks(taskMetadataStore, executor, HOSTNAME);

        CompletableFuture<Void> first = testTasks.testStreamLock(SCOPE, stream1);
        CompletableFuture<Void> second = testTasks.testStreamLock(SCOPE, stream1);
        try {
            first.getNow(null);
            second.getNow(null);
        } catch (CompletionException ce) {
            assertTrue(ce.getCause() instanceof LockFailedException);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    static class SweeperThread extends Thread {

        private final CompletableFuture<Void> result;
        private final String deadHostId;
        private final TaskSweeper taskSweeper;
        private final String hostId;

        public SweeperThread(String hostId, ScheduledExecutorService executor, TaskMetadataStore taskMetadataStore,
                             StreamMetadataTasks streamMetadataTasks, String deadHostId) {
            this.result = new CompletableFuture<>();
            this.taskSweeper = new TaskSweeper(taskMetadataStore, hostId, executor, streamMetadataTasks);
            this.deadHostId = deadHostId;
            this.hostId = hostId;
        }

        @Override
        public void run() {
            taskSweeper.sweepOrphanedTasks(() -> Collections.singleton(hostId))
                    .whenComplete((value, e) -> {
                        if (e != null) {
                            result.completeExceptionally(e);
                        } else {
                            result.complete(value);
                        }
                    });
        }
    }

    private <T extends ControllerEvent>
    void createEventProcessor(final String readerGroupName,
                              final String streamName,
                              final EventStreamReader<T> reader,
                              final EventStreamWriter<T> writer,
                              Supplier<EventProcessor<T>> factory) throws CheckpointStoreException {
        ClientFactory clientFactory = Mockito.mock(ClientFactory.class);
        Mockito.when(clientFactory.<T>createReader(anyString(), anyString(), any(), any())).thenReturn(reader);
        Mockito.when(clientFactory.<T>createEventWriter(anyString(), any(), any())).thenReturn(writer);

        ReaderGroup readerGroup = Mockito.mock(ReaderGroup.class);
        Mockito.when(readerGroup.getGroupName()).thenReturn(readerGroupName);

        ReaderGroupManager readerGroupManager = Mockito.mock(ReaderGroupManager.class);
        Mockito.when(readerGroupManager.createReaderGroup(anyString(), any(ReaderGroupConfig.class), any()))
                .then(invocation -> readerGroup);

        EventProcessorSystemImpl system = new EventProcessorSystemImpl("system", HOSTNAME, SCOPE, clientFactory, readerGroupManager);

        EventProcessorGroupConfig eventProcessorConfig = EventProcessorGroupConfigImpl.builder()
                .eventProcessorCount(1)
                .readerGroupName(readerGroupName)
                .streamName(streamName)
                .checkpointConfig(CheckpointConfig.periodic(1, 1))
                .build();

        EventProcessorConfig<T> config = EventProcessorConfig.<T>builder()
                .config(eventProcessorConfig)
                .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                .serializer(new JavaSerializer<>())
                .supplier(factory)
                .build();

        system.createEventProcessorGroup(config, CheckpointStoreFactory.createInMemoryStore());
    }
}

