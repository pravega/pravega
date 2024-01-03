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
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateInitSerializer;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ReaderGroupConfigRejectedException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.ReaderSegmentDistribution;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpointsBefore;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static io.pravega.test.common.AssertExtensions.assertSuppliedFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReaderGroupImplTest {

    private static final String SCOPE = "scope";
    private static final String GROUP_NAME = "readerGroup";
    private ReaderGroupImpl readerGroup;
    @Mock
    private SynchronizerConfig synchronizerConfig;
    @Mock
    private SynchronizerClientFactory clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private ConnectionPool connectionPool;
    @Mock
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @Mock
    private ReaderGroupState state;
    @Mock
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private Serializer<InitialUpdate<ReaderGroupState>> initSerializer = new ReaderGroupStateInitSerializer();
    private Serializer<Update<ReaderGroupState>> updateSerializer = new ReaderGroupStateUpdatesSerializer();
    private final StreamConfiguration configStream = StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build();
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                                                   any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(connectionPool.getInternalExecutor()).thenReturn(scheduledThreadPoolExecutor);
        readerGroup = new ReaderGroupImpl(SCOPE, GROUP_NAME, synchronizerConfig, initSerializer,
                updateSerializer, clientFactory, controller, connectionPool);
    }

    @After
    public void shutDown() {
        readerGroup.close();
        controller.close();
        clientFactory.close();
        connectionPool.close();
        scheduledThreadPoolExecutor.shutdownNow();
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamCutDuplicateStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s1", 3)).build())
        .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamMissingStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s2", 2)).build()).build());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void resetReadersToStreamCut() {
        final UUID rgId = UUID.randomUUID();
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .build();
        config = ReaderGroupConfig.cloneConfig(config, rgId, 0L);
        when(state.getConfig()).thenReturn(config);
        when(synchronizer.getState()).thenReturn(state);
        when(controller.updateReaderGroup(SCOPE, GROUP_NAME, config)).thenReturn(CompletableFuture.completedFuture(1L));

        readerGroup.resetReaderGroup(config);

        verify(synchronizer, times(1)).fetchUpdates();
        verify(controller, times(1)).updateReaderGroup(SCOPE, GROUP_NAME, config);
        verify(synchronizer, times(2)).updateState(any(StateSynchronizer.UpdateGenerator.class));
    }

    @Test
    public void testCancelOutstanding() {
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 12345);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController mkController = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        createScopeAndStream("scope", "stream", mkController);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

        @Cleanup
        SynchronizerClientFactory syncClientFactory = new ClientFactoryImpl("scope", mkController, connectionFactory, streamFactory,
                streamFactory, streamFactory, streamFactory);
        SynchronizerConfig syncConfig = SynchronizerConfig.builder().build();
        Map<SegmentWithRange, Long> segments = new HashMap<>();
        Segment s1 = new Segment("scope", "stream", 1);
        Segment s2 = new Segment("scope", "stream", 2);
        segments.put(new SegmentWithRange(s1, 0.0, 0.5), 1L);
        segments.put(new SegmentWithRange(s2, 0.5, 1.0), 2L);
        createScopeAndStream("scope", NameUtils.getStreamForReaderGroup(GROUP_NAME), mkController);
        readerGroup = new ReaderGroupImpl("scope", GROUP_NAME, syncConfig, initSerializer,
                updateSerializer, syncClientFactory, mkController, connectionPool);

        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        StateSynchronizer<ReaderGroupState> rgStateSynchronizer = readerGroup.getSynchronizer();
        rgStateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(
                ReaderGroupConfig.builder().stream(Stream.of("scope", "stream")).maxOutstandingCheckpointRequest(3).build(), segments, Collections.emptyMap(), false));
        CheckpointState rgState = rgStateSynchronizer.getState().getCheckpointState();
        rgState.beginNewCheckpoint("1", ImmutableSet.of("a", "b"), Collections.emptyMap());
        CompletableFuture<Checkpoint> c1 = readerGroup.initiateCheckpoint("test1", executor);
        rgState.beginNewCheckpoint("2", ImmutableSet.of("a", "b"), Collections.emptyMap());
        CompletableFuture<Checkpoint> c2 = readerGroup.initiateCheckpoint("test2", executor);
        rgState.beginNewCheckpoint("3", ImmutableSet.of("a", "b"), Collections.emptyMap());
        CompletableFuture<Checkpoint> c3 = readerGroup.initiateCheckpoint("test3", executor);
        assertEquals("1", rgState.getCheckpointForReader("a"));
        assertEquals("1", rgState.getCheckpointForReader("b"));
        assertEquals(null, rgState.getCheckpointForReader("c"));
        rgState.readerCheckpointed("1", "a", Collections.emptyMap());
        assertEquals("2", rgState.getCheckpointForReader("a"));
        assertEquals("1", rgState.getCheckpointForReader("b"));
        assertEquals(3, rgState.getOutstandingCheckpoints().size());
        readerGroup.cancelOutstandingCheckpoints();
        assertEquals(0, rgState.getOutstandingCheckpoints().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void resetRGToLastCheckpoint() {
        final UUID readerGroupId = UUID.randomUUID();
        final StreamCut startStreamCut = getStreamCut("s1", 10L, 1, 2);
        ReaderGroupConfig config = ReaderGroupConfig.builder()
                                                    .startFromStreamCuts(ImmutableMap.of(Stream.of(SCOPE, "s1"), startStreamCut))
                                                    .build();
        config = ReaderGroupConfig.cloneConfig(config, readerGroupId, 0L);
        when(state.getPositionsForLastCompletedCheckpoint())
                .thenReturn(Optional.of(ImmutableMap.of(Stream.of(SCOPE, "s1"), startStreamCut.asImpl().getPositions())));

        when(synchronizer.getState().getConfig()).thenReturn(config);
        when(controller.updateReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class))).thenReturn(CompletableFuture.completedFuture(1L));
        readerGroup.resetReaderGroup();

        verify(synchronizer, times(2)).fetchUpdates();
        verify(controller, times(1)).updateReaderGroup(SCOPE, GROUP_NAME, config);
        verify(synchronizer, times(2)).updateState(any(StateSynchronizer.UpdateGenerator.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void resetRGToLastCheckpointNOcheckPoint() {
        final UUID readerGroupId = UUID.randomUUID();
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(2).forEach(segNum -> positions.put(new Segment(SCOPE, "s1", segNum), 0L));
        ReaderGroupConfig config = ReaderGroupConfig.builder()
                                                    .stream(Stream.of("scope", "s1"))
                                                    .build();
        config = ReaderGroupConfig.cloneConfig(config, readerGroupId, 0L);

        when(synchronizer.getState().getConfig()).thenReturn(config);
        when(controller.updateReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class))).thenReturn(CompletableFuture.completedFuture(1L));
        when(controller.getSegmentsAtTime(any(Stream.class), anyLong())).thenReturn(CompletableFuture.completedFuture(positions));
        readerGroup.resetReaderGroup();

        verify(synchronizer, times(2)).fetchUpdates();
        verify(controller, times(1)).updateReaderGroup(SCOPE, GROUP_NAME, config);
        verify(synchronizer, times(2)).updateState(any(StateSynchronizer.UpdateGenerator.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void resetReaderGroupMigration() {
        final UUID rgId = UUID.randomUUID();
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .build();
        ReaderGroupConfig expectedConfig = ReaderGroupConfig.cloneConfig(config, rgId, 0L);
        when(state.getConfig()).thenReturn(config);
        when(synchronizer.getState()).thenReturn(state);
        when(controller.createReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedConfig));
        when(controller.updateReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class))).thenReturn(CompletableFuture.completedFuture(1L));

        readerGroup.resetReaderGroup(config);

        verify(synchronizer, times(1)).fetchUpdates();
        verify(controller, times(1)).updateReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class));
        verify(controller, times(1)).createReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class));
        verify(synchronizer, times(2)).updateState(any(StateSynchronizer.UpdateGenerator.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void resetReadersToCheckpoint() {
        final UUID readerGroupId = UUID.randomUUID();
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(2).forEach(segNum -> positions.put(new Segment(SCOPE, "s1", segNum), 10L));
        Checkpoint checkpoint = new CheckpointImpl("testChkPoint", positions);
        ReaderGroupConfig config = ReaderGroupConfig.builder()
                .startFromCheckpoint(checkpoint).build();
        config = ReaderGroupConfig.cloneConfig(config, readerGroupId, 0L);
        when(state.getConfig()).thenReturn(config);
        when(synchronizer.getState()).thenReturn(state);
        when(controller.updateReaderGroup(SCOPE, GROUP_NAME, config)).thenReturn(CompletableFuture.completedFuture(1L));

        readerGroup.resetReaderGroup(config);

        verify(synchronizer, times(1)).fetchUpdates();
        verify(controller, times(1)).updateReaderGroup(SCOPE, GROUP_NAME, config);
        verify(synchronizer, times(2)).updateState(any(StateSynchronizer.UpdateGenerator.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void laggingResetReaderGroup() {
        UUID rgId = UUID.randomUUID();
        ReaderGroupConfig config1 = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .build();
        config1 = ReaderGroupConfig.cloneConfig(config1, rgId, 0L);
        ReaderGroupConfig config2 = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s3"), createStreamCut("s3", 2))
                .put(createStream("s4"), createStreamCut("s4", 3)).build())
                .build();
        config2 = ReaderGroupConfig.cloneConfig(config2, rgId, 1L);
        when(state.getConfig()).thenReturn(config1, config2);
        when(synchronizer.getState()).thenReturn(state);

        CompletableFuture<Long> badFuture = new CompletableFuture<>();
        badFuture.completeExceptionally(new ReaderGroupConfigRejectedException("handle"));
        // The controller has config2
        when(controller.updateReaderGroup(eq(SCOPE), eq(GROUP_NAME), argThat(new ReaderGroupConfigMatcher(config1)))).thenReturn(badFuture);
        when(controller.updateReaderGroup(eq(SCOPE), eq(GROUP_NAME),
                                          argThat(new ReaderGroupConfigMatcher(ReaderGroupConfig.cloneConfig(config1, config1.getReaderGroupId(), config2.getGeneration())))))
                .thenReturn(CompletableFuture.completedFuture(2L));
        when(controller.getReaderGroupConfig(SCOPE, GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(config2));

        readerGroup.resetReaderGroup(config1);

        verify(synchronizer, times(1)).fetchUpdates();
        verify(controller, times(1)).updateReaderGroup(eq(SCOPE), eq(GROUP_NAME), argThat(new ReaderGroupConfigMatcher(config1)));
        verify(controller, times(1)).updateReaderGroup(eq(SCOPE), eq(GROUP_NAME), 
            argThat(new ReaderGroupConfigMatcher(ReaderGroupConfig.cloneConfig(config1, config1.getReaderGroupId(), config2.getGeneration()))));
        verify(controller, times(1)).getReaderGroupConfig(SCOPE, GROUP_NAME);
        verify(synchronizer, times(4)).updateState(any(StateSynchronizer.UpdateGenerator.class));
    }

    @Test(timeout = 10000L)
    @SuppressWarnings("unchecked")
    public void testAsyncResetReaderGroup() {
        UUID rgId = UUID.randomUUID();
        ReaderGroupConfig config1 = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2)).build())
                .build();
        config1 = ReaderGroupConfig.cloneConfig(config1, rgId, 0L);
        ReaderGroupConfig config2 = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s2"), createStreamCut("s2", 2)).build())
                .build();
        config2 = ReaderGroupConfig.cloneConfig(config2, rgId, 0L);
        ReaderGroupConfig config3 = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s3"), createStreamCut("s3", 2)).build())
                .build();
        config3 = ReaderGroupConfig.cloneConfig(config3, rgId, 0L);

        AtomicInteger x = new AtomicInteger(0);
        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();
        // The client's config.
        AtomicReference<ReaderGroupConfig> atomicConfig = new AtomicReference<>(config1);
        // The controller's config.
        AtomicReference<ReaderGroupConfig> atomicConfigController = new AtomicReference<>(config1);
        // return the client's config when calling state.getConfig.
        doAnswer(a -> atomicConfig.get()).when(state).getConfig();
        when(synchronizer.getState()).thenReturn(state);
        // return controllerConfig when calling getReaderGroupConfig.
        doAnswer(a -> CompletableFuture.completedFuture(atomicConfigController.get())).when(controller).getReaderGroupConfig(anyString(), anyString());
        // update the client config to the controller config whenever we update the StateSync.
        doAnswer(a -> {
            atomicConfig.set(atomicConfigController.get());
            return null;
        }).when(synchronizer).updateState(any(StateSynchronizer.UpdateGenerator.class));
        // update the controller config with the incremented generation if the generations match.
        doAnswer(a -> {
            ReaderGroupConfig c = a.getArgument(2);
            // the first one to call needs to wait until the second call has been completed.
            if (x.getAndIncrement() == 0) {
                signal.complete(null);
                wait.join();
            }
            if (c.getGeneration() == atomicConfigController.get().getGeneration()) {
                long incGen = c.getGeneration() + 1;
                atomicConfigController.set(ReaderGroupConfig.cloneConfig(c, c.getReaderGroupId(), incGen));
                return CompletableFuture.completedFuture(incGen);
            } else {
                CompletableFuture<Long> badFuture = new CompletableFuture<>();
                badFuture.completeExceptionally(new ReaderGroupConfigRejectedException("handle"));
                return badFuture;
            }
        }).when(controller).updateReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class));

        // run the first call async.
        final ReaderGroupConfig newConf = config2;
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> readerGroup.resetReaderGroup(newConf));
        // Once the first call has reached the controller.updateReaderGroup step then signal so he waits.
        signal.join();
        // start the second call.
        readerGroup.resetReaderGroup(config3);
        // Once the second is completed stop the wait so the first call can go ahead.
        wait.complete(null);
        // wait for the first call to complete.
        assertTrue(Futures.await(future));

        // assert the generation was incremented twice due to two updates.
        assertEquals(2L, atomicConfig.get().getGeneration());
        assertEquals(2L, atomicConfigController.get().getGeneration());
        // assert the first call happened last and the streams are s2.
        assertTrue(atomicConfig.get().getStartingStreamCuts().keySet().stream().anyMatch(s -> s.getStreamName().equals("s2")));
        assertTrue(atomicConfigController.get().getStartingStreamCuts().keySet().stream().anyMatch(s -> s.getStreamName().equals("s2")));
    }

    @Test
    public void getUnreadBytesBasedOnLastCheckpointPosition() {
        final String stream = "s1";
        final StreamCut startStreamCut = getStreamCut(stream, 10L, 1, 2);
        final StreamCut endStreamCut = getStreamCut(stream, 25L, 1, 2);
        //setup mocks
        when(state.getPositionsForLastCompletedCheckpoint())
                .thenReturn(Optional.of(ImmutableMap.of(Stream.of(SCOPE, stream), startStreamCut.asImpl().getPositions())));
        when(state.getEndSegments()).thenReturn(endStreamCut.asImpl().getPositions());
        when(synchronizer.getState()).thenReturn(state);
        ImmutableSet<Segment> segmentSet = ImmutableSet.<Segment>builder()
                .addAll(startStreamCut.asImpl().getPositions().keySet()).addAll(endStreamCut.asImpl().getPositions().keySet()).build();
        when(controller.getSegments(startStreamCut, endStreamCut))
                .thenReturn(CompletableFuture.completedFuture(new StreamSegmentSuccessors(segmentSet, "")));

        assertEquals(30L, readerGroup.unreadBytes());
    }

    @Test
    public void getUnreadBytesBasedOnLastPosition() {
        final String stream = "s1";
        final StreamCut startStreamCut = getStreamCut(stream, 10L, 1, 2);
        final StreamCut endStreamCut = getStreamCut(stream, 30L, 1, 2);

        //setup mocks
        when(state.getPositionsForLastCompletedCheckpoint()).thenReturn(Optional.empty()); // simulate zero checkpoints.
        Map<SegmentWithRange, Long> positions = startStreamCut.asImpl()
                                                              .getPositions()
                                                              .entrySet()
                                                              .stream()
                                                              .collect(Collectors.toMap(e -> new SegmentWithRange(e.getKey(), null),
                                                                                        e -> e.getValue()));
        when(state.getPositions()).thenReturn(ImmutableMap.of(Stream.of(SCOPE, stream), positions));
        when(state.getEndSegments()).thenReturn(endStreamCut.asImpl().getPositions());
        when(synchronizer.getState()).thenReturn(state);
        ImmutableSet<Segment> r = ImmutableSet.<Segment>builder()
                                              .addAll(startStreamCut.asImpl().getPositions().keySet())
                                              .addAll(endStreamCut.asImpl().getPositions().keySet())
                                              .build();
        when(controller.getSegments(startStreamCut,
                                    endStreamCut)).thenReturn(CompletableFuture.completedFuture(new StreamSegmentSuccessors(r, "")));

        assertEquals(40L, readerGroup.unreadBytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void initiateCheckpointFailure() {
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(false);
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", scheduledThreadPoolExecutor);
        assertTrue("expecting a checkpoint failure", result.isCompletedExceptionally());
        try {
            result.get();
        } catch (InterruptedException | ExecutionException e) {
            assertTrue("expecting MaxNumberOfCheckpointsExceededException", e.getCause() instanceof MaxNumberOfCheckpointsExceededException);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void initiateCheckpointSuccess() {
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(true);
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", scheduledThreadPoolExecutor);
        assertFalse("not expecting a checkpoint failure", result.isCompletedExceptionally());
    }

    @Test
    public void initiateCheckpointSuccessWithEmptyPositionMap() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(true);
        when(state.isCheckpointComplete("test")).thenReturn(false).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            completed.set(true);
            return null;
        }).when(synchronizer).updateStateUnconditionally(eq(new ClearCheckpointsBefore("test")));
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", executor);
        assertFalse(result.isDone());
        Checkpoint cp = result.get(5, TimeUnit.SECONDS);
        assertTrue(result.isDone());
        assertEquals("test", cp.asImpl().getName());
        assertEquals(Collections.EMPTY_MAP, cp.asImpl().getPositions());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void initiateCheckpointInternalExecutorSuccess() {
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(true);
        when(connectionPool.getInternalExecutor()).thenReturn(scheduledThreadPoolExecutor);
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("internalExecutor");
        assertFalse("not expecting a checkpoint failure", result.isCompletedExceptionally());
    }

    @Test(timeout = 10000)
    public void generateStreamCutSuccess() {
        when(synchronizer.getState()).thenReturn(state);
        when(state.isCheckpointComplete(any(String.class))).thenReturn(false).thenReturn(true);
        when(state.getStreamCutsForCompletedCheckpoint(anyString())).thenReturn(Optional.of(ImmutableMap.of(createStream("s1"),
                                                                                                                            createStreamCut("s1", 2))));
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        CompletableFuture<Map<Stream, StreamCut>> result = readerGroup.generateStreamCuts(executor);
        assertEquals(createStreamCut("s1", 2), result.join().get(createStream("s1")));
    }

    @Test(timeout = 10000)
    public void generateStreamCutsError() {
        when(synchronizer.getState()).thenReturn(state);
        when(state.isCheckpointComplete(any(String.class))).thenReturn(true);
        when(state.getStreamCutsForCompletedCheckpoint(anyString())).thenReturn(Optional.empty()); //mock empty.
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        CompletableFuture<Map<Stream, StreamCut>> result = readerGroup.generateStreamCuts(executor);
        assertSuppliedFutureThrows("CheckpointFailedException is expected", () -> readerGroup.generateStreamCuts(executor),
                     t -> t instanceof CheckpointFailedException);
    }

    @Test(timeout = 1000)
    public void getEndSegmentsForStream() {
        Map<Segment, Long> endSegmentMap = ReaderGroupImpl.getEndSegmentsForStreams(ReaderGroupConfig.builder().stream(Stream.of(SCOPE, "s1"),
                                                                                                            getStreamCut("s1", 0L, 0),
                                                                                                            getStreamCut("s1", -1L, 0))
                                                                                          .build());
        assertEquals(Long.MAX_VALUE, endSegmentMap.get(new Segment(SCOPE, "s1", 0)).longValue());
    }

        @Test(timeout = 10000)
    public void initiateCheckpointOutstandingCheckPoint() throws Exception {
            PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 12345);
            MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
            MockController mkController = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
            createScopeAndStream("scope", "stream", mkController);
            MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

            @Cleanup
            SynchronizerClientFactory syncClientFactory = new ClientFactoryImpl("scope", mkController, connectionFactory, streamFactory,
                    streamFactory, streamFactory, streamFactory);
            SynchronizerConfig syncConfig = SynchronizerConfig.builder().build();
            Map<SegmentWithRange, Long> segments = new HashMap<>();
            Segment s1 = new Segment("scope", "stream", 1);
            Segment s2 = new Segment("scope", "stream", 2);
            segments.put(new SegmentWithRange(s1, 0.0, 0.5), 1L);
            segments.put(new SegmentWithRange(s2, 0.5, 1.0), 2L);
            createScopeAndStream("scope", NameUtils.getStreamForReaderGroup(GROUP_NAME), mkController);
            readerGroup = new ReaderGroupImpl("scope", GROUP_NAME, syncConfig, initSerializer,
                    updateSerializer, syncClientFactory, mkController, connectionPool);

            @Cleanup("shutdown")
            InlineExecutor executor = new InlineExecutor();
            StateSynchronizer<ReaderGroupState> rgStateSynchronizer = readerGroup.getSynchronizer();
            rgStateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(
                    ReaderGroupConfig.builder().stream(Stream.of("scope", "stream")).maxOutstandingCheckpointRequest(1).build(), segments, Collections.emptyMap(), false));
            CheckpointState rgState = rgStateSynchronizer.getState().getCheckpointState();
            rgState.beginNewCheckpoint("1", ImmutableSet.of("a", "b"), Collections.emptyMap());
            CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", executor);
            assertTrue("expecting a checkpoint failure", result.isCompletedExceptionally());
            AssertExtensions.assertThrows("", result::get, e -> e instanceof MaxNumberOfCheckpointsExceededException);
        }

    private void createScopeAndStream(String scope, String stream, MockController controller) {
        controller.createScope(scope).join();
        controller.createStream(scope, stream, configStream).join();
    }

    private StreamCut createStreamCut(String streamName, int numberOfSegments) {
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(numberOfSegments).forEach(segNum -> positions.put(new Segment(SCOPE, streamName, segNum), 10L));
        return new StreamCutImpl(createStream(streamName), positions);
    }

    private Stream createStream(String streamName) {
        return Stream.of(SCOPE, streamName);
    }

    private StreamCut getStreamCut(String streamName, long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segmentNumbers).forEach(seg -> {
            builder.put(new Segment(SCOPE, streamName, seg), offset);
        });

        return new StreamCutImpl(Stream.of(SCOPE, streamName), builder.build());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFutureCancelation() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(true);
        when(state.isCheckpointComplete("test")).thenReturn(false).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            completed.set(true);
            return null;
        }).when(synchronizer).updateStateUnconditionally(eq(new ClearCheckpointsBefore("test")));
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", executor);
        assertFalse(result.isDone());
        result.cancel(false);
        AssertExtensions.assertEventuallyEquals(true, completed::get, 5000);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void readerGroupSegmentDistribution() {
        ReaderGroupState state = mock(ReaderGroupState.class);
        when(synchronizer.getState()).thenReturn(state);

        Set<String> readers = new HashSet<>();
        readers.add("1");
        readers.add("2");
        readers.add("3");
        when(state.getOnlineReaders()).thenReturn(readers);

        SegmentWithRange segment = mock(SegmentWithRange.class);
        Map<SegmentWithRange, Long> map = Collections.singletonMap(segment, 0L);
        when(state.getAssignedSegments(anyString())).thenReturn(map);

        when(state.getNumberOfUnassignedSegments()).thenReturn(2);

        ReaderSegmentDistribution readerSegmentDistribution = readerGroup.getReaderSegmentDistribution();

        Map<String, Integer> distribution = readerSegmentDistribution.getReaderSegmentDistribution();
        assertEquals(3, distribution.size());
        assertTrue(distribution.containsKey("1"));
        assertTrue(distribution.containsKey("2"));
        assertTrue(distribution.containsKey("3"));
        assertEquals(2, readerSegmentDistribution.getUnassignedSegments());
        assertEquals(1, distribution.get("1").intValue());
        assertEquals(1, distribution.get("2").intValue());
        assertEquals(1, distribution.get("3").intValue());
    }

    @Test
    public void updateRetentionStreamCutTestSuccess() {
        final UUID rgId = UUID.randomUUID();
        Stream test = createStream("test");
        ReaderGroupState state = mock(ReaderGroupState.class);
        when(synchronizer.getState()).thenReturn(state);
        ReaderGroupConfig config = ReaderGroupConfig.builder().stream(test)
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        config = ReaderGroupConfig.cloneConfig(config, rgId, 0L);
        when(synchronizer.getState().getConfig()).thenReturn(config);
        when(controller.updateSubscriberStreamCut(test.getScope(), test.getStreamName(), NameUtils.getScopedReaderGroupName(SCOPE, GROUP_NAME),
                config.getReaderGroupId(), 0L, createStreamCut("test", 1)))
                .thenReturn(CompletableFuture.completedFuture(true));
        Map<Stream, StreamCut> cuts = new HashMap<>();
        cuts.put(test, createStreamCut("test", 1));
        readerGroup.updateRetentionStreamCut(cuts);
        verify(controller, times(1))
                .updateSubscriberStreamCut(test.getScope(), test.getStreamName(), NameUtils.getScopedReaderGroupName(SCOPE, GROUP_NAME),
                        config.getReaderGroupId(), 0L, createStreamCut("test", 1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void updateRetentionStreamCutTestFailure() {
        Stream test = createStream("test");
        ReaderGroupState state = mock(ReaderGroupState.class);
        when(synchronizer.getState()).thenReturn(state);
        ReaderGroupConfig config = ReaderGroupConfig.builder().stream(test)
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT).build();
        config = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        when(synchronizer.getState().getConfig()).thenReturn(config);
        Map<Stream, StreamCut> cuts = new HashMap<>();
        cuts.put(test, createStreamCut("test", 1));
        readerGroup.updateRetentionStreamCut(cuts);
    }

    private static class ReaderGroupConfigMatcher implements ArgumentMatcher<ReaderGroupConfig> {
        private final ReaderGroupConfig expected;

        public ReaderGroupConfigMatcher(ReaderGroupConfig expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(ReaderGroupConfig actual) {
            if (actual == null) {
                return false;
            }
            return actual.equals(expected) && actual.getGeneration() == expected.getGeneration()
                    && actual.getReaderGroupId().equals(expected.getReaderGroupId());
        }
    }
}
