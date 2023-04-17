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
package io.pravega.client.admin.impl;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.ConfigMismatchException;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderGroupNotFoundException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.ObjectClosedException;
import io.pravega.shared.NameUtils;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class ReaderGroupManagerImplTest {
    private static final String SCOPE = "scope";
    private static final String GROUP_NAME = "readerGroup";
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(30);

    @Mock
    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private ReaderGroupManagerImpl readerGroupManager;

    @Mock
    private ClientFactoryImpl clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @Mock
    private ReaderGroupState state;
    @Mock
    private ConnectionPool pool;

    @Before
    public void setUp() throws Exception {
        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        when(clientFactory.getConnectionPool()).thenReturn(pool);
    }

    @After
    public void shutDown() {
        synchronizer.close();
        controller.close();
        clientFactory.close();
    }

    @Test
    public void testCreateReaderGroup() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        ReaderGroupConfig expectedConfig = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(controller.createReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedConfig));
        mockConnectionPoolExecutor(false);
        // Create a ReaderGroup
        boolean created = readerGroupManager.createReaderGroup(GROUP_NAME, config);
        assertTrue(created);
        verify(clientFactory, times(1)).createStateSynchronizer(anyString(), any(Serializer.class),
                any(Serializer.class), any(SynchronizerConfig.class));
        verify(synchronizer, times(1)).initialize(any(InitialUpdate.class));
    }

    @Test
    public void testCreateReaderGroupWithNewConfig() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                                                                                               .put(createStream("s2"), createStreamCut("s2", 0)).build())
                                                    .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                                                    .build();
        ReaderGroupConfig expectedConfig = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        when(controller.createReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedConfig));
        mockConnectionPoolExecutor(false);
        // Create a ReaderGroup
        ReaderGroupConfig newConfig = ReaderGroupConfig.builder()
                                                       .stream(createStream("s1"), createStreamCut("s1", 2))
                                                       .build();
        assertThrows(ConfigMismatchException.class, () -> readerGroupManager.createReaderGroup(GROUP_NAME, newConfig));
        verify(clientFactory, never()).createStateSynchronizer(anyString(), any(Serializer.class),
                                                                any(Serializer.class), any(SynchronizerConfig.class));
        Map<SegmentWithRange, Long> segments = ImmutableMap.<SegmentWithRange, Long>builder()
                                                           .put(new SegmentWithRange(new Segment(SCOPE, "s2", 0), 0.0, 1.0), 10L).build();
        ReaderGroupState.ReaderGroupStateInit initState = new ReaderGroupState.ReaderGroupStateInit(expectedConfig, segments, new HashMap<>(), false);
        verify(synchronizer, never()).initialize(initState);
    }

    @Test(expected = ReaderGroupNotFoundException.class)
    public void testMissingGetReaderGroup() {

        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                                                   any(SynchronizerConfig.class))).thenThrow(new InvalidStreamException("invalid RG stream"));
        when(pool.getInternalExecutor()).thenReturn(scheduledThreadPoolExecutor);
        readerGroupManager.getReaderGroup(GROUP_NAME);
    }

    @Test
    public void testCreateReaderGroupWithSameConfig() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                                                                                               .put(createStream("s1"), createStreamCut("s1", 2))
                                                                                               .put(createStream("s2"), createStreamCut("s2", 3)).build())
                                                    .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                                                    .build();
        ReaderGroupConfig expectedConfig = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 1L);
        when(controller.createReaderGroup(anyString(), anyString(), any(ReaderGroupConfig.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedConfig));
        mockConnectionPoolExecutor(false);
        // Create a ReaderGroup
        boolean created = readerGroupManager.createReaderGroup(GROUP_NAME, config);
        assertFalse(created);
        verify(clientFactory, never()).createStateSynchronizer(anyString(), any(Serializer.class),
                                                                any(Serializer.class), any(SynchronizerConfig.class));
        verify(synchronizer, never()).initialize(any(InitialUpdate.class));
    }

    @Test
    public void testDeleteReaderGroup() {
        final UUID rgId = UUID.randomUUID();
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        config = ReaderGroupConfig.cloneConfig(config, rgId, 0L);
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(config);
        when(controller.deleteReaderGroup(SCOPE, GROUP_NAME, config.getReaderGroupId())).thenReturn(CompletableFuture.completedFuture(true));
        mockConnectionPoolExecutor(false);
        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);
        verify(controller, times(1)).deleteReaderGroup(SCOPE, GROUP_NAME, config.getReaderGroupId());
    }

    @Test
    public void testDeleteReaderGroupRGStreamDeleted() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenThrow(new InvalidStreamException(""));
        when(controller.getReaderGroupConfig(SCOPE, GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(config));
        when(controller.deleteReaderGroup(SCOPE, GROUP_NAME, config.getReaderGroupId()))
                .thenReturn(CompletableFuture.completedFuture(true));
        mockConnectionPoolExecutor(false);
        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);
        verify(controller, times(1)).getReaderGroupConfig(SCOPE, GROUP_NAME);
        verify(controller, times(1)).deleteReaderGroup(SCOPE, GROUP_NAME, config.getReaderGroupId());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteRGMigrationConfigOnController() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(config);

        ReaderGroupConfig expectedConfig = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);

        when(controller.getReaderGroupConfig(anyString(), anyString()))
               .thenReturn(CompletableFuture.completedFuture(expectedConfig));
        when(controller.deleteReaderGroup(anyString(), anyString(), any(UUID.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        mockConnectionPoolExecutor(false);
        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);
        verify(controller, times(1)).getReaderGroupConfig(SCOPE, GROUP_NAME);
        verify(controller, times(1)).deleteReaderGroup(SCOPE, GROUP_NAME, expectedConfig.getReaderGroupId());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeleteRGMigrationNoConfigOnController() {
        ReaderGroupConfig config = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .build();
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(config);

        ReaderGroupConfig expectedConfig = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);

        when(controller.getReaderGroupConfig(SCOPE, GROUP_NAME))
                .thenThrow(new ReaderGroupNotFoundException(NameUtils.getScopedReaderGroupName(SCOPE, GROUP_NAME)));
        when(controller.sealStream(SCOPE, NameUtils.getStreamForReaderGroup(GROUP_NAME)))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(controller.deleteStream(SCOPE, NameUtils.getStreamForReaderGroup(GROUP_NAME)))
                .thenReturn(CompletableFuture.completedFuture(true));
        mockConnectionPoolExecutor(false);
        // Delete ReaderGroup
        readerGroupManager.deleteReaderGroup(GROUP_NAME);
        verify(controller, times(1)).getReaderGroupConfig(SCOPE, GROUP_NAME);
        verify(controller, times(1)).deleteStream(SCOPE, NameUtils.getStreamForReaderGroup(GROUP_NAME));
        verify(controller, times(0)).deleteReaderGroup(SCOPE, GROUP_NAME, expectedConfig.getReaderGroupId());
    }

    @Test
    public void testCreateReaderGroupManager() {
        ClientConfig config = ClientConfig.builder().controllerURI(URI.create("tls://localhost:9090")).build();
        @Cleanup
        ReaderGroupManagerImpl readerGroupMgr = (ReaderGroupManagerImpl) ReaderGroupManager.withScope(SCOPE, config);
        ClientFactoryImpl factory = (ClientFactoryImpl) readerGroupMgr.getClientFactory();
        ConnectionPoolImpl cp = (ConnectionPoolImpl) factory.getConnectionPool();
        assertEquals(1, cp.getClientConfig().getMaxConnectionsPerSegmentStore());
        assertEquals(config.isEnableTls(), cp.getClientConfig().isEnableTls());
    }

    @Test(expected = ObjectClosedException.class)
    public void testWhenExecutorIsUnavailable() {
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                any(SynchronizerConfig.class))).thenThrow(new InvalidStreamException("invalid RG stream"));
        mockConnectionPoolExecutor(true);
        readerGroupManager.getReaderGroup(GROUP_NAME);
    }

    private void mockConnectionPoolExecutor(boolean value) {
        when(pool.getInternalExecutor()).thenReturn(scheduledThreadPoolExecutor);
        when(clientFactory.getConnectionPool().getInternalExecutor().isShutdown()).thenReturn(value);
    }
    
    private StreamCut createStreamCut(String streamName, int numberOfSegments) {
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(numberOfSegments).forEach(segNum -> positions.put(new Segment(SCOPE, streamName, segNum), 10L));
        return new StreamCutImpl(createStream(streamName), positions);
    }

    private Stream createStream(String streamName) {
        return Stream.of(SCOPE, streamName);
    }
}
