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
package io.pravega.controller.store.stream;

import com.google.common.collect.Lists;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StateRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

public class StreamTest extends ThreadPooledTestSuite {
    private TestingServer zkTestServer;
    private CuratorFramework cli;
    private ZkOrderedStore orderer;

    @Override
    protected int getThreadPoolSize() {
        return 10;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        orderer = new ZkOrderedStore("txnOrderer", new ZKStoreHelper(cli, executorService()), executorService());
    }

    @After
    public void tearDown() throws Exception {
        cli.close();
        zkTestServer.close();
    }

    @Test(timeout = 10000)
    public void testPravegaTablesCreateStream() throws ExecutionException, InterruptedException {
        PravegaTablesStoreHelper storeHelper = new PravegaTablesStoreHelper(
                SegmentHelperMock.getSegmentHelperMockForTables(executorService()), GrpcAuthHelper.getDisabledAuthHelper(), executorService());
        PravegaTablesScope scope = new PravegaTablesScope("test", storeHelper);
        scope.createScope().join();
        scope.addStreamToScope("test").join();

        PravegaTablesStream stream = new PravegaTablesStream("test", "test",
                storeHelper, orderer, () -> 0,
                scope::getStreamsInScopeTableName, executorService());
        testStream(stream);
    }
    
    @Test(timeout = 10000)
    public void testZkCreateStream() throws ExecutionException, InterruptedException {
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(cli, executorService());
        ZkOrderedStore orderer = new ZkOrderedStore("txn", zkStoreHelper, executorService());
        ZKStream zkStream = new ZKStream("test", "test", zkStoreHelper, executorService(), orderer);
        testStream(zkStream);
    }

    @Test(timeout = 10000)
    public void testInMemoryCreateStream() throws ExecutionException, InterruptedException {
        InMemoryStream stream = new InMemoryStream("test", "test");
        testStream(stream);
    }

    private void testStream(PersistentStreamBase stream) throws InterruptedException, ExecutionException {
        long creationTime1 = System.currentTimeMillis();
        long creationTime2 = creationTime1 + 1;
        final ScalingPolicy policy1 = ScalingPolicy.fixed(5);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(6);
        final int startingSegmentNumber = 0;
        final StreamConfiguration streamConfig1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        final StreamConfiguration streamConfig2 = StreamConfiguration.builder().scalingPolicy(policy2).build();

        CreateStreamResponse response = stream.checkStreamExists(streamConfig1, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        stream.createStreamMetadata().join();
        stream.storeCreationTimeIfAbsent(creationTime1).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());

        stream.createConfigurationIfAbsent(StreamConfigurationRecord.complete("test", "test", streamConfig1)).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.createStateIfAbsent(StateRecord.builder().state(State.UNKNOWN).build()).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.updateState(State.CREATING).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.updateState(State.ACTIVE).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());

        stream.updateState(State.SEALING).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2, startingSegmentNumber).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
    }

    @Test(timeout = 10000)
    public void testConcurrentGetSuccessorScaleZk() throws Exception {
        try (final StreamMetadataStore store = new ZKStreamMetadataStore(cli, executorService())) {
            ZKStoreHelper zkStoreHelper = new ZKStoreHelper(cli, executorService());
            testConcurrentGetSuccessorScale(store, (x, y) -> new ZKStream(x, y, zkStoreHelper, executorService(), orderer));
        }
    }

    @Test(timeout = 10000)
    public void testConcurrentGetSuccessorScalePravegaTables() throws Exception {
        @Cleanup
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executorService());
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        try (final StreamMetadataStore store = new PravegaTablesStreamMetadataStore(
                segmentHelper, cli, executorService(), authHelper)) {

            testConcurrentGetSuccessorScale(store, (x, y) -> {
                PravegaTablesStoreHelper storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executorService());
                PravegaTablesScope scope = new PravegaTablesScope(x, storeHelper);
                Futures.exceptionallyExpecting(scope.createScope(), e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException, null).join();
                scope.addStreamToScope(y).join();
                return new PravegaTablesStream(x, y, storeHelper, orderer, () -> 0, scope::getStreamsInScopeTableName, executorService());
            });
        }
    }
    
    private void testConcurrentGetSuccessorScale(StreamMetadataStore store, BiFunction<String, String, Stream> createStream) throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final String streamName = "test";
        String scopeName = "test";
        store.createScope(scopeName).get();

        Stream stream = spy(createStream.apply(scopeName, streamName));

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(policy)
                .build();

        store.createStream(scopeName, streamName, streamConfig, System.currentTimeMillis(), null, executorService()).get();
        store.setState(scopeName, streamName, State.ACTIVE, null, executorService()).get();

        List<Map.Entry<Double, Double>> newRanges;

        newRanges = Arrays.asList(new AbstractMap.SimpleEntry<>(0.0, 0.5), new AbstractMap.SimpleEntry<>(0.5, 1.0));

        long scale = System.currentTimeMillis();
        ArrayList<Long> sealedSegments = Lists.newArrayList(0L);
        long one = NameUtils.computeSegmentId(1, 1);
        long two = NameUtils.computeSegmentId(2, 1);
        VersionedMetadata<EpochTransitionRecord> response = stream.submitScale(sealedSegments, newRanges, scale, null).join();
        Map<Long, Map.Entry<Double, Double>> newSegments = response.getObject().getNewSegmentsWithRange();
        VersionedMetadata<State> state = stream.getVersionedState().join();
        state = stream.updateVersionedState(state, State.SCALING).join();
        stream.startScale(false, response, state).join();
        stream.scaleCreateNewEpoch(response).get();
        // history table has a partial record at this point.
        // now we could have sealed the segments so get successors could be called.

        Map<Long, List<Long>> successors = stream.getSuccessorsWithPredecessors(0).get()
                                                   .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));

        assertTrue(successors.containsKey(one) && successors.containsKey(two));

        // reset mock so that we can resume scale operation

        stream.scaleOldSegmentsSealed(sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response).get();
        stream.completeScale(response).join();

        successors = stream.getSuccessorsWithPredecessors(0).get()
                             .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));

        assertTrue(successors.containsKey(one) && successors.containsKey(two));
    }
}
