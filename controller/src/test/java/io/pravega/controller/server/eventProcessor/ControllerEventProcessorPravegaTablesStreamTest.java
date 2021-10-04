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

import com.google.common.collect.ImmutableList;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.time.Duration;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;

/**
 * Controller Event ProcessorTests.
 */
public class ControllerEventProcessorPravegaTablesStreamTest extends ControllerEventProcessorTest {

    @Override
    StreamMetadataStore createStore() {
        return StreamStoreFactory.createPravegaTablesStore(
                SegmentHelperMock.getSegmentHelperMockForTables(executor), GrpcAuthHelper.getDisabledAuthHelper(), PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
    }

    @Test(timeout = 10000)
    public void testTxnPartialCommitRetry() {
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        PravegaTablesStoreHelper storeHelper = spy(new PravegaTablesStoreHelper(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(), executor));
        StreamMetadataStore streamStore = new PravegaTablesStreamMetadataStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), storeHelper);
        BucketStore bucketStore = StreamStoreFactory.createZKBucketStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        EventHelper eventHelperMock = EventHelperMock.getEventHelperMock(executor, "1", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, TaskStoreFactory.createInMemoryStore(executor),
                segmentHelperMock, executor, "1", GrpcAuthHelper.getDisabledAuthHelper(), eventHelperMock);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock,
                executor, "host", GrpcAuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        String scope = "scope";
        String stream = "stream";
        // region createStream
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        streamStore.createScope(scope, null, executor).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(scope, stream, configuration1, start, null, executor).join();
        streamStore.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataTasks spyStreamMetadataTasks = spy(streamMetadataTasks);
        List<VersionedTransactionData> txnDataList = createAndCommitTransactions(3);
        int epoch = txnDataList.get(0).getEpoch();

        final String committingTxnsRecordKey = "committingTxns";
        long clientRequestId = 123L;
        doReturn(Futures.failedFuture(new RuntimeException())).when(storeHelper).updateEntry(anyString(), eq(committingTxnsRecordKey), any(), ArgumentMatchers.<Function<String, byte[]>>any(), any(), eq(clientRequestId));
        ImmutableList.Builder<UUID> txIdList = ImmutableList.builder();
        txnDataList.stream().forEach(txn -> txIdList.add(txn.getId()));
        CommittingTransactionsRecord commitTxnsRecord = new CommittingTransactionsRecord(0, txIdList.build());
        doReturn(CompletableFuture.completedFuture(commitTxnsRecord)).when(storeHelper).getCachedOrLoad(anyString(), eq(committingTxnsRecordKey), ArgumentMatchers.<Function<byte[], String>>any(), anyLong(), eq(clientRequestId));
        doReturn(clientRequestId).when(spyStreamMetadataTasks).getRequestId(any());
        spyStreamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, spyStreamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        streamStore.startCommitTransactions(scope, stream, 100, null, executor).join();
        //commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();

        AssertExtensions.assertFutureThrows("Updating CommittingTxnRecord fails", commitEventProcessor.processEvent(new CommitEvent(scope, stream, epoch)), e -> Exceptions.unwrap(e) instanceof RuntimeException);
        //verify(storeHelper, times(1)).removeEntries(anyString(), any(), eq(clientRequestId));

    }
}
