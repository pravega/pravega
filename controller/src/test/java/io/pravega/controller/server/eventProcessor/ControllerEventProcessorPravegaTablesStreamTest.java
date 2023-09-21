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
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.mockito.ArgumentMatchers;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

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
        PravegaTablesStoreHelper storeHelper = spy(new PravegaTablesStoreHelper(SegmentHelperMock.getSegmentHelperMockForTables(executor), GrpcAuthHelper.getDisabledAuthHelper(), executor));
        this.streamStore = new PravegaTablesStreamMetadataStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), storeHelper);
        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        EventHelper eventHelperMock = EventHelperMock.getEventHelperMock(executor, "1", ((AbstractStreamMetadataStore) this.streamStore).getHostTaskIndex());
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, this.bucketStore, TaskStoreFactory.createInMemoryStore(executor),
                segmentHelperMock, executor, "1", GrpcAuthHelper.getDisabledAuthHelper(), eventHelperMock);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(this.streamStore, segmentHelperMock,
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
        spyStreamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, spyStreamMetadataTasks,
                                                                             streamTransactionMetadataTasks,
                                                                             bucketStore, executor);

        final String committingTxnsRecordKey = "committingTxns";
        long failingClientRequestId = 123L;
        doReturn(failingClientRequestId).when(spyStreamMetadataTasks).getRequestId(any());

        OperationContext context = this.streamStore.createStreamContext(scope, stream, failingClientRequestId);
        streamStore.startCommitTransactions(scope, stream, 100, context, executor).join();

        doReturn(Futures.failedFuture(new RuntimeException())).when(storeHelper)
                                                              .updateEntry(anyString(), eq(committingTxnsRecordKey), any(),
                                                                           ArgumentMatchers.<Function<String, byte[]>>any(),
                                                                           any(), eq(failingClientRequestId));
        AssertExtensions.assertFutureThrows("Updating CommittingTxnRecord fails", 
            commitEventProcessor.processEvent(new CommitEvent(scope, stream, epoch)), e -> Exceptions.unwrap(e) instanceof RuntimeException);
        verify(storeHelper, times(1)).removeEntries(anyString(), any(), eq(failingClientRequestId));
        VersionedMetadata<CommittingTransactionsRecord> versionedCommitRecord = this.streamStore.getVersionedCommittingTransactionsRecord(scope, stream, context, executor).join();
        CommittingTransactionsRecord commitRecord = versionedCommitRecord.getObject();
        assertFalse(CommittingTransactionsRecord.EMPTY.equals(commitRecord));
        for (VersionedTransactionData txnData : txnDataList) {
            checkTransactionState(scope, stream, txnData.getId(), TxnStatus.COMMITTED);
        }

        long goodClientRequestId = 4567L;
        doReturn(goodClientRequestId).when(spyStreamMetadataTasks).getRequestId(any());
        commitEventProcessor.processEvent(new CommitEvent(scope, stream, epoch)).join();
        versionedCommitRecord = this.streamStore.getVersionedCommittingTransactionsRecord(scope, stream, context, executor).join();
        commitRecord = versionedCommitRecord.getObject();
        assertTrue(CommittingTransactionsRecord.EMPTY.equals(commitRecord));

        for (VersionedTransactionData txnData : txnDataList) {
            checkTransactionState(scope, stream, txnData.getId(), TxnStatus.COMMITTED);
        }
    }
}
