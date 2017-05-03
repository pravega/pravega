/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Controller Event ProcessorTests.
 */
public class ControllerEventProcessorTest {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    private ScheduledExecutorService executor;
    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;
    private TestingServer zkServer;
    private SegmentHelper segmentHelperMock;
    private CuratorFramework zkClient;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newScheduledThreadPool(10);

        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        zkClient.start();

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();

        // region createStream
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();
        streamStore.createScope(SCOPE).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, STREAM, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        zkClient.close();
        zkServer.close();
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void testCommitEventProcessor() {
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, 10000, 10000, 10000, null,
                executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

        CommitEventProcessor commitEventProcessor = new CommitEventProcessor(streamStore, hostStore, executor,
                segmentHelperMock, null);
        commitEventProcessor.process(new CommitEvent(SCOPE, STREAM, txnData.getId()), null);
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
    }

    @Test(timeout = 10000)
    public void testAbortEventProcessor() {
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, 10000, 10000, 10000, null,
                executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), false, Optional.empty(), null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTING);

        AbortEventProcessor abortEventProcessor = new AbortEventProcessor(streamStore, hostStore, executor,
                segmentHelperMock, null);
        abortEventProcessor.process(new AbortEvent(SCOPE, STREAM, txnData.getId()), null);
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTED);
    }

    private void checkTransactionState(String scope, String stream, UUID txnId, TxnStatus expectedStatus) {
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnId, null, executor).join();
        Assert.assertEquals(expectedStatus, txnStatus);
    }
}
