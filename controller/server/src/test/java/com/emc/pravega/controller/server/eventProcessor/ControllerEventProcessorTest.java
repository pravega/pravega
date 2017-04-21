/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.controller.mocks.SegmentHelperMock;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.stream.TxnStatus;
import com.emc.pravega.controller.store.stream.VersionedTransactionData;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.testcommon.TestingServerStarter;
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

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;
    private TestingServer zkServer;
    private SegmentHelper segmentHelperMock;
    private CuratorFramework cli;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();

        streamStore = StreamStoreFactory.createZKStore(cli, executor);
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
        cli.close();
        zkServer.stop();
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
        commitEventProcessor.process(new CommitEvent(SCOPE, STREAM, txnData.getId()));
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
        abortEventProcessor.process(new AbortEvent(SCOPE, STREAM, txnData.getId()));
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTED);
    }

    private void checkTransactionState(String scope, String stream, UUID txnId, TxnStatus expectedStatus) {
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnId, null, executor).join();
        Assert.assertEquals(expectedStatus, txnStatus);
    }
}
