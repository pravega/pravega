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
package io.pravega.cli.admin.bookkeeper;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.NetworkInterface;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test basic functionality of Bookkeeper commands.
 */
public class BookkeeperCommandsTest extends BookKeeperClusterTestCase {

    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final InputStream originalIn = System.in;

    public BookkeeperCommandsTest() {
        super(1);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        baseConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        baseClientConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        boolean successfulSetup = false;
        while (interfaces.hasMoreElements()) {
            try {
                super.setUp();
                successfulSetup = true;
                break;
            } catch (Exception e) {
                // On some environments, using default interface does not allow to resolve the host name. We keep
                // iterating over existing interfaces to start the Bookkeeper cluster.
                super.tearDown();
                baseConf.setListeningInterface(interfaces.nextElement().getName());
            }
        }
        assert successfulSetup;

        STATE.set(new AdminCommandState());
        Properties bkProperties = new Properties();
        bkProperties.setProperty("pravegaservice.container.count", "4");
        bkProperties.setProperty("pravegaservice.zk.connect.uri", zkUtil.getZooKeeperConnectString());
        bkProperties.setProperty("bookkeeper.ledger.path", "/ledgers");
        bkProperties.setProperty("bookkeeper.zk.metadata.path", "ledgers");
        bkProperties.setProperty("bookkeeper.ensemble.size", "1");
        bkProperties.setProperty("bookkeeper.ack.quorum.size", "1");
        bkProperties.setProperty("bookkeeper.write.quorum.size", "1");
        bkProperties.setProperty("pravegaservice.clusterName", "");
        STATE.get().getConfigBuilder().include(bkProperties);

        System.setOut(new PrintStream(outContent));
    }

    @Override
    @After
    @SneakyThrows
    public void tearDown() {
        System.setOut(originalOut);
        System.setIn(originalIn);
        STATE.get().close();
        super.tearDown();
    }

    @Test
    public void testBookKeeperListCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("bk list", STATE.get());
        Assert.assertTrue(commandResult.contains("log_summary") && commandResult.contains("logId\": 0"));
    }

    @Test
    public void testBookKeeperDetailsCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("bk details 0", STATE.get());
        Assert.assertTrue(commandResult.contains("log_summary") && commandResult.contains("logId\": 0"));
        commandResult = TestUtils.executeCommand("bk details 100", STATE.get());
        Assert.assertTrue(commandResult.contains("log_no_metadata"));
    }

    @Test
    public void testBookKeeperDisableAndEnableCommands() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        System.setIn(new ByteArrayInputStream("no".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        String commandResult = TestUtils.executeCommand("bk disable 0", STATE.get());
        Assert.assertTrue(commandResult.contains("enabled\": false"));
        // Disable an already disabled log.
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        commandResult = TestUtils.executeCommand("bk disable 0", STATE.get());
        Assert.assertTrue(commandResult.contains("already disabled"));

        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        commandResult = TestUtils.executeCommand("bk enable 0", STATE.get());
        Assert.assertTrue(commandResult.contains("enabled\": true"));
        // Enable an already enabled log.
        System.setIn(new ByteArrayInputStream("no".getBytes()));
        TestUtils.executeCommand("bk enable 0", STATE.get());
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        commandResult = TestUtils.executeCommand("bk enable 0", STATE.get());
        Assert.assertTrue(commandResult.contains("enabled\": true"));
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        commandResult = TestUtils.executeCommand("bk enable 100", STATE.get());
        Assert.assertTrue(commandResult.contains("log_no_metadata"));
        // Execute closing Zookeeper server.
        this.zkUtil.killCluster();
        AssertExtensions.assertThrows(DataLogNotAvailableException.class, () -> TestUtils.executeCommand("bk enable 0", STATE.get()));
    }

    @Test
    public void testBookKeeperCleanupCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        String commandResult = TestUtils.executeCommand("bk cleanup", STATE.get());
        Assert.assertTrue(commandResult.contains("no Ledgers eligible for deletion"));
        System.setIn(new ByteArrayInputStream("no".getBytes()));
        TestUtils.executeCommand("bk cleanup", STATE.get());

        CommandArgs args = new CommandArgs(Collections.singletonList(""), STATE.get());
        BookKeeperCleanupCommand command = new BookKeeperCleanupCommand(args);
        BookKeeperCommand.Context context = command.createContext();

        // List one existing and one
        command.listCandidates(Collections.singletonList(0L), context);
        Assert.assertFalse(outContent.toString().contains("No such ledger exists"));
        command.listCandidates(Collections.singletonList(1L), context);
        Assert.assertTrue(outContent.toString().contains("No such ledger exists"));

        // Try to exercise deletion standalone.
        command.deleteCandidates(Collections.singletonList(0L), Collections.singletonList(0L), context);
        command.deleteCandidates(Collections.singletonList(0L), Collections.singletonList(1L), context);
        Assert.assertTrue(outContent.toString().contains("Deleted Ledger 0"));
        command.deleteCandidates(Collections.singletonList(-1L), Collections.singletonList(0L), context);
        command.deleteCandidates(Collections.singletonList(0L), Collections.singletonList(1L), null);
    }

    @Test
    public void testBookKeeperRecoveryCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("container recover 0", STATE.get());
        Assert.assertTrue(commandResult.contains("Recovery complete"));
        CommandArgs args = new CommandArgs(Collections.singletonList("0"), STATE.get());
        ContainerRecoverCommand command = new ContainerRecoverCommand(args);
        // Test unwrap exception options.
        command.unwrapDataCorruptionException(new DataCorruptionException("test"));
        command.unwrapDataCorruptionException(new DataCorruptionException("test", "test"));
        command.unwrapDataCorruptionException(new DataCorruptionException("test", Arrays.asList("test", "test")));
        command.unwrapDataCorruptionException(new DataCorruptionException("test", (DataCorruptionException) null));
        // Check that exception is thrown if ZK is not available.
        this.zkUtil.stopCluster();
        AssertExtensions.assertThrows(DataLogNotAvailableException.class, () -> TestUtils.executeCommand("container recover 0", STATE.get()));
    }

    @Test
    public void testBookKeeperContinuousRecoveryCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("container continuous-recover 2 1", STATE.get());
        Assert.assertTrue(commandResult.contains("Recovery complete"));

        CommandArgs args = new CommandArgs(Arrays.asList("1", "1"), STATE.get());
        ContainerContinuousRecoveryCommand command = Mockito.spy(new ContainerContinuousRecoveryCommand(args));
        Mockito.doThrow(new DurableDataLogException("Intentional")).when(command).performRecovery(ArgumentMatchers.anyInt());
        command.execute();
        Assert.assertNotNull(ContainerContinuousRecoveryCommand.descriptor());
    }

    @Test
    public void testRecoveryState() {
        CommandArgs args = new CommandArgs(Collections.singletonList("0"), STATE.get());
        ContainerRecoverCommand.RecoveryState state = new ContainerRecoverCommand(args).new RecoveryState();
        Operation op = new TestOperation();
        List<DataFrameRecord.EntryInfo> entries = new ArrayList<>();
        entries.add(new TestEntryInfo());
        // Exercise RecoveryState logic.
        state.operationComplete(op, new DataCorruptionException("Test exception"));
        state.newOperation(op, entries);
        state.operationComplete(op, null);
    }

    @Test
    public void testBookKeeperReconcileCommand() throws Exception {
        // Try the command against a non-existent log.
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        Assert.assertFalse(TestUtils.executeCommand("bk reconcile 0", STATE.get()).contains("reconciliation completed"));
        createLedgerInBookkeeperTestCluster(0);
        // Try the command against an enabled log.
        Assert.assertFalse(TestUtils.executeCommand("bk reconcile 0", STATE.get()).contains("reconciliation completed"));
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());
        // Now, let's try the command under the expected conditions.
        System.setIn(new ByteArrayInputStream("no".getBytes()));
        TestUtils.executeCommand("bk reconcile 0", STATE.get());
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        String commandResult = TestUtils.executeCommand("bk reconcile 0", STATE.get());
        Assert.assertTrue(commandResult.contains("reconciliation completed"));
    }

    @Test
    public void testBookKeeperDeleteLedgersCommand() throws Exception {
        // Try the command against a non-existent log.
        Assert.assertTrue(TestUtils.executeCommand("bk delete-ledgers 5 1", STATE.get()).contains("does not exist."));
        createLedgerInBookkeeperTestCluster(0);
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());
        // Now, let's try the command under the expected conditions.
        String commandResult = TestUtils.executeCommand("bk details 0", STATE.get());
        Assert.assertTrue(commandResult.contains("log_summary") && commandResult.contains("logId\": 0"));
        System.out.println("CommandResult: " + commandResult);
        System.setIn(new ByteArrayInputStream("no".getBytes()));
        TestUtils.executeCommand("bk delete-ledgers 0 0", STATE.get());
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        commandResult = TestUtils.executeCommand("bk delete-ledgers 0 0", STATE.get());
        Assert.assertTrue(commandResult.contains("Delete ledgers command completed"));
    }

    @Test
    public void testBookKeeperListAllLedgersCommand() throws Exception {
        Assert.assertTrue(TestUtils.executeCommand("bk list-ledgers", STATE.get()).contains("List of ledgers in the system"));
        createLedgerInBookkeeperTestCluster(0);
        Assert.assertTrue(TestUtils.executeCommand("bk list-ledgers", STATE.get()).contains("bookieLogID: 0"));
    }

    private void createLedgerInBookkeeperTestCluster(int logId) throws Exception {
        BookKeeperConfig bookKeeperConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.ZK_METADATA_PATH, "ledgers")
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/ledgers")
                .with(BookKeeperConfig.ZK_ADDRESS, zkUtil.getZooKeeperConnectString()).build();
        @Cleanup
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zkUtil.getZooKeeperConnectString(), new RetryOneTime(5000));
        curatorFramework.start();
        @Cleanup("shutdownNow")
        ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "bk-test");
        @Cleanup
        BookKeeperLogFactory bookKeeperLogFactory = new BookKeeperLogFactory(bookKeeperConfig, curatorFramework, executorService);
        bookKeeperLogFactory.initialize();
        @Cleanup
        DurableDataLog log = bookKeeperLogFactory.createDurableDataLog(logId);
        log.initialize(Duration.ofSeconds(5));
    }

    private static class TestOperation extends Operation {

    }

    private static class TestEntryInfo extends DataFrameRecord.EntryInfo {
        TestEntryInfo() {
            super(null, 0, 0, true);
        }
    }
}
