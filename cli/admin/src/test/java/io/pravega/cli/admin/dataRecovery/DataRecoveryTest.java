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
package io.pravega.cli.admin.dataRecovery;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.tables.EntrySerializer;
import io.pravega.test.integration.utils.LocalServiceStarter;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.DebugBookKeeperLogWrapper;
import io.pravega.segmentstore.storage.impl.bookkeeper.ReadOnlyBookkeeperLogMetadata;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Tests Data recovery commands.
 */
@Slf4j
public class DataRecoveryTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "testScope";
    // Setup utility.
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);
    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    /**
     * A directory for FILESYSTEM storage as LTS.
     */
    private File baseDir = null;
    private StorageFactory storageFactory = null;

    /**
     * A directory for storing logs and CSV files generated during the test..
     */
    private File logsDir = null;
    private BookKeeperLogFactory factory = null;

    @Override
    protected int getThreadPoolSize() {
        return 10;
    }

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("TestDataRecovery").toFile().getAbsoluteFile();
        this.logsDir = Files.createTempDirectory("DataRecovery").toFile().getAbsoluteFile();
        FileSystemStorageConfig adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();
        this.storageFactory = new FileSystemStorageFactory(adapterConfig, executorService());
    }

    @After
    public void tearDown() {
        STATE.get().close();
        if (this.factory != null) {
            this.factory.close();
        }
        FileHelpers.deleteFileOrDirectory(this.baseDir);
        FileHelpers.deleteFileOrDirectory(this.logsDir);
    }

    /**
     * Tests DurableLog recovery command.
     * @throws Exception    In case of any exception thrown while execution.
     */
    @Test
    public void testDataRecoveryCommand() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId++);
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, null);
        String streamName = "testDataRecoveryCommand";

        TestUtils.createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName, config);
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Write events to the streams.
            TestUtils.writeEvents(streamName, clientRunner.getClientFactory());
        }
        pravegaRunner.shutDownControllerRunner(); // Shut down the controller

        // Flush all Tier 1 to LTS
        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());
        for (int containerId = 0; containerId < containerCount; containerId++) {
            componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT).join();
        }

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper

        // start a new BookKeeper and ZooKeeper.
        pravegaRunner.startBookKeeperRunner(instanceId++);

        // set pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort());
        pravegaProperties.setProperty("bookkeeper.ledger.path", pravegaRunner.getBookKeeperRunner().getLedgerPath());
        pravegaProperties.setProperty("bookkeeper.zk.metadata.path", pravegaRunner.getBookKeeperRunner().getLogMetaNamespace());
        pravegaProperties.setProperty("pravegaservice.clusterName", pravegaRunner.getBookKeeperRunner().getBaseNamespace());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Command under test
        TestUtils.executeCommand("data-recovery durableLog-recovery", STATE.get());

        // Start a new segment store and controller
        this.factory = new BookKeeperLogFactory(pravegaRunner.getBookKeeperRunner().getBkConfig().get(), pravegaRunner.getBookKeeperRunner().getZkClient().get(),
                executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.factory);
        log.info("Started a controller and segment store.");
        // Create the client with new controller.
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Try reading all events to verify that the recovery was successful.
            TestUtils.readAllEvents(SCOPE, streamName, clientRunner.getClientFactory(), clientRunner.getReaderGroupManager(), "RG", "R");
            log.info("Read all events again to verify that segments were recovered.");
        }
        Assert.assertNotNull(StorageListSegmentsCommand.descriptor());
    }

    /**
     * Tests list segments command.
     * @throws Exception    In case of any exception thrown while execution.
     */
    @Test
    public void testListSegmentsCommand() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId);
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, null);
        String streamName = "testListSegmentsCommand";

        TestUtils.createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName, config);
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Write events to the streams.
            TestUtils.writeEvents(streamName, clientRunner.getClientFactory());
        }
        pravegaRunner.shutDownControllerRunner(); // Shut down the controller

        // Flush all Tier 1 to LTS
        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());
        for (int containerId = 0; containerId < containerCount; containerId++) {
            componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT).join();
        }

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper

        // set pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Execute the command for list segments
        TestUtils.executeCommand("data-recovery list-segments " + this.logsDir.getAbsolutePath(), STATE.get());
        // There should be a csv file created for storing segments in Container 0
        Assert.assertTrue(new File(this.logsDir.getAbsolutePath(), "Container_0.csv").exists());
        // Check if the file has segments listed in it
        Path path = Paths.get(this.logsDir.getAbsolutePath() + "/Container_0.csv");
        long lines = Files.lines(path).count();
        AssertExtensions.assertGreaterThan("There should be at least one segment.", 1, lines);
        Assert.assertNotNull(StorageListSegmentsCommand.descriptor());
    }

    @Test
    public void testBasicDurableLogRepairCommand() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId);
        val bkConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.factory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.factory);

        String streamName = "testDataRecoveryCommand";
        TestUtils.createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName, config);
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Write events to the streams.
            TestUtils.writeEvents(streamName, clientRunner.getClientFactory());
        }
        // Shut down services, we assume that the cluster is in very bad shape in this test.
        pravegaRunner.shutDownControllerRunner();
        pravegaRunner.shutDownSegmentStoreRunner();

        // set Pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort());
        pravegaProperties.setProperty("bookkeeper.ledger.path", pravegaRunner.getBookKeeperRunner().getLedgerPath());
        pravegaProperties.setProperty("bookkeeper.zk.metadata.path", pravegaRunner.getBookKeeperRunner().getLogMetaNamespace());
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Execute basic command workflow for repairing DurableLog.
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = Mockito.spy(new DurableDataLogRepairCommand(args));

        // First execution, just exit when asking to disable the original log.
        command.execute();

        // Disable Original Log first.
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());

        // The test will exercise editing the Container 0 log with an operation of each type.
        Mockito.doReturn(true).doReturn(false).doReturn(false)
                .doReturn(true).when(command).confirmContinue();
        Mockito.doReturn(900L).doReturn(901L).doReturn(902L).doReturn(1L)
                .when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("delete").doReturn("add").doReturn("DeleteSegmentOperation")
                .when(command).getStringUserInput(Mockito.any());
        command.execute();

        // Disable Original Log first.
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());

        // Now, re-execute the command to exercise the case in which there is an existing backup log.
        Mockito.doReturn(1).when(command).getIntUserInput(Mockito.any());
        Mockito.doReturn(true).doReturn(false).doReturn(false)
                .doReturn(true).when(command).confirmContinue();
        Mockito.doReturn(900L).doReturn(901L).doReturn(902L).doReturn(1L)
                .when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("delete").doReturn("add").doReturn("DeleteSegmentOperation")
                .when(command).getStringUserInput(Mockito.any());
        command.execute();

        // Disable Original Log first.
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());

        // Re-execute, now adding a replace operation and not destroying previous backup log.
        Mockito.doReturn(2).when(command).getIntUserInput(Mockito.any());
        Mockito.doReturn(true).doReturn(false).doReturn(true)
                .when(command).confirmContinue();
        Mockito.doReturn(900L).doReturn(1L).when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("replace").doReturn("StreamSegmentSealOperation").doReturn("replace")
                .doReturn("StreamSegmentSealOperation").when(command).getStringUserInput(Mockito.any());
        command.execute();

        // Disable Original Log first.
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());

        // Do nothing if we find an existing backup log.
        Mockito.doReturn(3).when(command).getIntUserInput(Mockito.any());
        command.execute();
        DurableDataLogRepairCommand.descriptor();
    }

    @Test
    public void testDurableLogRepairCommandExpectedLogOutput() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId);
        val bkConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.factory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.factory);

        String streamName = "testDataRecoveryCommand";
        TestUtils.createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName, config);
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Write events to the streams.
            TestUtils.writeEvents(streamName, clientRunner.getClientFactory());
        }
        // Shut down services, we assume that the cluster is in very bad shape in this test.
        pravegaRunner.shutDownControllerRunner();
        pravegaRunner.shutDownSegmentStoreRunner();

        // set Pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort());
        pravegaProperties.setProperty("bookkeeper.ledger.path", pravegaRunner.getBookKeeperRunner().getLedgerPath());
        pravegaProperties.setProperty("bookkeeper.zk.metadata.path", pravegaRunner.getBookKeeperRunner().getLogMetaNamespace());
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Execute basic command workflow for repairing DurableLog.
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = Mockito.spy(new DurableDataLogRepairCommand(args));

        this.factory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        this.factory.initialize();

        // First, keep all the Operations of Container 0 in this list, so we can compare with the modified one.
        List<Operation> originalOperations = new ArrayList<>();
        @Cleanup
        DebugDurableDataLogWrapper wrapper = this.factory.createDebugLogWrapper(0);
        command.readDurableDataLogWithCustomCallback((op, entry) -> originalOperations.add(op), 0, wrapper.asReadOnly());

        // Disable Original Log first.
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        TestUtils.executeCommand("bk disable 0", STATE.get());

        // Second, add 2 operations, delete 1 operation, replace 1 operation.
        Mockito.doReturn(true).doReturn(true).doReturn(false)
                .doReturn(true).doReturn(true).doReturn(false).doReturn(false)
                .doReturn(true)
                .when(command).confirmContinue();
        Mockito.doReturn(900L).doReturn(901L)
                .doReturn(901L).doReturn(1L).doReturn(123L)
                .doReturn(2L).doReturn(123L)
                .doReturn(903L).doReturn(3L).doReturn(123L)
                .doReturn(905L).doReturn(4L).doReturn(123L)
                .when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("delete")
                .doReturn("add").doReturn("DeleteSegmentOperation").doReturn("DeleteSegmentOperation")
                .doReturn("replace").doReturn("DeleteSegmentOperation")
                .doReturn("add").doReturn("StreamSegmentSealOperation")
                .when(command).getStringUserInput(Mockito.any());
        command.execute();

        List<Operation> originalOperationsEdited = new ArrayList<>();
        @Cleanup
        DebugDurableDataLogWrapper wrapperEdited = this.factory.createDebugLogWrapper(0);
        command.readDurableDataLogWithCustomCallback((op, entry) -> originalOperationsEdited.add(op), 0, wrapperEdited.asReadOnly());

        // Now, let's check that the edited log has the Operations we expect.
        // Original Log: OP-899, OP-900, OP-901, OP-902, OP-903, OP-904, OP-905
        // Edited Log:   OP-899, NEW-ADD 900 (DeleteSegment), NEW-ADD 901 (DeleteSegment), OP-901(now 902), OP-902(now 903),
        //               NEW REPLACE 903 (DeleteSegment now 904), OP-904 (now 905), NEW-ADD 905 (StreamSegmentSealOperation now 906),
        //               OP-905 (now 907)
        for (int i = 899; i < 910; i++) {
            // Sequence numbers will defer between the original and edited logs. To do equality comparisons between
            // Operations in both logs, reset the sequence numbers (other fields should be the same).
            originalOperations.get(i).resetSequenceNumber(0);
            originalOperationsEdited.get(i).resetSequenceNumber(0);
        }
        Assert.assertNotEquals(originalOperations.get(899), originalOperationsEdited.get(899));
        Assert.assertTrue(originalOperationsEdited.get(899) instanceof DeleteSegmentOperation);
        Assert.assertTrue(originalOperationsEdited.get(900) instanceof DeleteSegmentOperation);
        Assert.assertEquals(originalOperations.get(900).toString(), originalOperationsEdited.get(901).toString());
        Assert.assertEquals(originalOperations.get(901).toString(), originalOperationsEdited.get(902).toString());
        Assert.assertTrue(originalOperationsEdited.get(903) instanceof DeleteSegmentOperation);
        Assert.assertEquals(originalOperations.get(903).toString(), originalOperationsEdited.get(904).toString());
        Assert.assertTrue(originalOperationsEdited.get(905) instanceof StreamSegmentSealOperation);
        Assert.assertEquals(originalOperations.get(904).toString(), originalOperationsEdited.get(906).toString());
        this.factory.close();
    }

    @Test
    public void testRepairLogEditOperationCorrectness() throws IOException {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = new DurableDataLogRepairCommand(args);

        // Check when no commands are available.
        command.checkDurableLogEdits(new ArrayList<>());

        // Check adding Edit Operations with sequence numbers lower than 0.
        AssertExtensions.assertThrows("Edit Operations should have initial sequence ids > 0.",
                () -> command.checkDurableLogEdits(Arrays.asList(
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                                -1, 10, new DeleteSegmentOperation(0)),
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.REPLACE_OPERATION,
                                0, 10, new DeleteSegmentOperation(0)))),
                ex -> ex instanceof IllegalStateException);

        // A Delete Edit Operation should have an initial sequence number lower than the final one.
        AssertExtensions.assertThrows("Edit Operations should have initial sequence ids > 0.",
                () -> command.checkDurableLogEdits(List.of(
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION,
                                2, 2, null))),
                ex -> ex instanceof IllegalStateException);

        // Add one Add Edit and one Replace Edit on the same sequence number. This is expected to fail.
        AssertExtensions.assertThrows("Two non-Add Edit Operation on the same Sequence Number should not be accepted.",
                () -> command.checkDurableLogEdits(Arrays.asList(
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                                10, 10, new DeleteSegmentOperation(0)),
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.REPLACE_OPERATION,
                                10, 10, new DeleteSegmentOperation(0)))),
                ex -> ex instanceof IllegalStateException);

        // We can have multiple Add Edit Operations on the same sequence number.
        command.checkDurableLogEdits(Arrays.asList(
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                                10, 10, new DeleteSegmentOperation(0)),
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                                10, 10, new DeleteSegmentOperation(0)),
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                                10, 10, new DeleteSegmentOperation(0))));
        AssertExtensions.assertThrows("Two non-Add Edit Operation on the same Sequence Number should not be accepted.",
                () -> command.checkDurableLogEdits(Arrays.asList(
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                                10, 10, new DeleteSegmentOperation(0)),
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                                10, 10, new DeleteSegmentOperation(0)),
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.REPLACE_OPERATION,
                                10, 10, new DeleteSegmentOperation(0)))),
                ex -> ex instanceof IllegalStateException);
        AssertExtensions.assertThrows("Two non-Add Edit Operation on the same Sequence Number should not be accepted.",
                () -> command.checkDurableLogEdits(Arrays.asList(
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION,
                                1, 10, null),
                        new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION,
                                5, 20, null))),
                ex -> ex instanceof IllegalStateException);
    }

    @Test
    public void testRepairLogEditOperationUserInput() throws IOException {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = Mockito.spy(new DurableDataLogRepairCommand(args));

        // Case 1: Input a Delete Edit Operation with wrong initial/final ids. Then retry with correct ids.
        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(1L).doReturn(1L).doReturn(1L).doReturn(2L)
                .when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("delete").when(command).getStringUserInput(Mockito.any());
        Assert.assertEquals(List.of(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION,
                1, 2, null)), command.getDurableLogEditsFromUser());

        // Case 2: Input an Add Edit Operation with a wrong operation type. Then retry with correct operation type.
        Mockito.doReturn(true).doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(1L).doReturn(1L).when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("add").doReturn("wrong").doReturn("add")
                .doReturn("DeleteSegmentOperation").when(command).getStringUserInput(Mockito.any());
        DeleteSegmentOperation deleteOperationAdded = new DeleteSegmentOperation(1);
        List<DurableDataLogRepairCommand.LogEditOperation> editOps = new ArrayList<>();
        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                1, 1, deleteOperationAdded));
        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                1, 1, deleteOperationAdded));
        Assert.assertEquals(editOps, command.getDurableLogEditsFromUser());

        // Case 3: Create rest of operation types without payload (MergeSegmentOperation, StreamSegmentMapOperation, StreamSegmentTruncateOperation, UpdateAttributesOperation)
        long timestamp = System.currentTimeMillis();
        UUID uuid = UUID.randomUUID();
        editOps.clear();

        Mockito.doReturn(true).doReturn(false).doReturn(false)
                .doReturn(true).doReturn(true).doReturn(false).doReturn(false)
                .doReturn(true).doReturn(false)
                .doReturn(true).doReturn(true).doReturn(false).doReturn(false)
                .when(command).confirmContinue();
        Mockito.doReturn(1L).doReturn(1L).doReturn(2L).doReturn(1L).doReturn(2L).doReturn(123L)
                .doReturn(2L).doReturn(2L).doReturn(3L).doReturn(1L).doReturn(10L).doReturn(timestamp)
                .doReturn(3L).doReturn(3L)
                .doReturn(4L).doReturn(4L).doReturn(3L).doReturn(1L).doReturn(2L)
                .when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("add").doReturn("MergeSegmentOperation").doReturn(uuid.toString())
                .doReturn("add").doReturn("StreamSegmentMapOperation").doReturn("test").doReturn(uuid.toString())
                .doReturn("add").doReturn("StreamSegmentTruncateOperation")
                .doReturn("add").doReturn("UpdateAttributesOperation").doReturn(uuid.toString())
                .when(command).getStringUserInput(Mockito.any());
        Mockito.doReturn((int) AttributeUpdateType.Replace.getTypeId()).when(command).getIntUserInput(Mockito.any());
        Mockito.doReturn(true).doReturn(true).doReturn(false).doReturn(false)
                .when(command).getBooleanUserInput(Mockito.any());

        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        attributeUpdates.add(new AttributeUpdate(AttributeId.fromUUID(uuid), AttributeUpdateType.Replace, 1, 2));
        MergeSegmentOperation mergeSegmentOperation =  new MergeSegmentOperation(1, 2, attributeUpdates);
        mergeSegmentOperation.setStreamSegmentOffset(123);
        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                1, 1, mergeSegmentOperation));

        Map<AttributeId, Long> attributes = new HashMap<>();
        attributes.put(AttributeId.fromUUID(uuid), 10L);
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name("test").startOffset(2).length(3).storageLength(1)
                .sealed(true).deleted(false).sealedInStorage(true).deletedInStorage(false)
                .attributes(attributes).lastModified(new ImmutableDate(timestamp)).build();
        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                2, 2, new StreamSegmentMapOperation(segmentProperties)));

        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                3, 3, new StreamSegmentTruncateOperation(3, 3)));

        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                4, 4, new UpdateAttributesOperation(4, attributeUpdates)));

        Assert.assertEquals(editOps, command.getDurableLogEditsFromUser());

        // Case 4: Add wrong inputs.
        Mockito.doReturn(true).doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doThrow(NumberFormatException.class).doThrow(NullPointerException.class).when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("wrong").doReturn("replace").doReturn("replace").when(command).getStringUserInput(Mockito.any());
        command.getDurableLogEditsFromUser();
    }

    @Test
    public void testRepairLogEditOperationsWithContent() throws IOException {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = Mockito.spy(new DurableDataLogRepairCommand(args));
        List<DurableDataLogRepairCommand.LogEditOperation> editOps = new ArrayList<>();

        // Case 1: Input Add Edit Operations for a MetadataCheckpointOperation and StorageMetadataCheckpointOperation operations
        // with payload operation with zeros as content.
        Mockito.doReturn(true).doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(1L).when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn(100).when(command).getIntUserInput(Mockito.any());
        Mockito.doReturn("add").doReturn("MetadataCheckpointOperation").doReturn("zero")
                .doReturn("StorageMetadataCheckpointOperation").doReturn("zero")
                .when(command).getStringUserInput(Mockito.any());
        MetadataCheckpointOperation metadataCheckpointOperation = new MetadataCheckpointOperation();
        metadataCheckpointOperation.setContents(new ByteArraySegment(new byte[100]));
        StorageMetadataCheckpointOperation storageMetadataCheckpointOperation = new StorageMetadataCheckpointOperation();
        storageMetadataCheckpointOperation.setContents(new ByteArraySegment(new byte[100]));
        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                1, 1, metadataCheckpointOperation));
        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                1, 1, storageMetadataCheckpointOperation));
        Assert.assertEquals(editOps, command.getDurableLogEditsFromUser());

        // Case 2: Input an Add Edit Operation for a StreamSegmentAppendOperation with content loaded from a file.
        editOps.clear();
        byte[] content = new byte[]{1, 2, 3, 4, 5};
        File tmpFile = File.createTempFile("operationContent", "bin");
        Files.write(tmpFile.toPath(), content);
        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(1L).when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn(1).doReturn(10).when(command).getIntUserInput(Mockito.any());
        Mockito.doReturn("wrong").doReturn("add").doReturn("StreamSegmentAppendOperation")
                .doReturn("file").doReturn(tmpFile.toString())
                .when(command).getStringUserInput(Mockito.any());
        StreamSegmentAppendOperation appendOperation = new StreamSegmentAppendOperation(1, 20, new ByteArraySegment(content), new AttributeUpdateCollection());
        editOps.add(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION,
                1, 1, appendOperation));
        Assert.assertEquals(editOps, command.getDurableLogEditsFromUser());
        Files.delete(tmpFile.toPath());

        // Case 3: Abort content generation.
        Mockito.doReturn("quit").when(command).getStringUserInput(Mockito.any());
        AssertExtensions.assertThrows("", command::createOperationContents, ex -> ex instanceof RuntimeException);
    }

    @Test
    public void testRepairLogEditOperationCreateSegmentProperties() throws IOException {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = Mockito.spy(new DurableDataLogRepairCommand(args));

        // Create a SegmentProperties object via the command logic and verify that it is equal to the expected one.
        long timestamp = System.currentTimeMillis();
        Map<AttributeId, Long> attributes = new HashMap<>();
        UUID uuid = UUID.randomUUID();
        attributes.put(AttributeId.fromUUID(uuid), 10L);
        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(2L).doReturn(3L).doReturn(1L).doReturn(10L)
                .doReturn(timestamp).when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("test").doReturn(uuid.toString()).when(command).getStringUserInput(Mockito.any());
        Mockito.doReturn(true).doReturn(true).doReturn(false).doReturn(false)
                .when(command).getBooleanUserInput(Mockito.any());
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name("test").startOffset(2).length(3).storageLength(1)
                .sealed(true).deleted(false).sealedInStorage(true).deletedInStorage(false)
                .attributes(attributes).lastModified(new ImmutableDate(timestamp)).build();
        Assert.assertEquals(segmentProperties, command.createSegmentProperties());

        // Induce exceptions during the process of creating attributes to check error handling.
        segmentProperties = StreamSegmentInformation.builder().name("test").startOffset(2).length(3).storageLength(1)
                .sealed(true).deleted(false).sealedInStorage(true).deletedInStorage(false)
                .attributes(new HashMap<>()).lastModified(new ImmutableDate(timestamp)).build();

        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(2L).doReturn(3L).doReturn(1L).doReturn(timestamp)
                .when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("test").doThrow(NumberFormatException.class).when(command).getStringUserInput(Mockito.any());
        Mockito.doReturn(true).doReturn(true).doReturn(false).doReturn(false)
                .when(command).getBooleanUserInput(Mockito.any());
        Assert.assertEquals(segmentProperties, command.createSegmentProperties());

        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(2L).doReturn(3L).doReturn(1L).doReturn(timestamp)
                .when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn("test").doThrow(NullPointerException.class).when(command).getStringUserInput(Mockito.any());
        Mockito.doReturn(true).doReturn(true).doReturn(false).doReturn(false)
                .when(command).getBooleanUserInput(Mockito.any());
        Assert.assertEquals(segmentProperties, command.createSegmentProperties());
    }

    @Test
    public void testRepairLogEditOperationCreateAttributeUpdateCollection() throws IOException {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = Mockito.spy(new DurableDataLogRepairCommand(args));

        // Create an AttributeUpdateCollection via the command logic and check the expected output.
        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        UUID uuid = UUID.randomUUID();
        attributeUpdates.add(new AttributeUpdate(AttributeId.fromUUID(uuid), AttributeUpdateType.Replace, 1, 2));
        Mockito.doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doReturn(uuid.toString()).when(command).getStringUserInput(Mockito.any());
        Mockito.doReturn(1L).doReturn(2L).doReturn(1L).when(command).getLongUserInput(Mockito.any());
        Mockito.doReturn((int) AttributeUpdateType.Replace.getTypeId()).when(command).getIntUserInput(Mockito.any());
        Assert.assertArrayEquals(attributeUpdates.getUUIDAttributeUpdates().toArray(),
                command.createAttributeUpdateCollection().getUUIDAttributeUpdates().toArray());

        // Induce exceptions during the process to check error handling.
        Mockito.doReturn(true).doReturn(true).doReturn(false).when(command).confirmContinue();
        Mockito.doThrow(NumberFormatException.class).doThrow(NullPointerException.class).when(command).getStringUserInput(Mockito.any());
        Assert.assertArrayEquals(new Object[0], command.createAttributeUpdateCollection().getUUIDAttributeUpdates().toArray());
    }

    @Test
    public void testUserInputMethods() throws Exception {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = new DurableDataLogRepairCommand(args);

        System.setIn(new ByteArrayInputStream("true".getBytes()));
        Assert.assertTrue(command.getBooleanUserInput("Test message"));
        System.setIn(new ByteArrayInputStream("yes".getBytes()));
        Assert.assertEquals("yes", command.getStringUserInput("Test message"));
        System.setIn(new ByteArrayInputStream("1".getBytes()));
        Assert.assertEquals(1, command.getIntUserInput("Test message"));
        System.setIn(new ByteArrayInputStream("2".getBytes()));
        Assert.assertEquals(2L, command.getLongUserInput("Test message"));
    }

    @Test
    public void testForceMetadataOverWrite() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId);
        val bkConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.factory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.factory);

        String streamName = "testDataRecoveryCommand";
        TestUtils.createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName, config);
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Write events to the streams.
            TestUtils.writeEvents(streamName, clientRunner.getClientFactory());
        }
        // Shut down services, we assume that the cluster is in very bad shape in this test.
        pravegaRunner.shutDownControllerRunner();
        pravegaRunner.shutDownSegmentStoreRunner();

        // set Pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort());
        pravegaProperties.setProperty("bookkeeper.ledger.path", pravegaRunner.getBookKeeperRunner().getLedgerPath());
        pravegaProperties.setProperty("bookkeeper.zk.metadata.path", pravegaRunner.getBookKeeperRunner().getLogMetaNamespace());
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Execute basic command workflow for repairing DurableLog.
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = Mockito.spy(new DurableDataLogRepairCommand(args));

        // Test the DurableLogWrapper options to get, overwrite and destroy logs.
        @Cleanup
        val newFactory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        newFactory.initialize();
        @Cleanup
        DebugBookKeeperLogWrapper debugLogWrapper0 = newFactory.createDebugLogWrapper(0);
        int container0LogEntries = command.readDurableDataLogWithCustomCallback((a, b) -> { }, 0, debugLogWrapper0.asReadOnly());
        Assert.assertTrue(container0LogEntries > 0);
        ReadOnlyBookkeeperLogMetadata metadata0 = debugLogWrapper0.fetchMetadata();
        Assert.assertNotNull(metadata0);

        // Create a Repair log with some random content.
        @Cleanup
        DurableDataLog repairLog = newFactory.createDurableDataLog(this.factory.getRepairLogId());
        repairLog.initialize(TIMEOUT);
        repairLog.append(new CompositeByteArraySegment(new byte[0]), TIMEOUT).join();
        @Cleanup
        DebugBookKeeperLogWrapper debugLogWrapperRepair = newFactory.createDebugLogWrapper(0);

        // Overwrite metadata of repair container with metadata of container 0.
        debugLogWrapperRepair.forceMetadataOverWrite(metadata0);
        // Now the amount of log entries read should be equal to the ones of container 0.
        int newContainerRepairLogEntries = command.readDurableDataLogWithCustomCallback((a, b) -> { }, this.factory.getRepairLogId(), debugLogWrapperRepair.asReadOnly());
        ReadOnlyBookkeeperLogMetadata newMetadata1 = debugLogWrapperRepair.fetchMetadata();
        Assert.assertEquals(container0LogEntries, newContainerRepairLogEntries);
        Assert.assertEquals(metadata0.getLedgers(), newMetadata1.getLedgers());

        // Destroy contents of Container 0.
        debugLogWrapper0.deleteDurableLogMetadata();
        Assert.assertNull(debugLogWrapper0.fetchMetadata());
    }

    @Test
    public void testLogEditOperationObject() throws IOException {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Delete Edit Operations should not take into account the newOperation field doing equality.
        Assert.assertEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION, 1, 2, null),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION, 1, 2, new DeleteSegmentOperation(1)));
        Assert.assertEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION, 1, 2, null).hashCode(),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.DELETE_OPERATION, 1, 2, new DeleteSegmentOperation(1)).hashCode());

        // Other cases for equality of operations.
        Assert.assertEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, null),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, null));
        Assert.assertEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1)),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1)));
        // Equality of payload operations are checked by type and sequence number, which are the common attributes of Operation class.
        DurableDataLogRepairCommand.LogEditOperation deleteOp = new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(2));
        Assert.assertEquals(deleteOp, new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1)));
        deleteOp.getNewOperation().resetSequenceNumber(123);
        Assert.assertNotEquals(deleteOp, new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1)));

        // Test the cases for the same object reference and for null comparison.
        DurableDataLogRepairCommand.LogEditOperation sameOp = new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1));
        Assert.assertEquals(sameOp, sameOp);
        Assert.assertNotEquals(sameOp, null);

        Assert.assertNotEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, null),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1)));
        Assert.assertNotEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.REPLACE_OPERATION, 1, 2, null),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1)));
        Assert.assertNotEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, null),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 2, 2, new DeleteSegmentOperation(1)));
        Assert.assertNotEquals(new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 2, new DeleteSegmentOperation(1)),
                new DurableDataLogRepairCommand.LogEditOperation(DurableDataLogRepairCommand.LogEditType.ADD_OPERATION, 1, 1, new DeleteSegmentOperation(1)));
    }

    @Test
    public void testCheckBackupLogAssertions() throws IOException {
        // Setup command object.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega0");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        CommandArgs args = new CommandArgs(List.of("0"), STATE.get());
        DurableDataLogRepairCommand command = new DurableDataLogRepairCommand(args);

        AssertExtensions.assertThrows("Different beforeCommitCalls and commitSuccessCalls should have thrown an assertion error.",
                () -> command.checkBackupLogAssertions(1, 0, 1, false), t -> t instanceof IllegalStateException);
        AssertExtensions.assertThrows("Different beforeCommitCalls and commitSuccessCalls should have thrown an assertion error.",
                () -> command.checkBackupLogAssertions(0, 1, 1, false), t -> t instanceof IllegalStateException);
        AssertExtensions.assertThrows("Different commitSuccessCalls and originalReads from Original Log should have thrown an assertion error.",
                () -> command.checkBackupLogAssertions(1, 1, 2, false), t -> t instanceof IllegalStateException);
        AssertExtensions.assertThrows("Not successful BackupLogProcessor execution should have thrown an assertion error..",
                () -> command.checkBackupLogAssertions(1, 1, 1, true), t -> t instanceof IllegalStateException);
    }

    @Test
    public void testTableSegmentRecoveryCommand() throws Exception {
        // set pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "CHUNKED_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        pravegaProperties.setProperty("bookkeeper.ledger.path", "/pravega/bookkeeper/ledgers0");
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Create the data to test.
        File testDataDir = Files.createTempDirectory("test-data-table-segment-recovery").toFile().getAbsoluteFile();
        File pravegaStorageDir = Files.createTempDirectory("table-segment-recovery-command").toFile().getAbsoluteFile();

        List<TableEntry> tableSegmentPuts = List.of(
                TableEntry.unversioned(new ByteArraySegment("k1".getBytes()), new ByteArraySegment("v1".getBytes())),
                TableEntry.unversioned(new ByteArraySegment("k2".getBytes()), new ByteArraySegment("v2".getBytes())),
                TableEntry.unversioned(new ByteArraySegment("k3".getBytes()), new ByteArraySegment("v3".getBytes())),
                TableEntry.unversioned(new ByteArraySegment("k4".getBytes()), new ByteArraySegment("v4".getBytes()))); // This is a delete operation on k1.

        List<TableKey> tableSegmentRemovals = List.of(TableKey.unversioned(new ByteArraySegment("k1".getBytes())));

        EntrySerializer entrySerializer = new EntrySerializer();
        BufferView serializedEntries = BufferView.builder().add(entrySerializer.serializeUpdate(tableSegmentPuts))
                                                           .add(entrySerializer.serializeRemoval(tableSegmentRemovals))
                                                           .build();
        InputStream serializedEntriesReader = serializedEntries.getReader();

        Path p1 = Files.createTempFile(testDataDir.toPath(), "chunk-1", ".txt");
        Path p2 = Files.createTempFile(testDataDir.toPath(), "chunk-2", ".txt");
        Files.write(p1, serializedEntriesReader.readNBytes(serializedEntries.getLength() / 2), StandardOpenOption.WRITE);
        Files.write(p2, serializedEntriesReader.readAllBytes(), StandardOpenOption.WRITE);

        // Command under test
        TestUtils.executeCommand("data-recovery tableSegment-recovery " + testDataDir.getAbsolutePath() + " test " + pravegaStorageDir, STATE.get());
        Assert.assertNotNull(TableSegmentRecoveryCommand.descriptor());

        // After that, we need to check that the storage data chunk in Pravega instance is the same as the one generated in the test.
        File[] potentialFiles = new File(pravegaStorageDir.toString()).listFiles();
        assert potentialFiles != null;
        List<File> listOfFiles = Arrays.stream(potentialFiles)
                .filter(File::isFile)
                .filter(f -> !f.getName().contains("$attributes.index")) // We are interested in the data, not the attribute segments.
                .sorted()
                .collect(Collectors.toList());
        // There should one only one data chunk for this Table Segment.
        Assert.assertEquals(1, listOfFiles.size());
        // The contents of the test data chunks and the contents of the Pravega Table Segment data chunks should be the same.
        Assert.assertArrayEquals(Files.readAllBytes(Paths.get(pravegaStorageDir.toString(), listOfFiles.get(0).getName())), serializedEntries.getCopy());
    }
}
