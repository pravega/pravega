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
package io.pravega.cli.admin.controller;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonSyntaxException;
import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.cli.admin.utils.ZKHelper;
import io.pravega.client.ClientConfig;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.task.EventHelper;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import io.pravega.shared.controller.event.RGStreamCutRecord;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.SetupUtils;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.cli.admin.utils.FileHelper.createFileAndDirectory;
import static io.pravega.shared.NameUtils.DELETED_STREAMS_TABLE;
import static io.pravega.shared.NameUtils.EPOCHS_WITH_TRANSACTIONS_TABLE;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.NameUtils.METADATA_TABLE;
import static io.pravega.shared.NameUtils.getMarkStreamForStream;
import static io.pravega.shared.NameUtils.getQualifiedTableName;
import static io.pravega.shared.NameUtils.getScopedStreamName;
import static io.pravega.test.integration.utils.TestUtils.pathToConfig;

public abstract class AbstractControllerMetadataCommandsTest {
    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    private ClientConfig clientConfig;

    public void setup(boolean enableAuth, boolean enableTls) throws Exception {
        ClientConfig.ClientConfigBuilder clientConfigBuilder = ClientConfig.builder().controllerURI(SETUP_UTILS.getControllerUri());

        STATE.set(new AdminCommandState());
        SETUP_UTILS.startAllServices(enableAuth, enableTls);
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", SETUP_UTILS.getControllerRestUri().toString());
        pravegaProperties.setProperty("cli.controller.grpc.uri", SETUP_UTILS.getControllerUri().toString());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", SETUP_UTILS.getZkTestServer().getConnectString());
        pravegaProperties.setProperty("pravegaservice.container.count", String.valueOf(1));
        pravegaProperties.setProperty("pravegaservice.admin.gateway.port", String.valueOf(SETUP_UTILS.getAdminPort()));
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega-cluster");

        if (enableAuth) {
            clientConfigBuilder = clientConfigBuilder.credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                    SecurityConfigDefaults.AUTH_ADMIN_USERNAME));
            pravegaProperties.setProperty("cli.channel.auth", Boolean.toString(true));
            pravegaProperties.setProperty("cli.credentials.username", SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
            pravegaProperties.setProperty("cli.credentials.pwd", SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }
        if (enableTls) {
            clientConfigBuilder = clientConfigBuilder.trustStore(pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME)
                    .validateHostName(false);
            pravegaProperties.setProperty("cli.channel.tls", Boolean.toString(true));
            pravegaProperties.setProperty("cli.trustStore.location", "../../config/" + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);
            pravegaProperties.setProperty("cli.trustStore.access.token.ttl.seconds", Integer.toString(300));
        }
        STATE.get().getConfigBuilder().include(pravegaProperties);

        clientConfig = clientConfigBuilder.build();
    }

    @Test
    public void testControllerMetadataTablesInfo() throws Exception {
        String commandResult = TestUtils.executeCommand("controller-metadata tables-info", STATE.get());
        Assert.assertTrue(commandResult.contains("metadata"));
        Assert.assertTrue(commandResult.contains("epochsWithTransactions"));
        Assert.assertTrue(commandResult.contains("writersPositions"));
        Assert.assertTrue(commandResult.contains("transactionsInEpoch"));
        Assert.assertTrue(commandResult.contains("completedTransactionsBatches"));
        Assert.assertTrue(commandResult.contains("completedTransactionsBatch-"));
        Assert.assertTrue(commandResult.contains("deletedStreams"));
    }

    @Test
    public void testGetControllerMetadataEntryCommand() throws Exception {
        String scope = "controllerMetadata1";
        String stream = "getEntry";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata get " + DELETED_STREAMS_TABLE + " " +
                getScopedStreamName(scope, stream) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("For the given key: %s", getScopedStreamName(scope, stream))));
    }

    @Test
    public void testGetControllerMetadataEntryJSONCommand() throws Exception {
        Path tempDirPath = Files.createTempDirectory("getEntryDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "deleted.json").toString();

        String scope = "controllerMetadataJSON";
        String stream = "getJSONEntry";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata get " + DELETED_STREAMS_TABLE + " " +
                getScopedStreamName(scope, stream) + " localhost " + filename, STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("For the given key: %s", getScopedStreamName(scope, stream))));
        Assert.assertTrue(commandResult.contains(String.format("Successfully wrote the value to %s in JSON.", filename)));
        File file = new File(filename);
        Assert.assertTrue(file.exists());
        Assert.assertNotEquals(0, file.length());

        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));

        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testGetControllerMetadataEntryCommandKeyDoesNotExist() throws Exception {
        String scope = "controllerMetadata2";
        String stream = "getEntryNoKey";
        String dummyStream = "getEntryDummy";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata get " + DELETED_STREAMS_TABLE + " " +
                getScopedStreamName(scope, dummyStream) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("Key not found: %s", getScopedStreamName(scope, dummyStream))));
    }

    @Test
    public void testGetControllerMetadataEntryCommandTableDoesNotExist() throws Exception {
        String dummyTable = getQualifiedTableName(INTERNAL_SCOPE_NAME,
                "randScope", "randStream", String.format(METADATA_TABLE, UUID.randomUUID()));
        String commandResult = TestUtils.executeCommand("controller-metadata get " + dummyTable + " creationTime localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("Table not found: %s", dummyTable)));
    }

    @Test
    public void testListControllerMetadataEntriesCommand() throws Exception {
        String scope = "controllerMetadata3";
        String stream = "listEntries";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata list-entries " + DELETED_STREAMS_TABLE + " 10 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(getScopedStreamName(scope, stream)));
        Assert.assertTrue(commandResult.contains(getScopedStreamName(scope, getMarkStreamForStream(stream))));
    }

    @Test
    public void testListControllerMetadataEntriesCommandTableDoesNotExist() throws Exception {
        String dummyTable = getQualifiedTableName(INTERNAL_SCOPE_NAME,
                "randScope", "randStream", String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, UUID.randomUUID()));
        String commandResult = TestUtils.executeCommand("controller-metadata list-entries " + dummyTable + " 10 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("Table not found: %s", dummyTable)));
    }

    @Test
    public void testListControllerMetadataKeysCommand() throws Exception {
        String scope = "controllerMetadata4";
        String stream = "listKeys";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata list-keys " + DELETED_STREAMS_TABLE + " 10 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(getScopedStreamName(scope, stream)));
        Assert.assertTrue(commandResult.contains(getScopedStreamName(scope, getMarkStreamForStream(stream))));
    }

    @Test
    public void testListControllerMetadataKeysCommandTableDoesNotExist() throws Exception {
        String dummyTable = getQualifiedTableName(INTERNAL_SCOPE_NAME,
                "randScope", "randStream", String.format(EPOCHS_WITH_TRANSACTIONS_TABLE, UUID.randomUUID()));
        String commandResult = TestUtils.executeCommand("controller-metadata list-keys " + dummyTable + " 10 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("Table not found: %s", dummyTable)));
    }

    @Test
    public void testUpdateControllerMetadataTableEntryCommand() throws Exception {
        Path tempDirPath = Files.createTempDirectory("updateEntryDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "deleted.json").toString();
        File f = createFileAndDirectory(filename);
        FileWriter writer = new FileWriter(f);
        writer.write("10");
        writer.close();

        String scope = "controllerMetadata5";
        String stream = "updateEntry";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata update " + DELETED_STREAMS_TABLE + " " +
                getScopedStreamName(scope, stream) + " localhost " + filename, STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("Successfully updated the key %s in table %s with version",
                getScopedStreamName(scope, stream), DELETED_STREAMS_TABLE)));

        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));

        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testUpdateControllerMetadataTableEntryCommandFileNotExists() throws Exception {
        String filename = "dummy/file.json";
        String scope = "controllerMetadata5";
        String stream = "updateEntry";

        String commandResult = TestUtils.executeCommand("controller-metadata update " + DELETED_STREAMS_TABLE + " " +
                getScopedStreamName(scope, stream) + " localhost " + filename, STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("File with new value does not exist: %s", filename)));
    }

    @Test
    public void testUpdateControllerMetadataTableEntryCommandKeyDoesNotExist() throws Exception {
        Path tempDirPath = Files.createTempDirectory("updateEntryDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "deleted.json").toString();
        File f = createFileAndDirectory(filename);
        FileWriter writer = new FileWriter(f);
        writer.write("10");
        writer.close();

        String scope = "controllerMetadata5";
        String stream = "updateEntry";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata update " + DELETED_STREAMS_TABLE +
                " dummyScope/dummyStream localhost " + filename, STATE.get());
        Assert.assertTrue(commandResult.contains("Key not found: dummyScope/dummyStream"));

        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));

        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testUpdateControllerMetadataEntryCommandTableDoesNotExist() throws Exception {
        Path tempDirPath = Files.createTempDirectory("updateEntryDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "deleted.json").toString();
        File f = createFileAndDirectory(filename);
        FileWriter writer = new FileWriter(f);
        writer.write("1000");
        writer.close();

        String dummyTable = getQualifiedTableName(INTERNAL_SCOPE_NAME,
                "randScope", "randStream", String.format(METADATA_TABLE, UUID.randomUUID()));
        String commandResult = TestUtils.executeCommand("controller-metadata update " + dummyTable +
                " creationTime localhost " + filename, STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("Table not found: %s", dummyTable)));

        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));

        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testUpdateControllerMetadataEntryCommandJSONSyntaxException() throws Exception {
        Path tempDirPath = Files.createTempDirectory("updateEntryDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "deleted.json").toString();
        File f = createFileAndDirectory(filename);
        FileWriter writer = new FileWriter(f);
        writer.write("{ state : ACTIVE }");
        writer.close();

        String dummyTable = getQualifiedTableName(INTERNAL_SCOPE_NAME,
                "randScope", "randStream", String.format(METADATA_TABLE, UUID.randomUUID()));
        AssertExtensions.assertThrows("Json syntax error", () -> TestUtils.executeCommand("controller-metadata update "
                + dummyTable + " creationTime localhost " + filename, STATE.get()), e -> e instanceof JsonSyntaxException);

        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));

        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testControllerMetadataViewReaderInfoCommand() throws Exception {
        final String process = UUID.randomUUID().toString();
        final String readerGroup = UUID.randomUUID().toString();
        final String reader = UUID.randomUUID().toString();
        ZKHelper zkHelper = ZKHelper.create(SETUP_UTILS.getZkTestServer().getConnectString(), "pravega-cluster");
        CheckpointStore checkpointStore = zkHelper.getCheckPointStore();
        checkpointStore.addReaderGroup(process, readerGroup);
        checkpointStore.addReader(process, readerGroup, reader);
        Position position = new PositionImpl(ImmutableMap.of(new SegmentWithRange(Segment.fromScopedName("testScope/testStream/0"), 0, 0.5), 9999999L,
                new SegmentWithRange(Segment.fromScopedName("testScope/testStream/1"), 0.5, 1.0), -1L));
        checkpointStore.setPosition(process, readerGroup, reader, position);
        String commandResult = TestUtils.executeCommand("controller-metadata get-reader " + process + " " + readerGroup + " " + reader, STATE.get() );
        Assert.assertTrue(commandResult.contains("testScope/testStream"));
    }

    @Test
    public void testControllerMetadataViewReaderInfoCommandWithException() throws Exception {
        final String process = UUID.randomUUID().toString();
        final String readerGroup = UUID.randomUUID().toString();
        final String reader = UUID.randomUUID().toString();
        ZKHelper zkHelper = ZKHelper.create(SETUP_UTILS.getZkTestServer().getConnectString(), "pravega-cluster");
        CheckpointStore checkpointStore = zkHelper.getCheckPointStore();
        checkpointStore.addReaderGroup(process, readerGroup);
        String commandResult = TestUtils.executeCommand("controller-metadata get-reader " + process + " " + readerGroup + " " + reader, STATE.get() );
        Assert.assertTrue(commandResult.contains("Exception accessing to reader metadata"));
    }

    private CreateReaderGroupEvent buildCreateRGEvent(String scope, String rgName, ReaderGroupConfig config,
                                                      final long requestId, final long createTimestamp) {
        Map<String, RGStreamCutRecord> startStreamCuts = config.getStartingStreamCuts().entrySet().stream()
                                                               .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                                                                       e -> new RGStreamCutRecord(ImmutableMap.copyOf(ModelHelper.getStreamCutMap(e.getValue())))));
        Map<String, RGStreamCutRecord> endStreamCuts = config.getEndingStreamCuts().entrySet().stream()
                                                             .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                                                                     e -> new RGStreamCutRecord(ImmutableMap.copyOf(ModelHelper.getStreamCutMap(e.getValue())))));
        return new CreateReaderGroupEvent(requestId, scope, rgName, config.getGroupRefreshTimeMillis(),
                config.getAutomaticCheckpointIntervalMillis(), config.getMaxOutstandingCheckpointRequest(),
                config.getRetentionType().ordinal(), config.getGeneration(), config.getReaderGroupId(),
                startStreamCuts, endStreamCuts, createTimestamp);
    }

    @Test
    public void testControllerMetadataViewPendingEventsCommand() throws Exception {
        final String host = UUID.randomUUID().toString();
        final String requestId = UUID.randomUUID().toString();

        @Cleanup("shutdownNow")
        ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "cli-test");
        ZKHelper zkHelper = ZKHelper.create(SETUP_UTILS.getZkTestServer().getConnectString(), "pravega-cluster");
        ZKHostIndex hostIndex = zkHelper.getZkHostIndex(executorService);
        @Cleanup
        EventHelper eventHelper = new EventHelper(executorService, host, hostIndex);

        String scopedStreamName = "scope/stream";
        ReaderGroupConfig rgConf = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                    .stream(scopedStreamName)
                                                    .retentionType(ReaderGroupConfig.StreamDataRetention.NONE)
                                                    .build();
        CreateReaderGroupEvent createEventOne = buildCreateRGEvent("scope", "rg1", rgConf, 1L, System.currentTimeMillis());
        eventHelper.addRequestToIndex(host, requestId, createEventOne).join();
        String commandResult = TestUtils.executeCommand("controller-metadata request-detail " + host + " " + requestId, STATE.get());
        Assert.assertTrue(commandResult.contains("rg1"));

        //No metadata found scenario.
        byte[] emptyByteData = new byte[0];
        hostIndex.removeEntity(host, requestId, false).join();
        hostIndex.addEntity(host, requestId, emptyByteData).join();
        commandResult = TestUtils.executeCommand("controller-metadata request-detail " + host + " " + requestId, STATE.get());
        Assert.assertTrue(commandResult.contains("No metadata found"));

        //Exception scenario : Remove the event from the host index. This will remove entry from zk hostIndex.
        eventHelper.removeTaskFromIndex(host, requestId ).join();
        String commandResultAfterRemoval = TestUtils.executeCommand("controller-metadata request-detail " + host + " " + requestId, STATE.get());
        Assert.assertTrue(commandResultAfterRemoval.contains("Exception accessing pending events metadata"));
    }

    @After
    public void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
        STATE.get().close();
    }

    public static class SecureControllerMetadataCommandsTest extends AbstractControllerMetadataCommandsTest {
        @Before
        public void startUp() throws Exception {
            setup(true, true);
        }
    }

    public static class ControllerMetadataCommandsTest extends AbstractControllerMetadataCommandsTest {
        @Before
        public void startUp() throws Exception {
            setup(false, false);
        }
    }
}
