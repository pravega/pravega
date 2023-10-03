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
package io.pravega.cli.admin.segmentstore;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.serializers.ContainerKeySerializer;
import io.pravega.cli.admin.serializers.ContainerMetadataSerializer;
import io.pravega.cli.admin.serializers.SltsKeySerializer;
import io.pravega.cli.admin.serializers.SltsMetadataSerializer;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.SetupUtils;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.cli.admin.segmentstore.tableSegment.GetTableSegmentInfoCommand.ENTRY_COUNT;
import static io.pravega.cli.admin.segmentstore.tableSegment.GetTableSegmentInfoCommand.KEY_LENGTH;
import static io.pravega.cli.admin.segmentstore.tableSegment.GetTableSegmentInfoCommand.LENGTH;
import static io.pravega.cli.admin.segmentstore.tableSegment.GetTableSegmentInfoCommand.SEGMENT_NAME;
import static io.pravega.cli.admin.segmentstore.tableSegment.GetTableSegmentInfoCommand.START_OFFSET;
import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_ID;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_LENGTH;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_NAME;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_SEALED;
import static io.pravega.cli.admin.serializers.ContainerMetadataSerializer.SEGMENT_PROPERTIES_START_OFFSET;
import static io.pravega.shared.NameUtils.getMetadataSegmentName;
import static io.pravega.test.integration.utils.TestUtils.pathToConfig;


/**
 *  This test is for testing the segment store cli commands.
 */
public abstract class AbstractSegmentStoreCommandsTest {
    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    protected static final int CONTAINER_COUNT = 1;

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
        pravegaProperties.setProperty("cli.controller.connect.grpc.uri", SETUP_UTILS.getControllerUri().getHost() + ":" + SETUP_UTILS.getControllerUri().getPort());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", SETUP_UTILS.getZkTestServer().getConnectString());
        pravegaProperties.setProperty("pravegaservice.container.count", String.valueOf(CONTAINER_COUNT));
        pravegaProperties.setProperty("pravegaservice.admin.gateway.port", String.valueOf(SETUP_UTILS.getAdminPort()));
        pravegaProperties.setProperty("pravegaservice.clusterName", "pravega/pravega-cluster");

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
    public void testGetSegmentInfoCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getinfo", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-info segmentstore/getinfo/0.#epoch.0 localhost", STATE.get());

        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_abortStream/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_requeststream/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGcommitStreamReaders/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGscaleGroup/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGkvtStreamReaders/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGabortStreamReaders/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/containers/metadata_0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore get-segment-info not/exists/0 localhost", STATE.get()));
        Assert.assertNotNull(GetSegmentInfoCommand.descriptor());
    }

    @Test
    public void testReadSegmentRangeCommand() throws Exception {
        // Create a temporary directory.
        Path tempDirPath = Files.createTempDirectory("readSegmentDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "readSegmentTest.txt").toString();

        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "readsegment", StreamConfiguration.builder().build());

        @Cleanup
        EventStreamClientFactory factory = EventStreamClientFactory.withScope("segmentstore", clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = factory.createEventWriter("readsegment", new JavaSerializer<>(), EventWriterConfig.builder().build());
        writer.writeEvents("rk", Arrays.asList("a", "2", "3"));
        writer.flush();

        // Check to make sure that the file exists and data is written into it.
        String commandResult = TestUtils.executeCommand("segmentstore read-segment segmentstore/readsegment/0.#epoch.0 0 8 localhost " + filename, STATE.get());
        Assert.assertTrue(commandResult.contains("The segment data has been successfully written into"));
        File file = new File(filename);
        Assert.assertTrue(file.exists());
        Assert.assertNotEquals(0, file.length());

        AssertExtensions.assertThrows(FileAlreadyExistsException.class, () ->
                TestUtils.executeCommand("segmentstore read-segment _system/_RGcommitStreamReaders/0.#epoch.0 0 8 localhost " + filename, STATE.get()));
        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));

        AssertExtensions.assertThrows(WireCommandFailedException.class, () ->
                TestUtils.executeCommand("segmentstore read-segment not/exists/0 0 1 localhost " + filename, STATE.get()));
        Assert.assertNotNull(ReadSegmentRangeCommand.descriptor());
        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));

        // Delete the temporary directory.
        tempDirPath.toFile().deleteOnExit();
    }

    @Test
    public void testGetSegmentAttributeCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getattribute", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/getattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore get-segment-attribute not/exists/0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get()));
        Assert.assertNotNull(GetSegmentAttributeCommand.descriptor());
    }

    @Test
    public void testUpdateSegmentAttributeCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "updateattribute", StreamConfiguration.builder().build());
        // First, get the existing value of that attribute for the segment.
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        long oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertNotEquals(0L, oldValue);
        // Update the Segment to a value of 0.
        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 " + oldValue + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("UpdateSegmentAttribute:"));
        // Check that the value has been updated.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertEquals(0L, oldValue);

        // Do the same for an internal segment.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertNotEquals(0L, oldValue);
        // Update the Segment to a value of 0.
        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 " + oldValue + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("UpdateSegmentAttribute:"));
        // Check that the value has been updated.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertEquals(0L, oldValue);

        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore update-segment-attribute not/exists/0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 0 localhost", STATE.get()));
        Assert.assertNotNull(UpdateSegmentAttributeCommand.descriptor());
    }

    @Test
    public void testFlushToStorageCommandAllCase() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        String commandResult = TestUtils.executeCommand("container flush-to-storage all", STATE.get());
        for (int id = 0; id < CONTAINER_COUNT; id++) {
            Assert.assertTrue(commandResult.contains("Flushed the Segment Container with containerId " + id + " to Storage."));
        }
        Assert.assertNotNull(FlushToStorageCommand.descriptor());
    }

    @Test
    public void testFlushToStorageCommand() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        String commandResult = TestUtils.executeCommand("container flush-to-storage 0", STATE.get());
        Assert.assertTrue(commandResult.contains("Flushed the Segment Container with containerId 0 to Storage."));
        Assert.assertNotNull(FlushToStorageCommand.descriptor());
    }

    @Test
    public void testFlushToStorageCommandNotInetAddress() {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.0.1", 1234);
        AssertExtensions.assertThrows("Flush with incorrect inet ip", () -> TestUtils.executeCommand("container flush-to-storage all", STATE.get()),
                ex -> ex instanceof WireCommandFailedException);
    }

    @Test
    public void testFlushToStorageCommandLocalhost() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", 1234);
        String commandResult = TestUtils.executeCommand("container flush-to-storage 0", STATE.get());
        Assert.assertTrue(commandResult.contains("Flushed the Segment Container with containerId 0 to Storage."));
        Assert.assertNotNull(FlushToStorageCommand.descriptor());
    }

    @Test
    public void testFlushToStorageCommandRangeCase() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        String commandResult = TestUtils.executeCommand("container flush-to-storage 0 " + (CONTAINER_COUNT - 1), STATE.get());
        Assert.assertTrue(commandResult.contains("Flushed the Segment Container with containerId 0 to Storage."));
        Assert.assertNotNull(FlushToStorageCommand.descriptor());
    }

    @Test
    public void testFlushToStorageCommandWithEndContainerNotNumber() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("End container id must be a number.", () -> TestUtils.executeCommand("container flush-to-storage 0 all", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFlushToStorageCommandWithoutGettingSegmentStoreHost() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("Error getting segment store hosts for containers.", () -> TestUtils.executeCommand("container flush-to-storage 0 all", STATE.get()),
                ex -> ex instanceof RuntimeException);
    }

    @Test
    public void testFlushToStorageCommandWithThreeArguments() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("Incorrect argument count.", () -> TestUtils.executeCommand("container flush-to-storage 0 1 1", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFlushToStorageCommandWithoutGettingSegmentStoreHostForGivenContainer() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "8");
        STATE.get().getConfigBuilder().include(pravegaProperties);
        AssertExtensions.assertThrows("No host found for given container", () -> TestUtils.executeCommand("container flush-to-storage 4", STATE.get()),
                ex -> ex instanceof RuntimeException);
        pravegaProperties.setProperty("pravegaservice.container.count", String.valueOf(CONTAINER_COUNT));
        STATE.get().getConfigBuilder().include(pravegaProperties);
    }

    @Test
    public void testFlushToStorageCommandWithoutArguments() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("Incorrect argument count.", () -> TestUtils.executeCommand("container flush-to-storage", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFlushToStorageCommandAllCaseWithRange() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("Incorrect argument count.", () -> TestUtils.executeCommand("container flush-to-storage all 0", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFlushToStorageCommandWithInvalidStartContainer() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("Incorrect argument count.", () -> TestUtils.executeCommand("container flush-to-storage 100", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFlushToStorageCommandWithInvalidEndContainer() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("Incorrect argument count.", () -> TestUtils.executeCommand("container flush-to-storage 0 100", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFlushToStorageCommandWithNegativeStartContainerId() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("The start container id must be a positive number.", () -> TestUtils.executeCommand("container flush-to-storage -1", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testFlushToStorageCommandWithNegativeEndContainerId() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "127.0.0.1", 1234);
        AssertExtensions.assertThrows("The end container id must be a positive number.", () -> TestUtils.executeCommand("container flush-to-storage 0 -1", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testSetSerializerCommand() throws Exception {
        Assert.assertNull(STATE.get().getKeySerializer());
        Assert.assertNull(STATE.get().getValueSerializer());

        String commandResult = TestUtils.executeCommand("table-segment set-serializer dummy", STATE.get());
        Assert.assertTrue(commandResult.contains("Serializers named dummy do not exist."));
        Assert.assertNull(STATE.get().getKeySerializer());
        Assert.assertNull(STATE.get().getValueSerializer());

        commandResult = TestUtils.executeCommand("table-segment set-serializer slts", STATE.get());
        Assert.assertTrue(commandResult.contains("Serializers changed to slts successfully."));
        Assert.assertTrue(STATE.get().getKeySerializer() instanceof SltsKeySerializer);
        Assert.assertTrue(STATE.get().getValueSerializer() instanceof SltsMetadataSerializer);

        commandResult = TestUtils.executeCommand("table-segment set-serializer container_meta", STATE.get());
        Assert.assertTrue(commandResult.contains("Serializers changed to container_meta successfully."));
        Assert.assertTrue(STATE.get().getKeySerializer() instanceof ContainerKeySerializer);
        Assert.assertTrue(STATE.get().getValueSerializer() instanceof ContainerMetadataSerializer);
    }

    @Test
    public void testGetTableSegmentInfoCommand() throws Exception {
        String tableSegmentName = getMetadataSegmentName(0);
        String commandResult = TestUtils.executeCommand("table-segment get-info " + tableSegmentName + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(tableSegmentName));
        Assert.assertTrue(commandResult.contains(SEGMENT_NAME));
        Assert.assertTrue(commandResult.contains(START_OFFSET));
        Assert.assertTrue(commandResult.contains(LENGTH));
        Assert.assertTrue(commandResult.contains(ENTRY_COUNT));
        Assert.assertTrue(commandResult.contains(KEY_LENGTH));
    }

    @Test
    public void testListTableSegmentKeysCommand() throws Exception {
        String setSerializerResult = TestUtils.executeCommand("table-segment set-serializer container_meta", STATE.get());
        Assert.assertTrue(setSerializerResult.contains("Serializers changed to container_meta successfully."));
        Assert.assertTrue(STATE.get().getKeySerializer() instanceof ContainerKeySerializer);
        Assert.assertTrue(STATE.get().getValueSerializer() instanceof ContainerMetadataSerializer);

        String tableSegmentName = getMetadataSegmentName(0);
        int keyCount = 5;
        String commandResult = TestUtils.executeCommand("table-segment list-keys " + tableSegmentName + " " + keyCount + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("List of at most " + keyCount + " keys in " + tableSegmentName));
    }

    @Test
    public void testGetTableSegmentEntryCommand() throws Exception {
        String setSerializerResult = TestUtils.executeCommand("table-segment set-serializer container_meta", STATE.get());
        Assert.assertTrue(setSerializerResult.contains("Serializers changed to container_meta successfully."));
        Assert.assertTrue(STATE.get().getKeySerializer() instanceof ContainerKeySerializer);
        Assert.assertTrue(STATE.get().getValueSerializer() instanceof ContainerMetadataSerializer);

        String tableSegmentName = getMetadataSegmentName(0);
        String key = "_system/_RGkvtStreamReaders/0.#epoch.0";
        String commandResult = TestUtils.executeCommand("table-segment get " + tableSegmentName + " " + key + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("container metadata info:"));
        Assert.assertTrue(commandResult.contains(SEGMENT_ID));
        Assert.assertTrue(commandResult.contains(SEGMENT_PROPERTIES_NAME));
        Assert.assertTrue(commandResult.contains(SEGMENT_PROPERTIES_SEALED));
        Assert.assertTrue(commandResult.contains(SEGMENT_PROPERTIES_START_OFFSET));
        Assert.assertTrue(commandResult.contains(SEGMENT_PROPERTIES_LENGTH));
    }
    
    @Test
    public void testPutTableSegmentEntryCommand() throws Exception {
        String setSerializerResult = TestUtils.executeCommand("table-segment set-serializer container_meta", STATE.get());
        Assert.assertTrue(setSerializerResult.contains("Serializers changed to container_meta successfully."));
        Assert.assertTrue(STATE.get().getKeySerializer() instanceof ContainerKeySerializer);
        Assert.assertTrue(STATE.get().getValueSerializer() instanceof ContainerMetadataSerializer);

        String tableSegmentName = getMetadataSegmentName(0);
        String key = "_system/_RGkvtStreamReaders/0.#epoch.0";
        StringBuilder newValueBuilder = new StringBuilder();
        appendField(newValueBuilder, SEGMENT_ID, "1");
        appendField(newValueBuilder, SEGMENT_PROPERTIES_NAME, key);
        appendField(newValueBuilder, SEGMENT_PROPERTIES_SEALED, "false");
        appendField(newValueBuilder, SEGMENT_PROPERTIES_START_OFFSET, "0");
        appendField(newValueBuilder, SEGMENT_PROPERTIES_LENGTH, "10");
        appendField(newValueBuilder, "80000000-0000-0000-0000-000000000000", "1632728432718");

        String commandResult = TestUtils.executeCommand("table-segment put " + tableSegmentName + " localhost " +
                        key + " " + newValueBuilder.toString(),
                STATE.get());
        Assert.assertTrue(commandResult.contains("Successfully updated the key " + key + " in table " + tableSegmentName));
    }

    @Test
    public void testModifyTableSegmentEntryCommandValidFieldCase() throws Exception {
        String setSerializerResult = TestUtils.executeCommand("table-segment set-serializer container_meta", STATE.get());
        Assert.assertTrue(setSerializerResult.contains("Serializers changed to container_meta successfully."));
        Assert.assertTrue(STATE.get().getKeySerializer() instanceof ContainerKeySerializer);
        Assert.assertTrue(STATE.get().getValueSerializer() instanceof ContainerMetadataSerializer);

        String tableSegmentName = getMetadataSegmentName(0);
        String key = "_system/_RGkvtStreamReaders/0.#epoch.0";
        StringBuilder newFieldValueBuilder = new StringBuilder();
        appendField(newFieldValueBuilder, SEGMENT_PROPERTIES_START_OFFSET, "20");
        appendField(newFieldValueBuilder, SEGMENT_PROPERTIES_LENGTH, "30");
        appendField(newFieldValueBuilder, "80000000-0000-0000-0000-000000000000", "1632728432718");
        appendField(newFieldValueBuilder, "dummy_field", "dummy");

        String commandResult = TestUtils.executeCommand("table-segment modify " + tableSegmentName + " localhost " +
                        key + " " + newFieldValueBuilder.toString(),
                STATE.get());
        Assert.assertTrue(commandResult.contains("dummy_field field does not exist."));
        Assert.assertTrue(commandResult.contains("Successfully modified the following fields in the value for key " + key + " in table " + tableSegmentName));
    }

    @Test
    public void testModifyTableSegmentEntryCommandInValidFieldCase() throws Exception {
        String setSerializerResult = TestUtils.executeCommand("table-segment set-serializer container_meta", STATE.get());
        Assert.assertTrue(setSerializerResult.contains("Serializers changed to container_meta successfully."));
        Assert.assertTrue(STATE.get().getKeySerializer() instanceof ContainerKeySerializer);
        Assert.assertTrue(STATE.get().getValueSerializer() instanceof ContainerMetadataSerializer);

        String tableSegmentName = getMetadataSegmentName(0);
        String key = "_system/_RGkvtStreamReaders/0.#epoch.0";
        StringBuilder newFieldValueBuilder = new StringBuilder();
        appendField(newFieldValueBuilder, "dummy_field", "dummy");

        String commandResult = TestUtils.executeCommand("table-segment modify " + tableSegmentName + " localhost " +
                        key + " " + newFieldValueBuilder.toString(),
                STATE.get());
        Assert.assertTrue(commandResult.contains("dummy_field field does not exist."));
        Assert.assertTrue(commandResult.contains("No fields provided to modify."));
    }

    @Test
    public void testGetContainerIdOfSegmentCommandWithIncorrectArgs() {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getContainerIdOfSegmentCommandWrongArgTest", StreamConfiguration.builder().build());
        AssertExtensions.assertThrows("Incorrect argument count.", 
                () -> TestUtils.executeCommand("segmentstore get-container-id segmentstore/getContainerIdOfSegmentCommandWrongArgTest/0.#epoch.0 localhost", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testGetContainerIdOfSegmentCommandWithInvalidSegmentName() {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getContainerIdOfSegmentCommandInvalidSegmentName", StreamConfiguration.builder().build());
        AssertExtensions.assertThrows("Invalid qualified-segment-name.", 
                () -> TestUtils.executeCommand("segmentstore get-container-id segmentstore/getContainerIdOfSegmentCommandInvalidSegmentName", STATE.get()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testGetContainerIdOfSegmentCommand() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", SETUP_UTILS.getServicePort());
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getContainerIdTest", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore get-container-id segmentstore/getContainerIdTest/0.#epoch.0", STATE.get());
        Assert.assertTrue(commandResult.contains("Container Id"));
        Assert.assertNotNull(GetContainerIdOfSegmentCommand.descriptor());
    }

    @Test
    public void testGetContainerIdwithskipcheck() throws Exception {
        String commandResult = TestUtils.executeCommand("segmentstore get-container-id segmentstore/getContainerIdwithskipcheck/0.#epoch.0 skipcheck", STATE.get());
        Assert.assertTrue(commandResult.contains("Container Id"));
        Assert.assertNotNull(GetContainerIdOfSegmentCommand.descriptor());
    }

    @Test
    public void testGetContainerIdOfNonSegmentCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getContainerIdTest", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore get-container-id segmentstores/notAStream/test", STATE.get());
        Assert.assertTrue(commandResult.contains("Error occurred while fetching containerId"));
        Assert.assertNotNull(GetContainerIdOfSegmentCommand.descriptor());
    }

    @Test
    public void testDeleteSegmentCommandWithIncorrectArgs() {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "deleteSegmentWrongArgTest", StreamConfiguration.builder().build());
        AssertExtensions.assertThrows("Incorrect argument count.", () -> {
            TestUtils.executeCommand("segmentstore delete-segment segmentstore/deleteSegmentWrongArgTest/0.#epoch.0 localhost", STATE.get());
        }, ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testDeleteSegmentInvalidSegmentName() {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "deleteSegmentInvalidSegmentName", StreamConfiguration.builder().build());
        AssertExtensions.assertThrows("Invalid qualified-segment-name.", () -> {
            TestUtils.executeCommand("segmentstore delete-segment segmentstore/deleteSegmentInvalidSegmentName", STATE.get());
        }, ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testDeleteSegmentCommand() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", SETUP_UTILS.getServicePort());
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "deleteSegmentTest", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore delete-segment segmentstore/deleteSegmentTest/0.#epoch.0", STATE.get());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        // here trying to delete the already deleted segment
        String commandResult2 = TestUtils.executeCommand("segmentstore delete-segment segmentstore/deleteSegmentTest/0.#epoch.0", STATE.get());
        Assert.assertTrue(commandResult2.contains("DeleteSegment failed"));
        Assert.assertNotNull(DeleteSegmentCommand.descriptor());
    }

    @Test
    public void testCreateSegmentCommandWithIncorrectArgs() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "createSegmentWrongArgTest", StreamConfiguration.builder().build());
        AssertExtensions.assertThrows("Incorrect argument count.", () -> {
            TestUtils.executeCommand("segmentstore create-segment segmentstore/createSegmentWrongArgTest/0.#epoch.0 localhost", STATE.get());
        }, ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testCreateSegmentCommandInvalidSegmentName() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "createSegmentInvalidSegmentName", StreamConfiguration.builder().build());
        AssertExtensions.assertThrows("Invalid qualified-segment-name", () -> {
            TestUtils.executeCommand("segmentstore create-segment segmentstore/InvalidSegmentName", STATE.get());
        }, ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testCreateSegmentAlreadyExist() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", SETUP_UTILS.getServicePort());
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "createSegmentAlreadyExist", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore create-segment segmentstore/createSegmentAlreadyExist/0.#epoch.0", STATE.get());
        Assert.assertTrue(commandResult.contains("already exists"));
    }

    @Test
    public void testCreateSegmentCommand() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", SETUP_UTILS.getServicePort());
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "createSegmentTest", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore create-segment segmentstore/createSegmentTest/5.#epoch.0", STATE.get());
        Assert.assertTrue(commandResult.contains("created successfully"));

        Assert.assertNotNull(CreateSegmentCommand.descriptor());
    }

    @Test
    public void testRemoveTableSegmentKeysCommandIncorrectKey() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", SETUP_UTILS.getServicePort());
        String tableSegmentName = getMetadataSegmentName(0);
        String key = "invalidKey";
        String commandResult = TestUtils.executeCommand("table-segment remove-key " + tableSegmentName + " " + key, STATE.get());
        Assert.assertTrue(commandResult.contains("RemoveTableKey failed: " + key + " does not exist"));
    }

    @Test
    public void testRemoveTableSegmentKeysCommand() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", SETUP_UTILS.getServicePort());
        String tableSegmentName = getMetadataSegmentName(0);
        String key = "_system/_RGkvtStreamReaders/0.#epoch.0";
        String commandResult = TestUtils.executeCommand("table-segment remove-key " + tableSegmentName + " " + key, STATE.get());
        Assert.assertTrue(commandResult.contains("RemoveTableKey: " + key + " removed successfully from " + tableSegmentName));
    }

    @After
    public void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
        STATE.get().close();
    }

    //endregion

    //region Actual Test Implementations

    public static class SecureSegmentStoreCommandsTest extends AbstractSegmentStoreCommandsTest {
        @Before
        public void startUp() throws Exception {
            setup(true, true);
        }
    }

    public static class SegmentStoreCommandsTest extends AbstractSegmentStoreCommandsTest {
        @Before
        public void startUp() throws Exception {
            setup(false, false);
        }
    }

    //endregion
}
