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

import io.pravega.cli.admin.AbstractAdminCommandTest;
import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.controller.ControllerListScopesCommand;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.*;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static io.pravega.cli.admin.utils.TestUtils.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class SecSegmentStoreCommandsTest {

    private static ClusterWrapper CLUSTER;
    boolean authEnable;
    private static boolean tlsEnable;
    private static AdminCommandState STATE;

    public void setupSegmentStore(boolean isSecure) {
//        if(isSecure) {
//            authEnable = true;
//            tlsEnable = true;
//        } else {
//            authEnable = false;
//            tlsEnable = false;
//        }
        this.authEnable = false;
        this.tlsEnable = false;

        CLUSTER = createPravegaCluster(authEnable, tlsEnable);
        CLUSTER.start();
        log.info("In test auth is {}, and tls is {}", authEnable, tlsEnable);
        STATE = createAdminCLIConfig(getCLIControllerRestUri(CLUSTER.controllerRestUri()),
                getCLIControllerUri(CLUSTER.controllerUri()), CLUSTER.zookeeperConnectString(), CLUSTER.getContainerCount()
                , authEnable, tlsEnable, CLUSTER.getAccessTokenTtlInSeconds());
        String scope = "testScope";
        String testStream = "getinfo";
        ClientConfig clientConfig = prepareValidClientConfig(CLUSTER.controllerUri(), authEnable, tlsEnable);

        // Generate the scope and stream required for testing.
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scope);

        // Check if scope created successfully.
        assertTrue("Failed to create scope", isScopeCreated);

        boolean isStreamCreated = streamManager.createStream(scope, testStream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());

        // Check if stream created successfully.
        assertTrue("Failed to create the stream ", isStreamCreated);
    }


    // The controller REST URI is generated only after the Pravega cluster has been started. So to maintain STATE as
    // static final, we use this instead of @BeforeClass.


    protected AdminCommandState cliConfig() {
        return STATE;
    }

    @AfterClass
    public static void shutDown() {
        if (CLUSTER != null) {
            CLUSTER.close();
        }
        STATE.close();
    }

    @Test
    @SneakyThrows
    public void testSegmentReadCommand() {
        String commandResult = TestUtils.executeCommand("segmentstore read-segment _system/_RGcommitStreamReaders/0.#epoch.0 0 8 localhost", cliConfig());
        Assert.assertTrue(commandResult.contains("ReadSegment:"));
    }

//    @Test
//    @SneakyThrows
//    public void testSegmentInfoCommand() {
//        String commandResult = TestUtils.executeCommand("segmentstore get-segment-info segmentstore/getinfo/0.#epoch.0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_abortStream/0.#epoch.0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_requeststream/0.#epoch.0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGcommitStreamReaders/0.#epoch.0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGscaleGroup/0.#epoch.0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGkvtStreamReaders/0.#epoch.0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGabortStreamReaders/0.#epoch.0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/containers/metadata_0 localhost", cliConfig());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore get-segment-info not/exists/0 localhost", cliConfig()));
//        Assert.assertNotNull(GetSegmentInfoCommand.descriptor());
//    }

//
//    @Test
//    @SneakyThrows
//    public void testAuthConfig() {
//        String scope = "testScope";
//        Properties pravegaProperties = new Properties();
//        pravegaProperties.setProperty("cli.security.auth.enable", "true");
//        pravegaProperties.setProperty("cli.security.auth.credentials.username", "admin");
//        pravegaProperties.setProperty("cli.security.auth.credentials.password", "1111_aaaa");
//        cliConfig().getConfigBuilder().include(pravegaProperties);
//        String commandResult = TestUtils.executeCommand("controller list-scopes", cliConfig());
//        // Check that both the new scope and the system one exist.
//        Assert.assertTrue(commandResult.contains("_system"));
//        Assert.assertTrue(commandResult.contains(scope));
//        Assert.assertNotNull(ControllerListScopesCommand.descriptor());
//        // Restore config
//        pravegaProperties.setProperty("cli.security.auth.enable", "false");
//        cliConfig().getConfigBuilder().include(pravegaProperties);
//
//        // Exercise response codes for REST requests.
//        @Cleanup
//        val c1 = new AdminCommandState();
//        CommandArgs commandArgs = new CommandArgs(Collections.emptyList(), c1);
//        ControllerListScopesCommand command = new ControllerListScopesCommand(commandArgs);
////        command.printResponseInfo(Response.status(Response.Status.UNAUTHORIZED).build());
////        command.printResponseInfo(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
//    }

//    @Test
//    public void testGetSegmentInfoCommand() throws Exception {
//        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getinfo", StreamConfiguration.builder().build());
//        String commandResult = TestUtils.executeCommand("segmentstore get-segment-info segmentstore/getinfo/0.#epoch.0 localhost", STATE.get());
//
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_abortStream/0.#epoch.0 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_requeststream/0.#epoch.0 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGcommitStreamReaders/0.#epoch.0 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGscaleGroup/0.#epoch.0 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGkvtStreamReaders/0.#epoch.0 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGabortStreamReaders/0.#epoch.0 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/containers/metadata_0 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
//        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore get-segment-info not/exists/0 localhost", STATE.get()));
//        Assert.assertNotNull(GetSegmentInfoCommand.descriptor());
//    }
//
//    @Test
//    public void testReadSegmentRangeCommand() throws Exception {
//        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "readsegment", StreamConfiguration.builder().build());
//        ClientConfig clientConfig = ClientConfig.builder().controllerURI(SETUP_UTILS.getControllerUri()).build();
//        @Cleanup
//        EventStreamClientFactory factory = EventStreamClientFactory.withScope("segmentstore", clientConfig);
//        @Cleanup
//        EventStreamWriter<String> writer = factory.createEventWriter("readsegment", new JavaSerializer<>(), EventWriterConfig.builder().build());
//        writer.writeEvents("rk", Arrays.asList("a", "2", "3"));
//        writer.flush();
//        String commandResult = TestUtils.executeCommand("segmentstore read-segment segmentstore/readsegment/0.#epoch.0 0 8 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("ReadSegment:"));
//        commandResult = TestUtils.executeCommand("segmentstore read-segment _system/_RGcommitStreamReaders/0.#epoch.0 0 8 localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("ReadSegment:"));
//        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore read-segment not/exists/0 0 1 localhost", STATE.get()));
//        Assert.assertNotNull(ReadSegmentRangeCommand.descriptor());
//    }
//
//    @Test
//    public void testGetSegmentAttributeCommand() throws Exception {
//        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getattribute", StreamConfiguration.builder().build());
//        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/getattribute/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
//        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore get-segment-attribute not/exists/0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get()));
//        Assert.assertNotNull(GetSegmentAttributeCommand.descriptor());
//    }
//
//    @Test
//    public void testUpdateSegmentAttributeCommand() throws Exception {
//        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "updateattribute", StreamConfiguration.builder().build());
//        // First, get the existing value of that attribute for the segment.
//        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
//        long oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
//        Assert.assertNotEquals(0L, oldValue);
//        // Update the Segment to a value of 0.
//        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 " + oldValue + " localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("UpdateSegmentAttribute:"));
//        // Check that the value has been updated.
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
//        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
//        Assert.assertEquals(0L, oldValue);
//
//        // Do the same for an internal segment.
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
//        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
//        Assert.assertNotEquals(0L, oldValue);
//        // Update the Segment to a value of 0.
//        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute _system/_abortStream/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 " + oldValue + " localhost", STATE.get());
//        Assert.assertTrue(commandResult.contains("UpdateSegmentAttribute:"));
//        // Check that the value has been updated.
//        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
//        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
//        Assert.assertEquals(0L, oldValue);
//
//        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore update-segment-attribute not/exists/0 "
//                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 0 localhost", STATE.get()));
//        Assert.assertNotNull(UpdateSegmentAttributeCommand.descriptor());
//    }



    //endregion

    //region Actual Test Implementations

    public static class SecureSegmentStoreCommandsTest extends SecSegmentStoreCommandsTest {
        @Before
        public void startUp() throws Exception {
            setupSegmentStore(true);

            log.info("After setup is called auth is ", super.authEnable);
        }
    }

    public static class RegularSegmentStoreCommandsTest extends SecSegmentStoreCommandsTest {
        @Before
        public void startUp() throws Exception {
            setupSegmentStore(false);
        }
    }

    //endregion
}
