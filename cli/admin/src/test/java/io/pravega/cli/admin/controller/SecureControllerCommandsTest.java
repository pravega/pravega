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

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.test.integration.utils.ClusterWrapper;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Properties;

import static io.pravega.cli.admin.utils.TestUtils.createAdminCLIConfig;
import static io.pravega.cli.admin.utils.TestUtils.createPravegaCluster;
import static io.pravega.cli.admin.utils.TestUtils.getCLIControllerRestUri;
import static io.pravega.cli.admin.utils.TestUtils.getCLIControllerUri;
import static io.pravega.cli.admin.utils.TestUtils.prepareValidClientConfig;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SecureControllerCommandsTest {
    private static final ClusterWrapper CLUSTER = createPravegaCluster(true, true);
    private static final AdminCommandState STATE;

    // The controller REST URI is generated only after the Pravega cluster has been started. So to maintain STATE as
    // static final, we use this instead of @BeforeClass.
    static {
        CLUSTER.start();
        STATE = createAdminCLIConfig(getCLIControllerRestUri(CLUSTER.controllerRestUri()),
                getCLIControllerUri(CLUSTER.controllerUri()), CLUSTER.zookeeperConnectString(), CLUSTER.getContainerCount(),
                true, true, CLUSTER.getAccessTokenTtl());
        String scope = "testScope";
        String testStream = "testStream";
        ClientConfig clientConfig = prepareValidClientConfig(CLUSTER.controllerUri(), true, true);

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
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                                               .stream(Stream.of("testScope", "testStream"))
                                                               .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                                                               .automaticCheckpointIntervalMillis(1000L)
                                                               .groupRefreshTimeMillis(1000L)
                                                               .build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope("testScope", clientConfig);
        boolean isRGCreated = readerGroupManager.createReaderGroup("testRG", readerGroupConfig);
        assertTrue("Failed to create reader group ", isRGCreated);
    }

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
    public void testListScopesCommand() {
        String commandResult = TestUtils.executeCommand("controller list-scopes", cliConfig());
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    @SneakyThrows
    public void testListStreamsCommand() {
        String commandResult = TestUtils.executeCommand("controller list-streams testScope", cliConfig());
        Assert.assertTrue(commandResult.contains("testStream"));
    }

    @Test
    @SneakyThrows
    public void testListReaderGroupsCommand() {
        String commandResult = TestUtils.executeCommand("controller list-readergroups _system", cliConfig());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
    }

    @Test
    @SneakyThrows
    public void testDescribeScopeCommand() {
        String commandResult = TestUtils.executeCommand("controller describe-scope _system", cliConfig());
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    @SneakyThrows
    public void testAuthConfig() {
        String scope = "testScope";
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.security.auth.enable", "true");
        pravegaProperties.setProperty("cli.security.auth.credentials.username", "admin");
        pravegaProperties.setProperty("cli.security.auth.credentials.password", "1111_aaaa");
        cliConfig().getConfigBuilder().include(pravegaProperties);
        String commandResult = TestUtils.executeCommand("controller list-scopes", cliConfig());
        // Check that both the new scope and the system one exist.
        Assert.assertTrue(commandResult.contains("_system"));
        Assert.assertTrue(commandResult.contains(scope));
        Assert.assertNotNull(ControllerListScopesCommand.descriptor());
        // Restore config
        pravegaProperties.setProperty("cli.security.auth.enable", "false");
        cliConfig().getConfigBuilder().include(pravegaProperties);

        // Exercise response codes for REST requests.
        @Cleanup
        val c1 = new AdminCommandState();
        CommandArgs commandArgs = new CommandArgs(Collections.emptyList(), c1);
        ControllerListScopesCommand command = new ControllerListScopesCommand(commandArgs);
        command.printResponseInfo(Response.status(Response.Status.UNAUTHORIZED).build());
        command.printResponseInfo(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
    }

    @Test
    @SneakyThrows
    public void testDescribeReaderGroupCommand() {
        // Check that the system reader group can be listed.
        String commandResult = TestUtils.executeCommand("controller describe-readergroup _system commitStreamReaders", cliConfig());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
        Assert.assertNotNull(ControllerDescribeReaderGroupCommand.descriptor());
    }

    @Test
    @SneakyThrows
    public void testDescribeStreamCommand() {
        String commandResult = TestUtils.executeCommand("controller describe-stream testScope testStream", cliConfig());
        Assert.assertTrue(commandResult.contains("testStream"));
    }


    @Test
    @SneakyThrows
    public void testDeleteReaderGroupCommand() {
        String commandResult = TestUtils.executeCommand("controller delete-readergroup _system testRG", cliConfig());
        Assert.assertTrue(commandResult.contains("404"));
        Assert.assertNotNull(ControllerDescribeReaderGroupCommand.descriptor());

        //execute the delete reader group command
        commandResult = TestUtils.executeCommand("controller delete-readergroup testScope testRG", cliConfig());
        Assert.assertTrue(commandResult.contains("Successful REST request."));
        //verify that reader group not exists after api call
        commandResult = TestUtils.executeCommand("controller list-readergroups testScope", cliConfig());
        Assert.assertFalse(commandResult.contains("testRG"));
    }
}

