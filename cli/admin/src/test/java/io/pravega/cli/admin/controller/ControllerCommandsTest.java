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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.cli.admin.utils.TestUtils.createAdminCLIConfig;
import static io.pravega.cli.admin.utils.TestUtils.createPravegaCluster;
import static io.pravega.cli.admin.utils.TestUtils.getCLIControllerRestUri;
import static io.pravega.cli.admin.utils.TestUtils.getCLIControllerUri;
import static io.pravega.cli.admin.utils.TestUtils.prepareValidClientConfig;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Validate basic controller commands.
 */
public class ControllerCommandsTest extends SecureControllerCommandsTest {
    private static final ClusterWrapper CLUSTER = createPravegaCluster(false, false);
    private static final AdminCommandState STATE;
    private static final ClientConfig CLIENT_CONFIG;

    // The controller REST URI is generated only after the Pravega cluster has been started. So to maintain STATE as
    // static final, we use this instead of @BeforeClass.
    static {
        CLUSTER.start();
        STATE = createAdminCLIConfig(getCLIControllerRestUri(CLUSTER.controllerRestUri()),
                getCLIControllerUri(CLUSTER.controllerUri()), CLUSTER.zookeeperConnectString(), CLUSTER.getContainerCount(),
                false, false, CLUSTER.getAccessTokenTtl());
        String scope = "testScope";
        String testStream = "testStream";
        CLIENT_CONFIG = prepareValidClientConfig(CLUSTER.controllerUri(), false, false);

        // Generate the scope and stream required for testing.
        @Cleanup
        StreamManager streamManager = StreamManager.create(CLIENT_CONFIG);
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
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope("testScope", CLIENT_CONFIG);
        boolean isRGCreated = readerGroupManager.createReaderGroup("testRG", readerGroupConfig);
        assertTrue("Failed to create reader group ", isRGCreated);
    }

    @Override
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

    @Override
    @Test
    @SneakyThrows
    public void testDescribeReaderGroupCommand() {
        // Check that the system reader group can be listed.
        String commandResult = TestUtils.executeCommand("controller describe-readergroup _system commitStreamReaders", cliConfig());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
        Assert.assertNotNull(ControllerDescribeReaderGroupCommand.descriptor());
    }

}
