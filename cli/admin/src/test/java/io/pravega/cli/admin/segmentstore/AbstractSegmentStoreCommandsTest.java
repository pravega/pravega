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

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.test.integration.utils.TestUtils.pathToConfig;


/**
 *  This test is for testing the segment store cli commands.
 */
public abstract class AbstractSegmentStoreCommandsTest {
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
        pravegaProperties.setProperty("pravegaservice.container.count", "4");
        pravegaProperties.setProperty("pravegaservice.admin.gateway.port", String.valueOf(SETUP_UTILS.getAdminPort()));

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
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "readsegment", StreamConfiguration.builder().build());

        @Cleanup
        EventStreamClientFactory factory = EventStreamClientFactory.withScope("segmentstore", clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = factory.createEventWriter("readsegment", new JavaSerializer<>(), EventWriterConfig.builder().build());
        writer.writeEvents("rk", Arrays.asList("a", "2", "3"));
        writer.flush();
        String commandResult = TestUtils.executeCommand("segmentstore read-segment segmentstore/readsegment/0.#epoch.0 0 8 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("ReadSegment:"));
        commandResult = TestUtils.executeCommand("segmentstore read-segment _system/_RGcommitStreamReaders/0.#epoch.0 0 8 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("ReadSegment:"));
        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore read-segment not/exists/0 0 1 localhost", STATE.get()));
        Assert.assertNotNull(ReadSegmentRangeCommand.descriptor());
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