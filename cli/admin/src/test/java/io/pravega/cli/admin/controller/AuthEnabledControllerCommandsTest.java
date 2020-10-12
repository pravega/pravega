package io.pravega.cli.admin.controller;

import io.pravega.cli.admin.AbstractSecureAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AuthEnabledControllerCommandsTest extends AbstractSecureAdminCommandTest {

    @Before
    @Override
    public void setUp() throws Exception {
        this.AUTH_ENABLED = true;
        super.setUp();
    }

    @Override
    public ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))

                // Auth-related
                .credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                        SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                .build();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testListScopesCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-scopes", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
    }

    @Test
    public void testListStreamsCommand() throws Exception {
        String scope = "testScope";
        String testStream = "testStream";
        ClientConfig clientConfig = prepareValidClientConfig();

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
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);

        String commandResult = TestUtils.executeCommand("controller list-streams " + scope, STATE.get());
        Assert.assertTrue(commandResult.contains(testStream));
    }

    @Test
    public void testListReaderGroupsCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller list-readergroups _system", STATE.get());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
    }

    @Test
    public void testDescribeScopeCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("controller describe-scope _system", STATE.get());
        Assert.assertTrue(commandResult.contains("_system"));
    }
}
