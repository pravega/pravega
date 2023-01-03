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
package io.pravega.cli.admin.readerGroup;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.SetupUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.test.integration.utils.TestUtils.pathToConfig;

public abstract class ParseReaderGroupStreamCommandTest {
    // Setup utility.
    protected static final SetupUtils SETUP_UTILS = new SetupUtils();
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    protected static final int CONTAINER_COUNT = 1;
    private static final String ENDPOINT = "localhost";

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    private ClientConfig clientConfig;

    public void setup(boolean enableAuth, boolean enableTls) throws Exception {
        ClientConfig.ClientConfigBuilder clientConfigBuilder = ClientConfig.builder().controllerURI(SETUP_UTILS.getControllerUri());
        STATE.set(new AdminCommandState());
        SETUP_UTILS.startAllServices(enableAuth, enableTls);
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.grpc.uri", SETUP_UTILS.getControllerUri().toString());
        pravegaProperties.setProperty("cli.controller.connect.grpc.uri", SETUP_UTILS.getControllerUri().getHost() + ":" + SETUP_UTILS.getControllerUri().getPort());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", SETUP_UTILS.getZkTestServer().getConnectString());
        pravegaProperties.setProperty("pravegaservice.container.count", String.valueOf(CONTAINER_COUNT));
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
    public void testParseReaderGroupStreamCommand() throws Exception {
        // Create a temporary directory.
        Path tempDirPath = Files.createTempDirectory("parseRgStreamDir");
        String filename = Paths.get(tempDirPath.toString(), "tmp" + System.currentTimeMillis(), "parseRgStreamTest").toString();

        String commandResult = TestUtils.executeCommand("readerGroup parse-rg-stream _system commitStreamReaders " + ENDPOINT + " " + filename, STATE.get());
        Assert.assertTrue(commandResult.contains("The readerGroup stream has been successfully written into " + filename));
        Assert.assertNotNull(ParseReaderGroupStreamCommand.descriptor());
        File file = new File(filename);
        Assert.assertTrue(file.exists());
        Assert.assertNotEquals(0, file.length());

        // Delete file created during the test.
        Files.deleteIfExists(Paths.get(filename));
        tempDirPath.toFile().deleteOnExit();
    }


    @After
    public void tearDown() throws Exception {
        SETUP_UTILS.stopAllServices();
        STATE.get().close();
    }

    //endregion

    //region Actual Test Implementations

    public static class SecureParseRGStreamCommandsTest extends ParseReaderGroupStreamCommandTest {
        @Before
        public void startUp() throws Exception {
            setup(true, true);
        }
    }

    public static class ParseRGStreamCommandsTest extends ParseReaderGroupStreamCommandTest {
        @Before
        public void startUp() throws Exception {
            setup(false, false);
        }
    }

    //endregion
}