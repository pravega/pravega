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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.SetupUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
                getScopedStreamName(scope, stream) + " false localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("For the given key: %s", getScopedStreamName(scope, stream))));
    }

    @Test
    public void testGetControllerMetadataEntryCommandKeyDoesNotExist() throws Exception {
        String scope = "controllerMetadata2";
        String stream = "getEntryNoKey";
        String dummyStream = "getEntryDummy";
        TestUtils.createScopeStream(SETUP_UTILS.getController(), scope, stream, StreamConfiguration.builder().build());
        TestUtils.deleteScopeStream(SETUP_UTILS.getController(), scope, stream);

        String commandResult = TestUtils.executeCommand("controller-metadata get " + DELETED_STREAMS_TABLE + " " +
                getScopedStreamName(scope, dummyStream) + " false localhost", STATE.get());
        Assert.assertTrue(commandResult.contains(String.format("Key not found: %s", getScopedStreamName(scope, dummyStream))));
    }

    @Test
    public void testGetControllerMetadataEntryCommandTableDoesNotExist() throws Exception {
        String dummyTable = getQualifiedTableName(INTERNAL_SCOPE_NAME,
                "randScope", "randStream", String.format(METADATA_TABLE, UUID.randomUUID()));
        String commandResult = TestUtils.executeCommand("controller-metadata get " + dummyTable + " creationTime false localhost", STATE.get());
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
