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
package io.pravega.cli.admin.utils;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.Parser;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.ZKHostStore;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.ClusterWrapper;
import io.pravega.test.integration.utils.LocalServiceStarter;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Class to contain convenient utilities for writing test cases.
 */
@Slf4j
public final class TestUtils {

    private static final int NUM_EVENTS = 10;
    private static final String EVENT = "12345";
    private static final Duration READ_TIMEOUT = Duration.ofMillis(1000);

    /**
     * Invoke any command and get the result by using a mock PrintStream object (instead of System.out). The returned
     * String is the output written by the Command that can be check in any test.
     *
     * @param inputCommand Command to execute.
     * @param state        Configuration to execute the command.
     * @return             Output of the command.
     * @throws Exception   If a problem occurs.
     */
    public static String executeCommand(String inputCommand, AdminCommandState state) throws Exception {
        Parser.Command pc = Parser.parse(inputCommand);
        CommandArgs args = new CommandArgs(pc.getArgs(), state);
        AdminCommand cmd = AdminCommand.Factory.get(pc.getComponent(), pc.getName(), args);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            cmd.setOut(ps);
            cmd.execute();
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    /**
     * Returns the relative path to `pravega/config` source directory from cli tests.
     *
     * @return the path
     */
    public static String pathToConfig() {
        return "../../config/";
    }

    /**
     * Creates a local Pravega cluster to test on using {@link ClusterWrapper}.
     *
     * @param authEnabled whether accessing the cluster require authentication or not.
     * @param tlsEnabled whether accessing the cluster require TLS or not.
     * @return A local Pravega cluster
     */
    public static ClusterWrapper createPravegaCluster(boolean authEnabled, boolean tlsEnabled) {
        ClusterWrapper.ClusterWrapperBuilder clusterWrapperBuilder = ClusterWrapper.builder();
        if (authEnabled) {
            clusterWrapperBuilder.authEnabled(authEnabled);
        }

        if (tlsEnabled) {
            clusterWrapperBuilder
                    .tlsEnabled(true)
                    .tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                    .tlsServerCertificatePath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                    .tlsServerKeyPath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                    .tlsHostVerificationEnabled(false)
                    .tlsServerKeystorePath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME)
                    .tlsServerKeystorePasswordPath(pathToConfig() + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME)
                    .tokenSigningKeyBasis("secret");
        }
        return clusterWrapperBuilder.controllerRestEnabled(true).build();
    }

    /**
     * Creates the admin state with the necessary CLI properties to use during testing.
     *
     * @param controllerRestUri the controller REST URI.
     * @param controllerUri the controller URI.
     * @param zkConnectUri the zookeeper URI.
     * @param containerCount the container count.
     * @param authEnabled whether the cli requires authentication to access the cluster.
     * @param tlsEnabled whether the cli requires TLS to access the cluster.
     * @param accessTokenTtl how long the access token will last
     */
    @SneakyThrows
    public static AdminCommandState createAdminCLIConfig(String controllerRestUri, String controllerUri, String zkConnectUri,
                                                         int containerCount, boolean authEnabled, boolean tlsEnabled, Duration accessTokenTtl) {
        AdminCommandState state = new AdminCommandState();
        Properties pravegaProperties = new Properties();
        System.out.println("REST URI: " + controllerRestUri);
        pravegaProperties.setProperty("cli.controller.connect.rest.uri", controllerRestUri);
        pravegaProperties.setProperty("cli.controller.connect.grpc.uri", controllerUri);
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", zkConnectUri);
        pravegaProperties.setProperty("pravegaservice.container.count", Integer.toString(containerCount));
        pravegaProperties.setProperty("cli.channel.auth", Boolean.toString(authEnabled));
        pravegaProperties.setProperty("cli.credentials.username", SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
        pravegaProperties.setProperty("cli.credentials.pwd", SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        pravegaProperties.setProperty("cli.channel.tls", Boolean.toString(tlsEnabled));
        pravegaProperties.setProperty("cli.trustStore.location", pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);
        pravegaProperties.setProperty("cli.trustStore.access.token.ttl.seconds", Long.toString(accessTokenTtl.toSeconds()));
        state.getConfigBuilder().include(pravegaProperties);
        return state;
    }

    public static ClientConfig prepareValidClientConfig(String controllerUri, boolean authEnabled, boolean tlsEnabled) {
        ClientConfig.ClientConfigBuilder clientBuilder = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri));
        if (authEnabled) {
            clientBuilder.credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                    SecurityConfigDefaults.AUTH_ADMIN_USERNAME));
        }
        if (tlsEnabled) {
            clientBuilder.trustStore(pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME)
                    .validateHostName(false);
        }
        return clientBuilder.build();
    }

    public static String getCLIControllerUri(String uri) {
        return uri.replace("tcp://", "").replace("tls://", "");
    }

    public static String getCLIControllerRestUri(String uri) {
        return uri.replace("http://", "").replace("https://", "");
    }

    /**
     * This method creates a dummy Host-Container mapping, given that it is not created in Pravega standalone.
     *
     * @param zkConnectString   Connection endpoint for Zookeeper.
     * @param hostIp            Name of the host to connect to.
     * @param hostPort          Port of the host to connect to.
     */
    public static void createDummyHostContainerAssignment(String zkConnectString, String hostIp, int hostPort) {
        @Cleanup
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().namespace("pravega/pravega-cluster")
                .connectString(zkConnectString)
                .retryPolicy(new RetryOneTime(5000)).build();
        curatorFramework.start();
        ZKHostStore zkHostStore = new ZKHostStore(curatorFramework, 4);
        Map<Host, Set<Integer>> dummyHostContainerAssignment = new HashMap<>();
        dummyHostContainerAssignment.put(new Host(hostIp, hostPort, ""), new HashSet<>(Arrays.asList(0, 1, 2, 3)));
        zkHostStore.updateHostContainersMap(dummyHostContainerAssignment);
    }

    /**
     * Creates the given scope and stream using the given controller instance.
     *
     * @param controller    Controller instance to use to create the Scope and Stream.
     * @param scopeName     Name of the Scope.
     * @param streamName    Name of the Stream.
     * @param streamConfig  Configuration for the Stream to be created.
     */
    public static void createScopeStream(Controller controller, String scopeName, String streamName, StreamConfiguration streamConfig) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        @Cleanup
        ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        @SuppressWarnings("resource") //Don't close the controller.
        StreamManager streamManager = new StreamManagerImpl(controller, cp);
        //create scope
        Boolean createScopeStatus = streamManager.createScope(scopeName);
        log.info("Create scope status {}", createScopeStatus);
        //create stream
        Boolean createStreamStatus = streamManager.createStream(scopeName, streamName, streamConfig);
        log.info("Create stream status {}", createStreamStatus);
    }

    /**
     * Deletes the given scope and stream using the given controller instance.
     *
     * @param controller    Controller instance to use to create the Scope and Stream.
     * @param scopeName     Name of the Scope.
     * @param streamName    Name of the Stream.
     */
    public static void deleteScopeStream(Controller controller, String scopeName, String streamName) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        @Cleanup
        ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        @SuppressWarnings("resource") //Don't close the controller.
        StreamManager streamManager = new StreamManagerImpl(controller, cp);
        //delete stream
        Boolean sealStreamStatus = streamManager.sealStream(scopeName, streamName);
        log.info("Seal stream status {}", sealStreamStatus);
        Boolean deleteStreamStatus = streamManager.deleteStream(scopeName, streamName);
        log.info("Delete stream status {}", deleteStreamStatus);
        //create scope
        Boolean deleteScopeStatus = streamManager.deleteScope(scopeName);
        log.info("Delete scope status {}", deleteScopeStatus);
    }

    /**
     * Write events to the given stream.
     *
     * @param streamName     Name of the Stream.
     * @param clientFactory  Client factory to create writers.
     */
    public static void writeEvents(String streamName, ClientFactoryImpl clientFactory) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < NUM_EVENTS; i++) {
            writer.writeEvent(EVENT).join();
        }
        writer.flush();
    }

    /**
     * Read all events from the given stream.
     *
     * @param scope               Scope of the targeted Stream.
     * @param streamName          Name of the Stream.
     * @param clientFactory       ClientFactory to instantiate readers.
     * @param readerGroupManager  ReaderGroupManager to create the ReaderGroup.
     * @param readerGroupName     Name of the ReadeGroup to be created.
     * @param readerName          Name of the Reader to instantiate.
     */
    public static void readAllEvents(String scope, String streamName, ClientFactoryImpl clientFactory, ReaderGroupManager readerGroupManager,
                               String readerGroupName, String readerName) {
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig
                        .builder()
                        .stream(Stream.of(scope, streamName))
                        .build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        for (int q = 0; q < NUM_EVENTS; q++) {
            String eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis()).getEvent();
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead);
        }
    }

    /**
     * Creates a client to read and write events.
     */
    public static class ClientRunner implements AutoCloseable {
        private final ConnectionFactory connectionFactory;
        @Getter
        private final ClientFactoryImpl clientFactory;
        @Getter
        private final ReaderGroupManager readerGroupManager;

        public ClientRunner(LocalServiceStarter.ControllerRunner controllerRunner, String scope) {
            this.connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                    .controllerURI(controllerRunner.getControllerURI()).build());
            this.clientFactory = new ClientFactoryImpl(scope, controllerRunner.getController(), connectionFactory);
            this.readerGroupManager = new ReaderGroupManagerImpl(scope, controllerRunner.getController(), clientFactory);
        }

        @Override
        public void close() {
            this.readerGroupManager.close();
            this.clientFactory.close();
            this.connectionFactory.close();
        }
    }
}
