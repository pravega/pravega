/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin;

import io.pravega.cli.admin.controller.ControllerDescribeStreamCommand;
import io.pravega.cli.admin.controller.ControllerListReaderGroupsInScopeCommand;
import io.pravega.cli.admin.controller.ControllerListScopesCommand;
import io.pravega.cli.admin.controller.ControllerListStreamsInScopeCommand;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTlsAdminCommandTest {

    @Rule
    public final Timeout globalTimeout = new Timeout(80, TimeUnit.SECONDS);

    protected final AtomicReference<AdminCommandState> state = new AtomicReference<>();
    protected boolean authEnabled = false;
    protected boolean tlsEnabled = false;
    LocalPravegaEmulator localPravega;
    private final String location = "../../config/";

    // Security related flags and instantiate local Pravega server.
    private final Integer controllerPort = TestUtils.getAvailableListenPort();
    private final Integer segmentStorePort = TestUtils.getAvailableListenPort();
    private final Integer restServerPort = TestUtils.getAvailableListenPort();

    @Before
    @SneakyThrows
    public void setUp() {

        // Create the secure Pravega server to test commands against.
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(controllerPort)
                .segmentStorePort(segmentStorePort)
                .zkPort(TestUtils.getAvailableListenPort())
                .restServerPort(TestUtils.getAvailableListenPort())
                .enableRestServer(true)
                .restServerPort(restServerPort)
                .enableAuth(authEnabled)
                .enableTls(tlsEnabled);

        // Since the server is being built right here, avoiding delegating these conditions to subclasses via factory
        // methods. This is so that it is easy to see the difference in server configs all in one place. This is also
        // unlike the ClientConfig preparation which is being delegated to factory methods to make their preparation
        // explicit in the respective test classes.

        if (authEnabled) {
            emulatorBuilder.passwdFile(location + SecurityConfigDefaults.AUTH_HANDLER_INPUT_FILE_NAME)
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }

        if (tlsEnabled) {
            emulatorBuilder.certFile(location + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                    .keyFile(location + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                    .jksKeyFile(location + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME)
                    .jksTrustFile(location + SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_NAME)
                    .keyPasswordFile(location + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME);
        }

        localPravega = emulatorBuilder.build();
        localPravega.start();

        // Set the CLI properties.
        state.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", "localhost:" + restServerPort.toString());
        pravegaProperties.setProperty("cli.controller.grpc.uri", "localhost:" + controllerPort.toString());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", localPravega.getInProcPravegaCluster().getZkUrl());
        pravegaProperties.setProperty("pravegaservice.container.count", "4");
        pravegaProperties.setProperty("cli.security.auth.enable", Boolean.toString(authEnabled));
        pravegaProperties.setProperty("cli.security.auth.credentials.username", SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
        pravegaProperties.setProperty("cli.security.auth.credentials.password", SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        pravegaProperties.setProperty("cli.security.auth.token.signingKey", "secret");
        pravegaProperties.setProperty("cli.security.tls.enable", Boolean.toString(tlsEnabled));
        pravegaProperties.setProperty("cli.security.tls.trustStore.location", location + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);

        state.get().getConfigBuilder().include(pravegaProperties);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        if (localPravega != null) {
            localPravega.close();
        }
    }

    protected ClientConfig prepareValidClientConfig() {
        ClientConfig.ClientConfigBuilder clientBuilder = ClientConfig.builder()
                .controllerURI(URI.create(this.localPravega.getInProcPravegaCluster().getControllerURI()));
        if (authEnabled) {
            clientBuilder.credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                    SecurityConfigDefaults.AUTH_ADMIN_USERNAME));
        }
        if (tlsEnabled) {
            clientBuilder.trustStore(location + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME)
                    .validateHostName(false);
        }
        return clientBuilder.build();
    }

    @Test
    @SneakyThrows
    public void testAllCommands() {
        String scope = "testScope";
        String testStream = "testStream";
        String readerGroup = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, testStream))
                .disableAutomaticCheckpoints()
                .build();
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
        assertTrue("Failed to create the stream ", isStreamCreated);

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

        String commandResult = io.pravega.cli.admin.utils.TestUtils.executeCommand("controller list-scopes", state.get());
        assertTrue("ListScopesCommand failed.", commandResult.contains(scope));
        assertNotNull(ControllerListScopesCommand.descriptor());

        commandResult = io.pravega.cli.admin.utils.TestUtils.executeCommand("controller list-streams " + scope, state.get());
        assertTrue("ListStreamsCommand failed.", commandResult.contains(testStream));
        assertNotNull(ControllerListStreamsInScopeCommand.descriptor());

        commandResult = io.pravega.cli.admin.utils.TestUtils.executeCommand("controller list-readergroups " + scope, state.get());
        assertTrue("ListReaderGroupsCommand failed.", commandResult.contains(readerGroup));
        assertNotNull(ControllerListReaderGroupsInScopeCommand.descriptor());

        commandResult = io.pravega.cli.admin.utils.TestUtils.executeCommand("controller describe-scope " + scope, state.get());
        assertTrue("DescribeScopeCommand failed.", commandResult.contains(scope));
        assertNotNull(ControllerDescribeStreamCommand.descriptor());
    }
}
