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

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractTlsAdminCommandTest {

    // Security related flags and instantiate local pravega server.
    private static final Integer CONTROLLER_PORT = TestUtils.getAvailableListenPort();
    private static final Integer SEGMENT_STORE_PORT = TestUtils.getAvailableListenPort();
    private static final Integer REST_SERVER_PORT = TestUtils.getAvailableListenPort();

    @Rule
    public final Timeout globalTimeout = new Timeout(80, TimeUnit.SECONDS);

    protected final AtomicReference<AdminCommandState> state = new AtomicReference<>();
    protected boolean authEnabled = false;
    protected boolean tlsEnabled = false;
    LocalPravegaEmulator localPravega;

    @Before
    public void setUp() throws Exception {

        // Create the secure pravega server to test commands against.
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(CONTROLLER_PORT)
                .segmentStorePort(SEGMENT_STORE_PORT)
                .zkPort(TestUtils.getAvailableListenPort())
                .restServerPort(TestUtils.getAvailableListenPort())
                .enableRestServer(true)
                .restServerPort(REST_SERVER_PORT)
                .enableAuth(authEnabled)
                .enableTls(tlsEnabled);

        // Since the server is being built right here, avoiding delegating these conditions to subclasses via factory
        // methods. This is so that it is easy to see the difference in server configs all in one place. This is also
        // unlike the ClientConfig preparation which is being delegated to factory methods to make their preparation
        // explicit in the respective test classes.

        if (authEnabled) {
            emulatorBuilder.passwdFile("../../config/" + SecurityConfigDefaults.AUTH_HANDLER_INPUT_FILE_NAME)
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }

        if (tlsEnabled) {
            emulatorBuilder.certFile("../../config/" + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                    .keyFile("../../config/" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                    .jksKeyFile("../../config/" + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME)
                    .jksTrustFile("../../config/" + SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_NAME)
                    .keyPasswordFile("../../config/" + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME);
        }

        localPravega = emulatorBuilder.build();
        localPravega.start();

        // Set the CLI properties.
        state.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", "localhost:" + REST_SERVER_PORT.toString());
        pravegaProperties.setProperty("cli.controller.grpc.uri", "localhost:" + CONTROLLER_PORT.toString());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", localPravega.getInProcPravegaCluster().getZkUrl());
        pravegaProperties.setProperty("pravegaservice.container.count", "4");
        pravegaProperties.setProperty("cli.security.auth.enable", Boolean.toString(authEnabled));
        pravegaProperties.setProperty("cli.security.auth.credentials.username", SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
        pravegaProperties.setProperty("cli.security.auth.credentials.password", SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        pravegaProperties.setProperty("cli.security.tls.enable", Boolean.toString(tlsEnabled));
        pravegaProperties.setProperty("cli.security.tls.trustStore.location", "../../config/" + SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_NAME);

        state.get().getConfigBuilder().include(pravegaProperties);
    }

    @After
    public void tearDown() throws Exception {
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
            clientBuilder.trustStore("../../config/" + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME)
                    .validateHostName(false);
        }
        return clientBuilder.build();
    }
}
