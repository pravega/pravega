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
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AbstractSecureAdminCommandTest {

    // Security related flags and instantiate local pravega server.
    private static final boolean REST_ENABLED = true;
    protected boolean AUTH_ENABLED = false;
    protected boolean TLS_ENABLED = false;
    private static final Integer CONTROLLER_PORT = 9090;
    private static final Integer REST_SERVER_PORT = 9091;
    private static final Integer SEGMENT_STORE_PORT = 6000;
    protected LocalPravegaEmulator localPravega;
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {

        // Create the secure pravega server to test commands against.
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(CONTROLLER_PORT)
                .segmentStorePort(SEGMENT_STORE_PORT)
                .zkPort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .restServerPort(io.pravega.test.common.TestUtils.getAvailableListenPort())
                .enableRestServer(REST_ENABLED)
                .enableAuth(AUTH_ENABLED)
                .enableTls(TLS_ENABLED)
                .restServerPort(REST_SERVER_PORT);

        // Since the server is being built right here, avoiding delegating these conditions to subclasses via factory
        // methods. This is so that it is easy to see the difference in server configs all in one place. This is also
        // unlike the ClientConfig preparation which is being delegated to factory methods to make their preparation
        // explicit in the respective test classes.

        if (AUTH_ENABLED) {
            emulatorBuilder.passwdFile("../" + SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }
        if (TLS_ENABLED) {
            emulatorBuilder.certFile("../" + SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                    .keyFile("../" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                    .jksKeyFile("../" + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_PATH)
                    .jksTrustFile("../" + SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_PATH)
                    .keyPasswordFile("../" + SecurityConfigDefaults.TLS_PASSWORD_PATH);
        }

        localPravega = emulatorBuilder.build();

        // The uri returned by LocalPravegaEmulator is in the form tcp://localhost:9090 (protocol + domain + port)
        // but for the CLI we need to set the GRPC uri as localhost:9090 (domain + port). Because the protocol
        // is decided based on whether security is enabled or not.

        // Set the CLI properties.
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", "localhost:" + REST_SERVER_PORT.toString());
        pravegaProperties.setProperty("cli.controller.grpc.uri", "localhost:" + CONTROLLER_PORT.toString());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", localPravega.getInProcPravegaCluster().getZkUrl());
        pravegaProperties.setProperty("pravegaservice.container.count", "4");
        pravegaProperties.setProperty("cli.security.auth.enable", Boolean.toString(AUTH_ENABLED));
        pravegaProperties.setProperty("cli.security.auth.credentials.username", "admin");
        pravegaProperties.setProperty("cli.security.auth.credentials.password", "1111_aaaa");
        pravegaProperties.setProperty("cli.security.tls.enable", Boolean.toString(TLS_ENABLED));
        pravegaProperties.setProperty("cli.security.tls.trustStore.location", "../" + SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_PATH);
        STATE.get().getConfigBuilder().include(pravegaProperties);

        localPravega.start();

        // Wait for the server to complete start-up.
        TimeUnit.SECONDS.sleep(20);
    }

    public ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))

                // TLS-related
                .trustStore("../" + SecurityConfigDefaults.TLS_CA_CERT_PATH)
                .validateHostName(false)

                // Auth-related
                .credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                        SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                .build();
    }

    @After
    public void tearDown() throws Exception {
        if (localPravega != null) {
            localPravega.close();
        }
    }
}
