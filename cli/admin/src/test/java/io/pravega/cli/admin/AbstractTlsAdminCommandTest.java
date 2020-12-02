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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.cli.admin.utils.TestUtils.pathToConfig;
import static io.pravega.cli.admin.utils.TestUtils.setAdminCLIProperties;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTlsAdminCommandTest {

    protected static final AtomicReference<LocalPravegaEmulator> LOCAL_PRAVEGA = new AtomicReference<>();
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    @Rule
    public final Timeout globalTimeout = new Timeout(80, TimeUnit.SECONDS);

    public static void setUpLocalPravegaEmulator(boolean authEnabled, boolean tlsEnabled) throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final int segmentStorePort = TestUtils.getAvailableListenPort();
        final int restServerPort = TestUtils.getAvailableListenPort();
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
            emulatorBuilder.passwdFile(pathToConfig() + SecurityConfigDefaults.AUTH_HANDLER_INPUT_FILE_NAME)
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }

        if (tlsEnabled) {
            emulatorBuilder.certFile(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                    .keyFile(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                    .jksKeyFile(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME)
                    .jksTrustFile(pathToConfig() + SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_NAME)
                    .keyPasswordFile(pathToConfig() + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME);
        }
        LOCAL_PRAVEGA.set(emulatorBuilder.build());
        LOCAL_PRAVEGA.get().start();
        setAdminCLIProperties("localhost:" + restServerPort,
                "localhost:" + controllerPort,
                LOCAL_PRAVEGA.get().getInProcPravegaCluster().getZkUrl(),
                4, authEnabled, "secret", tlsEnabled, STATE);

        String scope = "testScope";
        String testStream = "testStream";
        ClientConfig clientConfig = prepareValidClientConfig(authEnabled, tlsEnabled);

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
    }

    @BeforeClass
    @SneakyThrows
    public static void setUp() {
        setUpLocalPravegaEmulator(false, true);
    }

    @AfterClass
    @SneakyThrows
    public static void tearDown() {
        val localPravega = LOCAL_PRAVEGA.get();
        if (LOCAL_PRAVEGA.get() != null) {
            localPravega.close();
        }
    }

    protected static ClientConfig prepareValidClientConfig(boolean authEnabled, boolean tlsEnabled) {
        ClientConfig.ClientConfigBuilder clientBuilder = ClientConfig.builder()
                .controllerURI(URI.create(LOCAL_PRAVEGA.get().getInProcPravegaCluster().getControllerURI()));
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
}
