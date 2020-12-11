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
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.Cleanup;
import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.cli.admin.utils.TestUtils.createPravegaCluster;
import static io.pravega.cli.admin.utils.TestUtils.pathToConfig;
import static io.pravega.cli.admin.utils.TestUtils.setAdminCLIProperties;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractAdminCommandTest {

    protected static final AtomicReference<ClusterWrapper> CLUSTER = new AtomicReference<>();
    protected static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();
    @Rule
    public final Timeout globalTimeout = new Timeout(80, TimeUnit.SECONDS);

    public static void setUpCluster(boolean authEnabled, boolean tlsEnabled) throws Exception {
        CLUSTER.set(createPravegaCluster(authEnabled, tlsEnabled));
        CLUSTER.get().start();
        setAdminCLIProperties(CLUSTER.get().controllerRestUri().replace("http://", "").replace("https://", ""),
                CLUSTER.get().controllerUri().replace("tcp://", "").replace("tls://", ""),
                CLUSTER.get().zookeeperConnectString(),
                CLUSTER.get().getContainerCount(),
                authEnabled,
                CLUSTER.get().getTokenSigningKeyBasis(),
                tlsEnabled,
                STATE);

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
    public static void setUp() throws Exception {
        setUpCluster(false, true);
    }

    @AfterClass
    public static void tearDown() {
        val cluster = CLUSTER.getAndSet(null);
        if (cluster != null) {
            cluster.close();
        }
        STATE.get().close();
    }

    protected static ClientConfig prepareValidClientConfig(boolean authEnabled, boolean tlsEnabled) {
        ClientConfig.ClientConfigBuilder clientBuilder = ClientConfig.builder()
                .controllerURI(URI.create(CLUSTER.get().controllerUri()));
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
