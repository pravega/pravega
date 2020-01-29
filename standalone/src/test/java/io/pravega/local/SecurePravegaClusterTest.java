/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import java.net.URI;

import io.pravega.test.common.SecurityConfigDefaults;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;

/**
 * This class holds tests for TLS and auth enabled in-process standalone cluster. It inherits the test methods defined
 * in the parent class.
 */
@Slf4j
public class SecurePravegaClusterTest extends InProcPravegaClusterTest {
    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = true;
        this.tlsEnabled = true;
        super.setUp();
    }

    @Override
    String scopeName() {
        return "TlsAndAuthTestScope";
    }

    @Override
    String streamName() {
        return "TlsAndAuthTestStream";
    }

    @Override
    String eventMessage() {
        return "Test message on the encrypted channel with auth credentials";
    }

    @Override
    ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))

                // TLS-related
                .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                .validateHostName(false)

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
}
