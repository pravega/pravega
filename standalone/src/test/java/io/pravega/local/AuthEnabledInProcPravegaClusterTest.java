/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.AssertExtensions;
import java.net.URI;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class contains tests for auth enabled in-process standalone cluster. It inherits the test methods defined
 * in the parent class.
 */
@Slf4j
public class AuthEnabledInProcPravegaClusterTest extends InProcPravegaClusterTest {
    @Before
    @Override
    public void setUp() throws Exception {
        this.authEnabled = true;
        this.tlsEnabled = false;
        super.setUp();
    }

    @Override
    String scopeName() {
        return "AuthTestScope";
    }

    @Override
    String streamName() {
        return "AuthTestStream";
    }

    @Override
    String eventMessage() {
        return "Test message on the plaintext channel with auth credentials";
    }

    @Override
    ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .credentials(new DefaultCredentials(
                        SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                        SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                .build();
    }

    /**
     * This test verifies that create stream fails when the client config is invalid.
     *
     * Note: The timeout being used for the test is kept rather large so that there is ample time for the expected
     * exception to be raised even in case of abnormal delays in test environments.
     */
    @Test(timeout = 300000)
    public void testCreateStreamFailsWithInvalidClientConfig() {
       ClientConfig clientConfig = ClientConfig.builder()
                .credentials(new DefaultCredentials("", ""))
                .controllerURI(URI.create(localPravega.getInProcPravegaCluster().getControllerURI()))
                .build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        AssertExtensions.assertThrows("Auth exception did not occur.",
                () -> streamManager.createScope(scopeName()),
                e -> hasAuthExceptionAsRootCause(e));
    }

    private boolean hasAuthExceptionAsRootCause(Throwable e) {
        Throwable innermostException = ExceptionUtils.getRootCause(e);

        // Depending on an exception message for determining whether the given exception represents auth failure
        // is not a good thing to do, but we have no other choice here because auth failures are represented as the
        // overly general io.grpc.StatusRuntimeException.
        return innermostException instanceof StatusRuntimeException &&
             innermostException.getMessage().toUpperCase().contains("UNAUTHENTICATED");
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }
}