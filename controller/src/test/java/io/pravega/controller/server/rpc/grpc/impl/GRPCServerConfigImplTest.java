/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc.impl;

import org.junit.Test;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl.GRPCServerConfigImplBuilder;

import static org.junit.Assert.assertNotNull;


public class GRPCServerConfigImplTest {

    // region Tests that verify the toString() method.

    // Note: It might seem odd that we are unit testing the toString() method of the code under test. The reason we are
    // doing that is that the method is hand-rolled and there is a bit of logic there that isn't entirely unlikely to fail.

    @Test
    public void testToStringReturnsSuccessfullyWithAllConfigSpecified() {
        GRPCServerConfigImpl config = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost("localhost")
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                .userPasswordFile("/passwd")
                .tlsEnabled(true)
                .tlsCertFile("/cert.pem")
                .tlsKeyFile("./key.pem")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(200)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(true)
                .requestTracingEnabled(true)
                .build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringReturnsSuccessfullyWithSomeConfigNullOrEmpty() {
        GRPCServerConfigImpl config = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost(null)
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                .userPasswordFile(null)
                .tlsEnabled(true)
                .tlsCertFile("")
                .tlsKeyFile("")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(300)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(false)
                .requestTracingEnabled(true)
                .build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringReturnsSuccessfullyWithAccessTokenTtlNull() {
        GRPCServerConfigImpl config = new GRPCServerConfigImplBuilder().port(9090)
                .publishedRPCHost("localhost")
                .publishedRPCPort(9090)
                .authorizationEnabled(true)
                .userPasswordFile("/passwd")
                .tlsEnabled(true)
                .tlsCertFile("/cert.pem")
                .tlsKeyFile("./key.pem")
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(null)
                .tlsTrustStore("/cert.pem")
                .replyWithStackTraceOnError(true)
                .requestTracingEnabled(true)
                .build();
        assertNotNull(config.toString());
    }

    // endregion
}
