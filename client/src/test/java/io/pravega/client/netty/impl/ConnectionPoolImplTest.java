/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.netty.handler.ssl.SslContext;
import io.pravega.client.ClientConfig;
import io.pravega.test.common.SecurityConfigDefaults;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ConnectionPoolImplTest {

    @Test
    public void testGetSslContextReturnsNullWhenClientConfigTlsIsDisabled() {
        ClientConfig config = ClientConfig.builder().build();
        ConnectionPool pool = new ConnectionPoolImpl(config);
        SslContext ctx = ((ConnectionPoolImpl) pool).getSslContext();
        assertNull("SslContext is null", ctx);
    }

    @Test
    public void testGetSslContextSucceedsWhenTruststoreIsNotSpecified() {
        ClientConfig config = ClientConfig.builder().controllerURI(URI.create("tls://localhost:9090")).build();
        ConnectionPool pool = new ConnectionPoolImpl(config);
        SslContext ctx = ((ConnectionPoolImpl) pool).getSslContext();
        assertNotNull("SslContext is null", ctx);
    }

    @Test
    public void testGetSslContextSucceedsWhenTruststoreIsSpecified() {
        ClientConfig config = ClientConfig.builder()
                .controllerURI(URI.create("tls://localhost:9090"))
                .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                .build();
        ConnectionPool pool = new ConnectionPoolImpl(config);
        SslContext ctx = ((ConnectionPoolImpl) pool).getSslContext();
        assertNotNull("SslContext is null", ctx);
    }
}
