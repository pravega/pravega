/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.auth;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test to check the correctness of Zookeeper security settings in Pravega clients.
 */
public class ZKTLSUtilsTest {

    @Test
    public void secureZKClientPropertiesTest() {
        final String trustStorePath = "trustStorePath";
        final String trustStorePassword = "trustStorePassword";
        ZKTLSUtils.setSecureZKClientProperties(trustStorePath, trustStorePassword);
        Assert.assertEquals("true", System.getProperty("zookeeper.client.secure"));
        Assert.assertEquals("org.apache.zookeeper.ClientCnxnSocketNetty", System.getProperty("zookeeper.clientCnxnSocket"));
        Assert.assertEquals(trustStorePath, System.getProperty("zookeeper.ssl.trustStore.location"));
        Assert.assertEquals(trustStorePassword, System.getProperty("zookeeper.ssl.trustStore.password"));
        ZKTLSUtils.unsetSecureZKClientProperties();
        Assert.assertNull(System.getProperty("zookeeper.client.secure"));
        Assert.assertNull(System.getProperty("zookeeper.clientCnxnSocket"));
        Assert.assertNull(System.getProperty("zookeeper.ssl.trustStore.location"));
        Assert.assertNull(System.getProperty("zookeeper.ssl.trustStore.password"));
    }

    @Test
    public void secureZKServerPropertiesTest() {
        final String keyStore = "keyStore";
        final String keyStorePasswordPath = "../config/server.keystore.jks.passwd";
        final String trustStore = "trustStore";
        final String trustStorePasswordPath = "../config/server.keystore.jks.passwd";
        ZKTLSUtils.setSecureZKServerProperties(keyStore, keyStorePasswordPath, trustStore, trustStorePasswordPath);
        Assert.assertEquals("org.apache.zookeeper.server.NettyServerCnxnFactory", System.getProperty("zookeeper.serverCnxnFactory"));
        Assert.assertEquals(keyStore, System.getProperty("zookeeper.ssl.keyStore.location"));
        Assert.assertEquals(JKSHelper.loadPasswordFrom(keyStorePasswordPath), System.getProperty("zookeeper.ssl.keyStore.password"));
        Assert.assertEquals(trustStore, System.getProperty("zookeeper.ssl.trustStore.location"));
        Assert.assertEquals(JKSHelper.loadPasswordFrom(trustStorePasswordPath), System.getProperty("zookeeper.ssl.trustStore.password"));
        Assert.assertEquals("org.apache.zookeeper.server.auth.X509AuthenticationProvider", System.getProperty("zookeeper.authProvider.x509"));
        ZKTLSUtils.unsetSecureZKServerProperties();
        Assert.assertNull(System.getProperty("zookeeper.serverCnxnFactory"));
        Assert.assertNull(System.getProperty("zookeeper.ssl.keyStore.location"));
        Assert.assertNull(System.getProperty("zookeeper.ssl.keyStore.password"));
        Assert.assertNull(System.getProperty("zookeeper.ssl.trustStore.location"));
        Assert.assertNull(System.getProperty("zookeeper.ssl.trustStore.password"));
        Assert.assertNull(System.getProperty("zookeeper.authProvider.x509"));
    }
}
