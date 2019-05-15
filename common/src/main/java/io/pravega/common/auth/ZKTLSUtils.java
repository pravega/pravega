/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.auth;

/**
 * Utility class to set and unset the SSL-related configuration parameters in a Zookeeper client.
 */
public class ZKTLSUtils {

    public static void setSecureZKClientProperties(String trustStorePath, String trustStorePassword) {
        System.setProperty("zookeeper.client.secure", "true");
        System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
        System.setProperty("zookeeper.ssl.trustStore.location", "/home/raul/Documents/workspace/mine/pravega/config/client.truststore.jks");
        System.setProperty("zookeeper.ssl.trustStore.password", "1111_aaaa");
        System.setProperty("zookeeper.ssl.keyStore.location", "/home/raul/Documents/workspace/mine/pravega/config/server.keystore.jks");
        System.setProperty("zookeeper.ssl.keyStore.password", "1111_aaaa");
    }

    public static void unsetSecureZKClientProperties() {
        System.clearProperty("zookeeper.client.secure");
        System.clearProperty("zookeeper.clientCnxnSocket");
        System.clearProperty("zookeeper.ssl.trustStore.location");
        System.clearProperty("zookeeper.ssl.trustStore.password");
        System.clearProperty("zookeeper.ssl.keyStore.location");
        System.clearProperty("zookeeper.ssl.keyStore.password");
    }

}
