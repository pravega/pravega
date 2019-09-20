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

public class ZKTLSUtils {

    public static void setSecureZKClientProperties(String trustStorePath, String trustStorePassword) {
        System.setProperty("zookeeper.client.secure", "true");
        System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
        System.setProperty("zookeeper.ssl.trustStore.location", trustStorePath);
        System.setProperty("zookeeper.ssl.trustStore.password", trustStorePassword);
    }

    public static void unsetSecureZKClientProperties() {
        System.clearProperty("zookeeper.client.secure");
        System.clearProperty("zookeeper.clientCnxnSocket");
        System.clearProperty("zookeeper.ssl.trustStore.location");
        System.clearProperty("zookeeper.ssl.trustStore.password");
    }

}
