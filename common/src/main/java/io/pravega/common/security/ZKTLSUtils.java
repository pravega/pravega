/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.security;

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

    public static void setSecureZKServerProperties(String keyStore, String keyStorePasswordPath, String trustStore,
                                                   String trustStorePasswordPath) {
        System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        System.setProperty("zookeeper.ssl.keyStore.location", keyStore);
        System.setProperty("zookeeper.ssl.keyStore.password", JKSHelper.loadPasswordFrom(keyStorePasswordPath));
        System.setProperty("zookeeper.ssl.trustStore.location", trustStore);
        System.setProperty("zookeeper.ssl.trustStore.password", JKSHelper.loadPasswordFrom(trustStorePasswordPath));
        // This is needed to allow ZooKeeperServer to use the auth provider.
        System.setProperty("zookeeper.authProvider.x509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
    }

    public static void unsetSecureZKServerProperties() {
        System.clearProperty("zookeeper.serverCnxnFactory");
        System.clearProperty("zookeeper.ssl.keyStore.location");
        System.clearProperty("zookeeper.ssl.keyStore.password");
        System.clearProperty("zookeeper.ssl.trustStore.location");
        System.clearProperty("zookeeper.ssl.trustStore.password");
        System.clearProperty("zookeeper.authProvider.x509");
    }

}
