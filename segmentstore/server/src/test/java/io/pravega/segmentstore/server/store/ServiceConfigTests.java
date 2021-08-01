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
package io.pravega.segmentstore.server.store;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the ServiceConfig class
 */
public class ServiceConfigTests {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);
    @Test
    public void testListeningAndPublicIPAndPort() {
        // When the published IP and port are not specified, it should default to listening IP and port
        ServiceConfig cfg1 = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .build();
        Assert.assertTrue("Published IP and port should default to listening IP and port",
                cfg1.getListeningIPAddress().equals(cfg1.getPublishedIPAddress())
                        && cfg1.getListeningPort() == cfg1.getPublishedPort());
        // Published IP not defined but port is different as compared to listening port
        ServiceConfig cfg2 = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.PUBLISHED_IP_ADDRESS, "myhost1")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .build();
        Assert.assertTrue("Published IP should default to listening IP even when ports are different",
                !cfg2.getListeningIPAddress().equals(cfg2.getPublishedIPAddress())
                        && cfg2.getListeningPort() == cfg2.getPublishedPort());
        //Both published IP and port are defined and are different than listening IP and port
        ServiceConfig cfg3 = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.PUBLISHED_IP_ADDRESS, "myhost1")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .with(ServiceConfig.PUBLISHED_PORT, 5000)
                .build();
        Assert.assertTrue("When specified publishing IP and port should differ from listening IP and port",
                !cfg3.getListeningIPAddress().equals(cfg3.getPublishedIPAddress())
                        && cfg3.getListeningPort() != cfg3.getPublishedPort());
    }

    @Test
    public void testDefaultSecurityConfigValues() {
        ServiceConfig config = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .build();

        assertFalse(config.isEnableTls());
        assertFalse(config.isEnableTlsReload());
        assertEquals("", config.getCertFile());
        assertEquals("", config.getKeyFile());
        Assert.assertArrayEquals(new String[]{"TLSv1.2", "TLSv1.3"}, config.getTlsProtocolVersion());
    }

    // region Tests that verify the toString() method.

    @Test
    public void testToStringIsSuccessfulWithAllNonDefaultConfigSpecified() {
        ServiceConfig config = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "localhost")
                .with(ServiceConfig.PUBLISHED_PORT, 4000)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "1.2.3.4")
                .with(ServiceConfig.PUBLISHED_IP_ADDRESS, "1.2.3.4")
                .with(ServiceConfig.ZK_TRUSTSTORE_LOCATION, "/zkTruststorePath")
                .with(ServiceConfig.ZK_TRUST_STORE_PASSWORD_PATH, "/zkTruststorePasswordPath")
                .with(ServiceConfig.CERT_FILE, "/cert.pem")
                .with(ServiceConfig.KEY_FILE, "/key.pem")
                .with(ServiceConfig.INSTANCE_ID, "1234")
                .with(ServiceConfig.ENABLE_TLS_RELOAD, true)
                .build();
        Assert.assertNotNull(config.toString());
    }


    // endregion
}
