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
package io.pravega.client;

import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import java.net.URI;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClientConfigTest {

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    @Test
    public void serializable() {
        JavaSerializer<ClientConfig> s = new JavaSerializer<>();
        ClientConfig expected = ClientConfig.builder()
                .credentials(new DefaultCredentials(PASSWORD, USERNAME))
                .controllerURI(URI.create("tcp://localhost:9090"))
                .trustStore("truststore.jks")
                .validateHostName(false)
                .build();
        ClientConfig actual = s.deserialize(s.serialize(expected));
        assertEquals(expected, actual);
    }

    @Test
    public void testDefault() {
        ClientConfig defaultConfig = ClientConfig.builder().build();
        assertEquals(ClientConfig.DEFAULT_MAX_CONNECTIONS_PER_SEGMENT_STORE, defaultConfig.getMaxConnectionsPerSegmentStore());
        ClientConfig config1 = ClientConfig.builder().maxConnectionsPerSegmentStore(-1).build();
        assertEquals(ClientConfig.DEFAULT_MAX_CONNECTIONS_PER_SEGMENT_STORE, config1.getMaxConnectionsPerSegmentStore());
        ClientConfig config2 = ClientConfig.builder().maxConnectionsPerSegmentStore(1).build();
        assertEquals(1, config2.getMaxConnectionsPerSegmentStore());
    }

    @Test
    public void testTlsIsEnabledForControllerURIContainingSchemeTls() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create("tls://hostname:9090"));
        assertTrue("TLS is disabled", builder.build().isEnableTls());
    }

    @Test
    public void testTlsIsDisabledForControllerURIContainingSchemeTcp() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create("tcp://hostname:9090"));
        assertFalse("TLS is enabled", builder.build().isEnableTls());
    }

    @Test
    public void testTlsIsDisabledWhenTlsIsPartiallySet() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create("tcp://hostname:9090"))
                .enableTlsToController(true);
        assertFalse("TLS is enabled", builder.build().isEnableTls());
    }

    @Test
    public void testTlsIsEnabledWhenAllTlsEnabled() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create("tcp://hostname:9090"))
                .enableTlsToController(true)
                .enableTlsToSegmentStore(true);
        assertTrue("TLS is disabled", builder.build().isEnableTls());
    }

    @Test
    public void testTlsIsDisabledWhenAllTlsDisabled() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create("tcp://hostname:9090"))
                .enableTlsToController(true)
                .enableTlsToSegmentStore(true);
        assertTrue("TLS is disabled", builder.build().isEnableTls());
    }

    @Test
    public void testTlsIsDisabledWhenSchemeIsNull() {
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create("//hostname:9090"))
                .build();
        assertFalse("TLS is enabled", clientConfig.isEnableTls());
    }

    @Test
    public void testMetricsListener() {
        ClientConfig clientConfig = ClientConfig.builder()
                                                .controllerURI(URI.create("//hostname:9090"))
                                                .build();
        assertNull("Metrics listener is not configured", clientConfig.getMetricListener());
        clientConfig = ClientConfig.builder()
                                   .controllerURI(URI.create("//hostname:9090"))
                                   .metricListener(null)
                                   .build();
        assertNull("Metrics listener is not configured", clientConfig.getMetricListener());
    }
}
