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
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.ClusterWrapper;
import io.pravega.test.integration.utils.TestUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.pravega.common.util.CertificateUtils.createTrustStore;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ClusterWrapperTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @Test
    public void writeAndReadBackAMessageWithSecurityOff() {
        String scopeName = "testScope";
        String streamName = "testStream";
        String readerGroupName = "testReaderGroup";
        String testMessage = "test message";

        // Instantiate and run the cluster. By default, TLS and Auth are off. Also, by default Controller REST
        // interface is disabled.
        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder().build();

        // Checking that the cluster has default settings.
        assertFalse(cluster.isAuthEnabled());
        assertTrue(cluster.isRgWritesWithReadPermEnabled());
        assertEquals(600, cluster.getTokenTtlInSeconds());
        assertEquals(4, cluster.getContainerCount());
        assertTrue(cluster.getTokenSigningKeyBasis().length() > 0);
        assertEquals(-1, cluster.getControllerRestPort());

        cluster.start();

        // Write an event to the stream
        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .build();
        TestUtils.createScopeAndStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        TestUtils.writeDataToStream(scopeName, streamName, testMessage, writerClientConfig);

        // Read back the event from the stream and verify it is the same as what was written
        final ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .build();
        String readMessage = TestUtils.readNextEventMessage(scopeName, streamName, readerClientConfig, readerGroupName);
        assertEquals(testMessage, readMessage);
    }

    @Test
    public void writeAndReadBackAMessageWithTlsAndAuthOn() {
        String scopeName = "testScope";
        String streamName = "testStream";
        String readerGroupName = "testReaderGroup";
        String testMessage = "test message";
        String password = "secret-password";

        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("writer", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:testScope,READ",
                "prn::/scope:testScope/stream:testStream,READ",
                "prn::/scope:testScope/reader-group:testReaderGroup,READ"
        ));

        // Instantiate and run the cluster
        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder()
                // Auth related configs
                .authEnabled(true)
                .passwordAuthHandlerEntries(TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, password))

                // TLS related configs
                .tlsEnabled(true)
                .tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                .tlsServerCertificatePath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .tlsServerKeyPath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                .tlsHostVerificationEnabled(false)

                .build();
        cluster.start();

        // Write an event to the stream
        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .trustStore(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .validateHostName(false)
                .credentials(new DefaultCredentials(password, "writer"))
                .build();
        TestUtils.createScopeAndStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        TestUtils.writeDataToStream(scopeName, streamName, testMessage, writerClientConfig);

        // Read back the event from the stream and verify it is the same as what was written
        final ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .trustStore(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .validateHostName(false)
                .credentials(new DefaultCredentials(password, "reader"))
                .build();
        String readMessage = TestUtils.readNextEventMessage(scopeName, streamName, readerClientConfig, readerGroupName);
        assertEquals(testMessage, readMessage);
    }

    @SneakyThrows
    @Test
    public void restApiInvocationWithSecurityEnabled() {
        String restApiUser = "rest-api-user";
        String restApiUserPwd = "super-secret";

        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("restApiUser", "prn::*,READ_UPDATE");

        // Setup and start the cluster
        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder()
                .controllerRestEnabled(true)

                // Auth related configs
                .authEnabled(true)
                .passwordAuthHandlerEntries(TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, restApiUserPwd))

                // TLS related configs
                .tlsEnabled(true)
                .tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                .tlsServerCertificatePath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .tlsServerKeyPath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                .tlsHostVerificationEnabled(false)
                .tlsServerKeystorePath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME)
                .tlsServerKeystorePasswordPath(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME)

                .build();
        cluster.start();

        // Setup REST client config
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        clientConfig.register(HttpAuthenticationFeature.basic(restApiUser, restApiUserPwd));

        // Prepare a TLS context with truststore containing the signing CA's vertificate
        KeyStore trustStore = createTrustStore(TestUtils.pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        SSLContext tlsContext = SSLContext.getInstance("TLS");
        tlsContext.init(null, tmf.getTrustManagers(), null);

        // Instantiate the REST client
        javax.ws.rs.client.Client client = ClientBuilder.newBuilder()
                        .withConfig(clientConfig)
                        .sslContext(tlsContext)
                        .build();

        String restServerURI = cluster.controllerRestUri();
        log.info("REST Server URI: {}", restServerURI);

        // Invoke the REST operation
        String resourceURl = new StringBuilder(restServerURI).append("/ping").toString();
        WebTarget webTarget = client.target(resourceURl);
        Invocation.Builder builder = webTarget.request();

        // Check if the response was as expected
        Response response = builder.get();
        assertEquals("Response to /ping was not OK", OK.getStatusCode(), response.getStatus());
        log.info("Ping successful.");

        response.close();
        client.close();
    }
}
