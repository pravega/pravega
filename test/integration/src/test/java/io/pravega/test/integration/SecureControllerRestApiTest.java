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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.controller.server.rest.generated.model.ReaderGroupProperty;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsList;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsListReaderGroups;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.utils.ClusterWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import static io.pravega.common.util.CertificateUtils.createTrustStore;
import static io.pravega.test.integration.utils.TestUtils.pathToConfig;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class SecureControllerRestApiTest {

    private static final String TRUSTSTORE_PATH = pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME;
    private static final ClusterWrapper CLUSTER = ClusterWrapper.builder()
            .authEnabled(true)
            .tlsEnabled(true)
            .tlsServerCertificatePath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
            .tlsServerKeyPath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
            .tlsHostVerificationEnabled(false)
            .tlsServerKeystorePath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME)
            .tlsServerKeystorePasswordPath(pathToConfig() + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME)
            .controllerRestEnabled(true)
            .build();

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    private final Client client;
    private WebTarget webTarget;
    private String restServerURI;
    private String resourceURl;

    public SecureControllerRestApiTest() {
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");

        ClientBuilder builder = ClientBuilder.newBuilder().withConfig(clientConfig);

        SSLContext tlsContext;
        try {
            KeyStore ks = createTrustStore(TRUSTSTORE_PATH);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            tlsContext = SSLContext.getInstance("TLS");
            tlsContext.init(null, tmf.getTrustManagers(), null);
            builder.sslContext(tlsContext);
        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | KeyManagementException e) {
            String message = String.format("Encountered exception while trying to use the given truststore: %s", TRUSTSTORE_PATH);
            log.error(message, e);
        }
        client = builder.build();
        HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(SecurityConfigDefaults.AUTH_ADMIN_USERNAME,
                SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        client.register(auth);
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        CLUSTER.start();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        CLUSTER.close();
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void secureReaderGroupRestApiTest() throws Exception {

        Invocation.Builder builder;
        Response response;

        restServerURI = CLUSTER.controllerRestUri();
        log.info("REST Server URI: {}", restServerURI);

        // TEST REST server status, ping test
        resourceURl = new StringBuilder(restServerURI).append("/ping").toString();
        webTarget = client.target(resourceURl);
        builder = webTarget.request();
        response = builder.get();
        assertEquals("Ping test", OK.getStatusCode(), response.getStatus());
        log.info("REST Server is running. Ping successful.");

        // Test reader groups APIs.
        // Prepare the streams and readers using the admin client.
        final String testScope = RandomStringUtils.randomAlphanumeric(10);
        final String testStream1 = RandomStringUtils.randomAlphanumeric(10);
        final String testStream2 = RandomStringUtils.randomAlphanumeric(10);
        URI controllerUri = new URI(CLUSTER.controllerUri());
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri)
                .credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                        SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                .trustStore(TRUSTSTORE_PATH)
                .validateHostName(false)
                .build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(createController(controllerUri, inlineExecutor), cp)) {
            log.info("Creating scope: {}", testScope);
            streamManager.createScope(testScope);

            log.info("Creating stream: {}", testStream1);
            StreamConfiguration streamConf1 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
            streamManager.createStream(testScope, testStream1, streamConf1);

            log.info("Creating stream: {}", testStream2);
            StreamConfiguration streamConf2 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
            streamManager.createStream(testScope, testStream2, streamConf2);
        }

        final String readerGroupName1 = RandomStringUtils.randomAlphanumeric(10);
        final String readerGroupName2 = RandomStringUtils.randomAlphanumeric(10);
        final String reader1 = RandomStringUtils.randomAlphanumeric(10);
        final String reader2 = RandomStringUtils.randomAlphanumeric(10);
        try (ClientFactoryImpl clientFactory = new ClientFactoryImpl(testScope, createController(controllerUri, inlineExecutor), clientConfig);
             ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(testScope,
                     ClientConfig.builder()
                             .controllerURI(controllerUri)
                             .credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                                     SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                             .trustStore(TRUSTSTORE_PATH)
                             .validateHostName(false)
                             .build())) {
            readerGroupManager.createReaderGroup(readerGroupName1, ReaderGroupConfig.builder()
                    .stream(Stream.of(testScope, testStream1))
                    .stream(Stream.of(testScope, testStream2))
                    .build());
            readerGroupManager.createReaderGroup(readerGroupName2, ReaderGroupConfig.builder()
                    .stream(Stream.of(testScope, testStream1))
                    .stream(Stream.of(testScope, testStream2))
                    .build());
            clientFactory.createReader(reader1, readerGroupName1, new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            clientFactory.createReader(reader2, readerGroupName1, new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
        }

        // Test fetching readergroups.
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + testScope + "/readergroups").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroups status", OK.getStatusCode(), response.getStatus());
        ReaderGroupsList readerGroupsList = response.readEntity(ReaderGroupsList.class);
        assertEquals("Get readergroups size", 2, readerGroupsList.getReaderGroups().size());
        assertTrue(readerGroupsList.getReaderGroups().contains(
                new ReaderGroupsListReaderGroups().readerGroupName(readerGroupName1)));
        assertTrue(readerGroupsList.getReaderGroups().contains(
                new ReaderGroupsListReaderGroups().readerGroupName(readerGroupName2)));
        log.info("Get readergroups successful");

        // Test fetching readergroup info.
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + testScope + "/readergroups/"
                + readerGroupName1).toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroup properties status", OK.getStatusCode(), response.getStatus());
        ReaderGroupProperty readerGroupProperty = response.readEntity(ReaderGroupProperty.class);
        assertEquals("Get readergroup name", readerGroupName1, readerGroupProperty.getReaderGroupName());
        assertEquals("Get readergroup scope name", testScope, readerGroupProperty.getScopeName());
        assertEquals("Get readergroup streams size", 2, readerGroupProperty.getStreamList().size());
        assertTrue(readerGroupProperty.getStreamList().contains(Stream.of(testScope, testStream1).getScopedName()));
        assertTrue(readerGroupProperty.getStreamList().contains(Stream.of(testScope, testStream2).getScopedName()));
        assertEquals("Get readergroup onlinereaders size", 2, readerGroupProperty.getOnlineReaderIds().size());
        assertTrue(readerGroupProperty.getOnlineReaderIds().contains(reader1));
        assertTrue(readerGroupProperty.getOnlineReaderIds().contains(reader2));

        // Test readergroup or scope not found.
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + testScope + "/readergroups/" +
                "unknownreadergroup").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroup properties status", NOT_FOUND.getStatusCode(), response.getStatus());
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + "unknownscope" + "/readergroups/" +
                readerGroupName1).toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroup properties status", NOT_FOUND.getStatusCode(), response.getStatus());
        log.info("Get readergroup properties successful");

        log.info("Test restApiTests passed successfully!");
    }

    private Controller createController(URI controllerUri, InlineExecutor executor) {
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri)
                .credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                        SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                .trustStore(TRUSTSTORE_PATH)
                .validateHostName(false)
                .build();
        return new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .retryAttempts(1).build(), executor);
    }
}
