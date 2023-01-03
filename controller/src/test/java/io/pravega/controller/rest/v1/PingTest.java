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
package io.pravega.controller.rest.v1;

import io.pravega.controller.server.rest.resources.PingImpl;
import io.pravega.shared.rest.RESTServer;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.jersey.SslConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Test for ping API.
 */
public abstract class PingTest {

    private RESTServerConfig serverConfig;
    private RESTServer restServer;
    private Client client;

    @Before
    public void setup() throws Exception {
        serverConfig = getServerConfig();
        restServer = new RESTServer(serverConfig, Set.of(new PingImpl()));
        restServer.startAsync();
        restServer.awaitRunning();
        client = createJerseyClient();
    }

    protected abstract Client createJerseyClient() throws Exception;

    abstract RESTServerConfig getServerConfig() throws Exception;


    @After
    public void tearDown() {
        client.close();
        restServer.stopAsync();
        restServer.awaitTerminated();
    }

    @Test(timeout = 30000)
    public void test() {
        URI streamResourceURI = UriBuilder.fromPath("//localhost:" + serverConfig.getPort() + "/ping")
                                          .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        assertEquals(200, response.getStatus());
    }

    protected abstract String getURLScheme();

    public static class SimplePingTest extends PingTest {

        @Override
        protected Client createJerseyClient() {
            return ClientBuilder.newClient();
        }

        @Override
        RESTServerConfig getServerConfig() {
            return RESTServerConfigImpl.builder().host("localhost").port(TestUtils.getAvailableListenPort())
                                .build();
        }

        @Override
        protected String getURLScheme() {
            return "http";
        }
    }

    public static class SecurePingTest extends PingTest {

        protected String getResourcePath(String resource) throws Exception {
            return this.getClass().getClassLoader().getResource(resource).toURI().getPath();
        }

        @Override
        protected Client createJerseyClient() throws Exception {
            SslConfigurator sslConfig = SslConfigurator.newInstance().trustStoreFile(
                    getResourcePath(SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_NAME));

            SSLContext sslContext = sslConfig.createSSLContext();
            return ClientBuilder.newBuilder().sslContext(sslContext)
                                .hostnameVerifier((s1, s2) -> true)
                                .build();
        }

        @Override
        RESTServerConfig getServerConfig() throws Exception {
            return RESTServerConfigImpl.builder().host("localhost").port(TestUtils.getAvailableListenPort())
                                       .tlsEnabled(true)
                                       .tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                                       .keyFilePath(getResourcePath(SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME))
                                       .keyFilePasswordPath(getResourcePath(SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME))
                                       .build();
        }

        @Override
        protected String getURLScheme() {
            return "https";
        }
    }

    public static class FailingSecurePingTest extends SecurePingTest {
        @Override
        RESTServerConfig getServerConfig() throws Exception {
            return RESTServerConfigImpl.builder().host("localhost").port(TestUtils.getAvailableListenPort())
                                       .tlsEnabled(true)
                                       .tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                                       .keyFilePath(getResourcePath(SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME))
                                       .keyFilePasswordPath("Wrong_Path")
                                       .build();
        }

        @Override
        @Test
        public void test() {
            AssertExtensions.assertThrows(ProcessingException.class, () -> {
                super.test();
            });
        }

    }
}
