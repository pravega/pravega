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
package io.pravega.local;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import lombok.Builder;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.rules.ExternalResource;

import java.net.URI;
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;

/**
 * This class contains the rules to start and stop LocalPravega / standalone.
 * This resource can be configured for a test using @ClassRule and it will ensure standalone is started once
 * for all the methods in a test and is shutdown once all the tests methods have completed execution.
 *
 * - Usage pattern to start it once for all @Test methods in a class
 *  <pre>
 *  &#64;ClassRule
 *  public static final PravegaEmulatorResource EMULATOR = new PravegaEmulatorResource(false, false, false);
 *  </pre>
 *  - Usage pattern to start it before every @Test method.
 *  <pre>
 *  &#64;Rule
 *  public final PravegaEmulatorResource emulator = new PravegaEmulatorResource(false, false, false);
 *  </pre>
 *
 */
@Slf4j
@Builder
public class PravegaEmulatorResource extends ExternalResource {
    final boolean authEnabled;
    final boolean tlsEnabled;
    final boolean restEnabled;
    final String[] tlsProtocolVersion;
    final LocalPravegaEmulator pravega;

    public static final class PravegaEmulatorResourceBuilder {
        boolean authEnabled = false;
        boolean tlsEnabled = false;
        boolean restEnabled = false;
        String[] tlsProtocolVersion = SecurityConfigDefaults.TLS_PROTOCOL_VERSION;

        public PravegaEmulatorResource build() {
            return new PravegaEmulatorResource(authEnabled, tlsEnabled, restEnabled, tlsProtocolVersion);
        }
    }
    /**
     * Create an instance of Pravega Emulator resource.
     * @param authEnabled Authorisation enable flag.
     * @param tlsEnabled  Tls enable flag.
     * @param restEnabled REST endpoint enable flag.
     * @param tlsProtocolVersion TlsProtocolVersion
     */

    public PravegaEmulatorResource(boolean authEnabled, boolean tlsEnabled, boolean restEnabled, String[] tlsProtocolVersion) {
        this.authEnabled = authEnabled;
        this.tlsEnabled = tlsEnabled;
        this.restEnabled = restEnabled;
        this.tlsProtocolVersion = Arrays.copyOf(tlsProtocolVersion, tlsProtocolVersion.length);
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(TestUtils.getAvailableListenPort())
                .segmentStorePort(TestUtils.getAvailableListenPort())
                .zkPort(TestUtils.getAvailableListenPort())
                .restServerPort(TestUtils.getAvailableListenPort())
                .enableRestServer(restEnabled)
                .enableAuth(authEnabled)
                .enableTls(tlsEnabled)
                .tlsProtocolVersion(tlsProtocolVersion)
                .enabledAdminGateway(true)
                .adminGatewayPort(TestUtils.getAvailableListenPort());

        // Since the server is being built right here, avoiding delegating these conditions to subclasses via factory
        // methods. This is so that it is easy to see the difference in server configs all in one place. This is also
        // unlike the ClientConfig preparation which is being delegated to factory methods to make their preparation
        // explicit in the respective test classes.

        if (authEnabled) {
            emulatorBuilder.passwdFile(SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }
        if (tlsEnabled) {
            emulatorBuilder.certFile(SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                    .keyFile(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                    .jksKeyFile(SecurityConfigDefaults.TLS_SERVER_KEYSTORE_PATH)
                    .jksTrustFile(SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_PATH)
                    .keyPasswordFile(SecurityConfigDefaults.TLS_PASSWORD_PATH);
        }

        pravega = emulatorBuilder.build();
    }



    @Override
    protected void before() throws Exception {
        pravega.start();
        waitUntilHealthy();
    }

    /**
     * Return the ClientConfig based on the Pravega standalone configuration.
     *
     * @return ClientConfig
     */
    public ClientConfig getClientConfig() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder()
                .controllerURI(URI.create(pravega.getInProcPravegaCluster().getControllerURI()));
        if (authEnabled) {
            builder.credentials(new DefaultCredentials(
                    SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                    SecurityConfigDefaults.AUTH_ADMIN_USERNAME));
        }
        if (tlsEnabled) {
            builder.trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                    .validateHostName(false);
        }
        return builder.build();
    }

    @SneakyThrows
    @Override
    protected void after() {
        pravega.close();
    }

    /*
     * Wait until Pravega standalone is up and running.
     */
    private void waitUntilHealthy() {
        ClientConfig clientConfig = getClientConfig();
        ControllerImplConfig controllerConfig = ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .retryAttempts(20)
                .initialBackoffMillis(1000)
                .backoffMultiple(2)
                .maxBackoffMillis(10000)
                .build();
        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(clientConfig, controllerConfig);
        assertNotNull(streamManager);
        // Try creating a scope. This will retry based on the provided retry configuration.
        // If all the retries fail a RetriesExhaustedException will be thrown failing the tests.
        boolean isScopeCreated = streamManager.createScope("healthCheck-scope");
        log.debug("Health check scope creation is successful {}", isScopeCreated);
    }
}


