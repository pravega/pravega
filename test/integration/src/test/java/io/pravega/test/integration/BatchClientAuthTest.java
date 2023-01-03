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

import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The tests in this class are intended to verify whether Batch Client works with a Pravega cluster
 * that has "Auth" (short for authentication and authorization) enabled.
 *
 * This class inherits the tests of the parent class. Some of the test methods of the parent are reproduced here as
 * handles, to enable running an individual test interactively (for debugging purposes).
 */
@Slf4j
public class BatchClientAuthTest extends BatchClientTest {

    private static final File PASSWORD_AUTHHANDLER_INPUT = createAuthFile();

    // We use this to ensure that the tests that depend on system properties are run one at a time, in order to avoid
    // one test causing side effects in another.
    Lock sequential = new ReentrantLock();

    @AfterClass
    public static void classTearDown() {
        if (PASSWORD_AUTHHANDLER_INPUT.exists()) {
            PASSWORD_AUTHHANDLER_INPUT.delete();
        }
    }

    @Override
    protected ClientConfig createClientConfig() {
        return ClientConfig.builder()
                    .controllerURI(URI.create(this.controllerUri()))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .build();
    }

    @Override
    protected ServiceBuilder createServiceBuilder() {
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1))
                .include(AutoScalerConfig.builder()
                        .with(AutoScalerConfig.CONTROLLER_URI, this.controllerUri())
                        .with(AutoScalerConfig.TOKEN_SIGNING_KEY, "secret")
                        .with(AutoScalerConfig.AUTH_ENABLED, true));

        return ServiceBuilder.newInMemoryBuilder(configBuilder.build());
    }

    @Override
    protected ControllerWrapper createControllerWrapper() {
        return new ControllerWrapper(zkTestServer.getConnectString(),
                false, true,
                controllerPort, serviceHost, servicePort, containerCount, -1,
                true, PASSWORD_AUTHHANDLER_INPUT.getPath(), "secret");
    }

    @Test(timeout = 50000)
    public void testListAndReadSegmentsWithClientCredentialsViaSystemProperties() throws ExecutionException, InterruptedException {
        // Using a lock to prevent concurrent execution of tests that set system properties.
        sequential.lock();

        try {
            setClientAuthProperties("appaccount", "1111_aaaa");
            ClientConfig config = ClientConfig.builder()
                    .controllerURI(URI.create(this.controllerUri()))
                    .build();
            this.listAndReadSegmentsUsingBatchClient("testScope", "testBatchStream", config);
            unsetClientAuthProperties();
        } finally {
            sequential.unlock();
        }
    }

    @Test(timeout = 250000)
    public void testListAndReadSegmentsWithNoClientCredentials() {
        ClientConfig config = ClientConfig.builder()
                .controllerURI(URI.create(this.controllerUri()))
                .build();

        AssertExtensions.assertThrows("Auth exception did not occur.",
                () -> this.listAndReadSegmentsUsingBatchClient("testScope", "testBatchStream", config),
                e -> hasAuthenticationExceptionAsRootCause(e));
    }

    @Test(timeout = 250000)
    public void testListAndReadSegmentsWithInvalidClientCredentials() {
        ClientConfig config = ClientConfig.builder()
                .controllerURI(URI.create(this.controllerUri()))
                .credentials(new DefaultCredentials("wrong-password", "admin"))
                .build();

        AssertExtensions.assertThrows("Auth exception did not occur.",
                () -> this.listAndReadSegmentsUsingBatchClient("testScope", "testBatchStream", config),
                e -> hasAuthenticationExceptionAsRootCause(e));
    }

    @Test(timeout = 250000)
    public void testListAndReadSegmentsWithUnauthorizedAccountViaSystemProperties() {
        // Using a lock to prevent concurrent execution of tests that set system properties.
        sequential.lock();
        try {
            setClientAuthProperties("unauthorizeduser", "1111_aaaa");
            ClientConfig config = ClientConfig.builder()
                    .controllerURI(URI.create(this.controllerUri()))
                    .build();

            AssertExtensions.assertThrows("Auth exception did not occur.",
                    () -> this.listAndReadSegmentsUsingBatchClient("testScope", "testBatchStream", config),
                    e -> hasAuthorizationExceptionAsRootCause(e));
            unsetClientAuthProperties();
        } finally {
            sequential.unlock();
        }
    }

    private static File createAuthFile() {
        @SuppressWarnings("resource")
        PasswordAuthHandlerInput result = new PasswordAuthHandlerInput("BatchClientAuth", ".txt");

        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        try {
            String encryptedPassword = passwordProcessor.encryptPassword("1111_aaaa");

            List<PasswordAuthHandlerInput.Entry> entries = Arrays.asList(
                    PasswordAuthHandlerInput.Entry.of("admin", encryptedPassword, "prn::*,READ_UPDATE;"),
                    PasswordAuthHandlerInput.Entry.of("appaccount", encryptedPassword, "prn::*,READ_UPDATE;"),
                    PasswordAuthHandlerInput.Entry.of("unauthorizeduser", encryptedPassword, "prn::")
            );
            result.postEntries(entries);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
        return result.getFile();
    }

    private void setClientAuthProperties(String userName, String password) {
        // Prepare the token to be used for basic authentication
        String plainToken = userName + ":" + password;
        String base66EncodedToken = Base64.getEncoder().encodeToString(plainToken.getBytes(StandardCharsets.UTF_8));

        System.setProperty("pravega.client.auth.method", "Basic");
        System.setProperty("pravega.client.auth.token", base66EncodedToken);
    }

    private void unsetClientAuthProperties()  {
        System.clearProperty("pravega.client.auth.method");
        System.clearProperty("pravega.client.auth.token");
    }

    private boolean hasAuthorizationExceptionAsRootCause(Throwable e) {
        Throwable innermostException = ExceptionUtils.getRootCause(e);

        // Depending on an exception message for determining whether the given exception represents auth failure
        // is not a good thing to do, but we have no other choice here because auth failures are represented as the
        // overly general io.grpc.StatusRuntimeException.
        return innermostException != null && innermostException instanceof StatusRuntimeException &&
                innermostException.getMessage().toUpperCase().contains("PERMISSION_DENIED");
    }

    private boolean hasAuthenticationExceptionAsRootCause(Throwable e) {
        Throwable innermostException = ExceptionUtils.getRootCause(e);

        // Depending on an exception message for determining whether the given exception represents auth failure
        // is not a good thing to do, but we have no other choice here because auth failures are represented as the
        // overly general io.grpc.StatusRuntimeException.
        return innermostException != null && innermostException instanceof StatusRuntimeException &&
                innermostException.getMessage().toUpperCase().contains("UNAUTHENTICATED");
    }
}