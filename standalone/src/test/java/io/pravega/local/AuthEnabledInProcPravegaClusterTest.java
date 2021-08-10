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

import java.net.URI;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.common.Exceptions;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SerializedClassRunner;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;

/**
 * This class contains tests for auth enabled in-process standalone cluster. It inherits the test methods defined
 * in the parent class.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class AuthEnabledInProcPravegaClusterTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().authEnabled(true).build();
    final String scope = "AuthTestScope";
    final String stream = "AuthTestStream";
    final String msg = "Test message on the plaintext channel with auth credentials";

    /**
     * This test verifies that create stream fails when the client config is invalid.
     *
     * Note: The timeout being used for the test is kept rather large so that there is ample time for the expected
     * exception to be raised even in case of abnormal delays in test environments.
     */
    @Test(timeout = 50000)
    public void testCreateStreamFailsWithInvalidClientConfig() {
       ClientConfig clientConfig = ClientConfig.builder()
                .credentials(new DefaultCredentials("", ""))
                .controllerURI(URI.create(EMULATOR.pravega.getInProcPravegaCluster().getControllerURI()))
                .build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        AssertExtensions.assertThrows("Auth exception did not occur.",
                () -> streamManager.createScope(scope),
                e -> hasAuthExceptionAsRootCause(e));
    }

    @Test(timeout = 50000)
    public void testWriteAndReadEventWithValidClientConfig() throws Exception {
        testWriteAndReadAnEvent(scope, stream, msg, EMULATOR.getClientConfig());
    }

    private boolean hasAuthExceptionAsRootCause(Throwable e) {
        Throwable unwrapped = Exceptions.unwrap(e);

        // Depending on an exception message for determining whether the given exception represents auth failure
        // is not a good thing to do, but we have no other choice here because auth failures are represented as the
        // overly general io.grpc.StatusRuntimeException.
        return unwrapped instanceof StatusRuntimeException &&
                unwrapped.getMessage().toUpperCase().contains("UNAUTHENTICATED");
    }
}
