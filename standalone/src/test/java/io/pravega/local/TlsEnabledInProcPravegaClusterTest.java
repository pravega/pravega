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
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.AssertExtensions;
import java.net.URI;
import javax.net.ssl.SSLHandshakeException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;

/**
 * Tests for TLS enabled standalone cluster. It inherits the test methods defined in the parent class.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class TlsEnabledInProcPravegaClusterTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().tlsEnabled(true).build();
    final String scope = "TlsTestScope";
    final String stream = "TlsTestStream";
    final String msg = "Test message on the encrypted channel";

    /**
     * This test verifies that create stream fails when the client config is invalid.
     *
     * Note: The timeout being used for the test is kept rather large so that there is ample time for the expected
     * exception to be raised even in case of abnormal delays in test environments.
     */
    @Test(timeout = 50000)
    public void testCreateStreamFailsWithInvalidClientConfig() {
        // Truststore for the TLS connection is missing.
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(EMULATOR.pravega.getInProcPravegaCluster().getControllerURI()))
                .build();

        ControllerImplConfig controllerImplConfig = ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .retryAttempts(1)
                .initialBackoffMillis(1000)
                .backoffMultiple(1)
                .maxBackoffMillis(1000)
                .build();

        @Cleanup
        StreamManager streamManager = new StreamManagerImpl(clientConfig, controllerImplConfig);

        AssertExtensions.assertThrows("TLS exception did not occur.",
                () -> streamManager.createScope(scope),
                e -> hasTlsException(e));
    }

    @Test(timeout = 30000)
    public void testWriteAndReadEventWithValidClientConfig() throws Exception {
        testWriteAndReadAnEvent(scope, stream, msg, EMULATOR.getClientConfig());
    }

    private boolean hasTlsException(Throwable e) {
        return ExceptionUtils.indexOfThrowable(e, SSLHandshakeException.class) != -1;
    }
}
