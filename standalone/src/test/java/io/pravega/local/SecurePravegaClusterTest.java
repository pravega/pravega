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

import io.pravega.test.common.SerializedClassRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;

/**
 * This class holds tests for TLS and auth enabled in-process standalone cluster. It inherits the test methods defined
 * in the parent class.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class SecurePravegaClusterTest {
    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().authEnabled(true).tlsEnabled(true).build();
    final String scope = "TlsAndAuthTestScope";
    final String stream = "TlsAndAuthTestStream";
    final String msg = "Test message on the encrypted channel with auth credentials";

    @Test(timeout = 50000)
    public void testWriteAndReadEventWithValidClientConfig() throws Exception {
        testWriteAndReadAnEvent(scope, stream, msg, EMULATOR.getClientConfig());
    }
}
