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

import org.junit.ClassRule;
import org.junit.Test;

import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;

public class TlsProtocolVersion13Test {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().tlsEnabled(true).tlsProtocolVersion(new String[] {"TLSv1.3"}).build();

    @Test(timeout = 30000)
    public void testTlsProtocolVersiontls1_3() throws Exception {
        testWriteAndReadAnEvent("tls13scope", "tls13stream", "Test message on the TLSv1.3 encrypted channel",
                EMULATOR.getClientConfig());
    }
}
